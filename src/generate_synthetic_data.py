"""
generate_synthetic_data.py

Genera datos sinteticos que simulan el contenido de HospitalDB_ITVH
(pacientes, doctores, citas, resultados de laboratorio).

Estos datos representan "lo que habria en la base de datos MariaDB real".
Se guardan en data/source_mock/ para que extract.py los trate como si
vinieran de la base de datos origen (simulando una conexion JDBC real).

Uso:
    python src/generate_synthetic_data.py
"""

import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

# Semilla fija para que los datos sean reproducibles
# (importante en un pipeline: alguien mas debe poder correr esto y obtener lo mismo)
SEED = 42
random.seed(SEED)
fake = Faker("es_MX")
Faker.seed(SEED)

# --- Configuracion de volumen ---
N_PACIENTES = 5000
N_DOCTORES = 50
N_CITAS = 8000
PORC_CITAS_CON_LAB = 0.4  # no todas las citas generan resultados de laboratorio

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "source_mock"

ESPECIALIDADES = [
    "Medicina General", "Pediatria", "Cardiologia", "Dermatologia",
    "Ginecologia", "Traumatologia", "Oftalmologia", "Psiquiatria",
    "Endocrinologia", "Neurologia",
]

TIPOS_EXAMEN = [
    "Biometria Hematica", "Quimica Sanguinea", "Examen General de Orina",
    "Perfil Lipidico", "Prueba de Glucosa", "Radiografia de Torax",
]

ESTADOS_CITA = ["completada", "cancelada", "no_asistio"]
# Pesos realistas: la mayoria de las citas se completan
PESOS_ESTADO = [0.82, 0.10, 0.08]


def generar_pacientes(n: int) -> pd.DataFrame:
    """Genera la tabla de pacientes con datos demograficos basicos."""
    registros = []
    for i in range(1, n + 1):
        genero = random.choice(["M", "F"])
        nombre = fake.first_name_male() if genero == "M" else fake.first_name_female()
        registros.append({
            "id_paciente": i,
            "nombre": f"{nombre} {fake.last_name()}",
            "fecha_nacimiento": fake.date_of_birth(minimum_age=0, maximum_age=95),
            "genero": genero,
            "ciudad": fake.city(),
            "email": fake.email(),
            "telefono": fake.phone_number(),
            "fecha_registro": fake.date_between(start_date="-5y", end_date="today"),
        })
    return pd.DataFrame(registros)


def generar_doctores(n: int) -> pd.DataFrame:
    """Genera la tabla de doctores con su especialidad y experiencia."""
    registros = []
    for i in range(1, n + 1):
        registros.append({
            "id_doctor": i,
            "nombre": f"Dr. {fake.first_name()} {fake.last_name()}",
            "especialidad": random.choice(ESPECIALIDADES),
            "anios_experiencia": random.randint(1, 35),
            "cedula_profesional": fake.unique.numerify("########"),
        })
    return pd.DataFrame(registros)


def generar_citas(n: int, n_pacientes: int, n_doctores: int) -> pd.DataFrame:
    """
    Genera la tabla de citas, vinculando pacientes y doctores.

    Aqui a proposito metemos algo de "desorden real":
    - tiempos de espera variables
    - algunas fechas nulas simulando citas mal registradas (para probar
      las reglas de calidad de datos en la capa Silver mas adelante)
    """
    registros = []
    fecha_inicio = datetime.now() - timedelta(days=730)  # ultimos 2 anios

    for i in range(1, n + 1):
        fecha_cita = fecha_inicio + timedelta(
            days=random.randint(0, 730),
            hours=random.randint(7, 19),
            minutes=random.choice([0, 15, 30, 45]),
        )

        # ~2% de citas con fecha nula, simulando errores de captura reales
        fecha_final = None if random.random() < 0.02 else fecha_cita

        registros.append({
            "id_cita": i,
            "id_paciente": random.randint(1, n_pacientes),
            "id_doctor": random.randint(1, n_doctores),
            "fecha_hora": fecha_final,
            "duracion_minutos": random.choice([15, 20, 30, 45, 60]),
            "tiempo_espera_minutos": max(0, int(random.gauss(18, 12))),
            "motivo": fake.sentence(nb_words=6),
            "costo": round(random.uniform(250, 2500), 2),
            "estado": random.choices(ESTADOS_CITA, weights=PESOS_ESTADO)[0],
        })
    return pd.DataFrame(registros)


def generar_resultados_lab(citas_df: pd.DataFrame, porcentaje: float) -> pd.DataFrame:
    """
    Genera resultados de laboratorio SOLO para citas completadas.
    Esto refleja una regla de negocio real: no se generan resultados
    de laboratorio para citas canceladas o no asistidas.
    """
    citas_completadas = citas_df[citas_df["estado"] == "completada"]
    citas_con_lab = citas_completadas.sample(frac=porcentaje, random_state=SEED)

    registros = []
    for idx, (_, cita) in enumerate(citas_con_lab.iterrows(), start=1):
        registros.append({
            "id_resultado": idx,
            "id_cita": cita["id_cita"],
            "tipo_examen": random.choice(TIPOS_EXAMEN),
            "resultado": round(random.uniform(0.5, 200), 2),
            "valor_referencia": "Normal" if random.random() > 0.15 else "Fuera de rango",
            "fecha_resultado": cita["fecha_hora"],
        })
    return pd.DataFrame(registros)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Generando pacientes...")
    pacientes = generar_pacientes(N_PACIENTES)

    print("Generando doctores...")
    doctores = generar_doctores(N_DOCTORES)

    print("Generando citas...")
    citas = generar_citas(N_CITAS, N_PACIENTES, N_DOCTORES)

    print("Generando resultados de laboratorio...")
    resultados_lab = generar_resultados_lab(citas, PORC_CITAS_CON_LAB)

    # Guardado en CSV, simulando el formato tipico de un dump de base de datos
    pacientes.to_csv(OUTPUT_DIR / "pacientes.csv", index=False)
    doctores.to_csv(OUTPUT_DIR / "doctores.csv", index=False)
    citas.to_csv(OUTPUT_DIR / "citas.csv", index=False)
    resultados_lab.to_csv(OUTPUT_DIR / "resultados_lab.csv", index=False)

    print("\n--- Resumen ---")
    print(f"Pacientes:        {len(pacientes):,} filas")
    print(f"Doctores:         {len(doctores):,} filas")
    print(f"Citas:            {len(citas):,} filas")
    print(f"Resultados lab:   {len(resultados_lab):,} filas")
    print(f"\nArchivos guardados en: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
