# HospitalDB Medallion ETL

Pipeline de datos end-to-end que extrae información operacional de un sistema
hospitalario, la transforma con **PySpark** siguiendo arquitectura **Medallion**
(Bronze → Silver → Gold), y la modela en un **esquema estrella** listo para
analítica en un dashboard interactivo.

> 🔍 Este proyecto no es solo un ejercicio de ETL: fue diseñado para demostrar
> pensamiento de ingeniería de datos completo — manejo de datos incompletos sin
> perder trazabilidad, integridad referencial validada antes de publicar datos,
> y decisiones de arquitectura documentadas y justificadas.

## Estado del proyecto
✅ Pipeline completo funcional (extracción → limpieza → modelado → visualización)

## Demo en vivo
🔗 [Dashboard desplegado en Streamlit Cloud](https://hospitaldb-medallion-etl-vjnz8mypkhxkratpwnjkgv.streamlit.app/)

---

## Arquitectura

```
┌─────────────────────┐
│  Fuente de datos     │   Datos sintéticos generados con Faker
│  (data/source_mock)  │   (simulan HospitalDB_ITVH en MariaDB)
└──────────┬───────────┘
           │  extract.py
           ▼
┌─────────────────────┐
│      BRONZE          │   Datos crudos, sin transformar
│  (Parquet, particio-  │   Particionado por load_date
│   nado por fecha)     │
└──────────┬───────────┘
           │  transform_silver.py
           ▼
┌─────────────────────┐
│      SILVER          │   Limpieza, deduplicación, validación
│  (datos limpios,      │   Reglas de calidad documentadas en
│   con flags)           │   docs/data_dictionary.md
└──────────┬───────────┘
           │  transform_gold.py
           ▼
┌─────────────────────┐
│       GOLD            │   Esquema estrella:
│  (modelo dimensional) │   fact_citas + dim_paciente + dim_doctor
│                        │   + dim_tiempo
└──────────┬───────────┘
           │
           ▼
┌─────────────────────┐
│  Dashboard Streamlit  │   KPIs, filtros interactivos,
│                        │   transparencia de calidad de datos
└─────────────────────┘

           run_pipeline.py orquesta las 3 fases en orden,
           deteniéndose si alguna falla.
```

## Stack

| Capa | Herramienta | Por qué |
|---|---|---|
| Generación de datos | Faker (`es_MX`) | Evita usar datos reales de pacientes; reproducible con semilla fija |
| Procesamiento | PySpark 4.1.2 | Estándar de la industria para transformación de datos a escala |
| Almacenamiento | Parquet | Formato columnar, comprimido, con schema — más eficiente que CSV |
| Orquestación | Script Python custom | Simula dependencias entre tareas (ver "Próximos pasos") |
| Visualización | Streamlit + Plotly | Deployable gratis, queda en Python, se integra con el repo |

## Estructura del repositorio

```
hospitaldb-medallion-etl/
├── data/                    # No versionado (ver .gitignore)
│   ├── source_mock/         # Datos sintéticos generados
│   ├── bronze/               # Datos crudos particionados
│   ├── silver/                # Datos limpios con flags de calidad
│   └── gold/                  # Esquema estrella
├── src/
│   ├── generate_synthetic_data.py
│   ├── extract.py             # source_mock -> Bronze
│   ├── transform_silver.py    # Bronze -> Silver
│   ├── transform_gold.py      # Silver -> Gold
│   ├── run_pipeline.py        # Orquestador
│   └── dashboard.py           # Dashboard Streamlit
├── docs/
│   └── data_dictionary.md    # Reglas de calidad de datos
├── requirements.txt
└── README.md
```

## Cómo correrlo localmente

**Requisitos previos:**
- Python 3.10+
- Java 17 (requerido por PySpark)
- En Windows: `winutils.exe` y `HADOOP_HOME` configurados ([ver guía](https://github.com/cdarlint/winutils))

```bash
# 1. Clonar e instalar dependencias
git clone https://github.com/DiegoSant10/hospitaldb-medallion-etl.git
cd hospitaldb-medallion-etl
pip install -r requirements.txt

# 2. Generar datos sintéticos
python src/generate_synthetic_data.py

# 3. Correr el pipeline completo (extract -> silver -> gold)
python src/run_pipeline.py

# 4. Levantar el dashboard
streamlit run src/dashboard.py
```

## Decisiones de diseño clave

**1. Los datos incompletos se marcan, no se descartan.**
Cuando una cita llega sin fecha registrada (~2% de los casos, simulando
errores reales de captura), no se elimina del pipeline. Se conserva con
un flag `data_quality_flag`, y en la capa Gold se enlaza a un registro
especial "Fecha Desconocida" en `dim_tiempo` (técnica estándar de Kimball
para evitar nulls en llaves foráneas). Esto preserva trazabilidad completa:
se puede auditar exactamente cuántos y cuáles registros tienen problemas,
en vez de perderlos silenciosamente. Ver el detalle completo de reglas en
[`docs/data_dictionary.md`](docs/data_dictionary.md).

**2. Validación de integridad referencial antes de publicar en Gold.**
`transform_gold.py` valida que no existan llaves foráneas huérfanas
(citas apuntando a un doctor o paciente que no existe) antes de guardar
las tablas finales — el mismo tipo de chequeo que correría un test de
calidad de datos en producción.

**3. Datos sensibles excluidos de la capa analítica.**
`dim_paciente` no incluye email ni teléfono. Gold está pensado para
analítica agregada, no para operación diaria con datos personales.

**4. Full load, no incremental (limitación conocida).**
El pipeline recarga todas las tablas completas en cada corrida. En
producción, esto se resolvería con carga incremental vía una columna
`updated_at` o mecanismos de CDC (Change Data Capture), evitando releer
datos que no cambiaron.

## Calidad de datos

| Tabla | Filas totales | Filas con flag de calidad |
|---|---|---|
| pacientes | 5,000 | 0 |
| doctores | 50 | 0 |
| citas | 8,000 | 178 (2.2%) — fecha de cita no registrada |
| resultados_lab | 2,640 | 0 |

## Próximos pasos

- [ ] Migrar la orquestación de `run_pipeline.py` a **Apache Airflow** o
      **Microsoft Fabric Pipelines**, con reintentos automáticos y alertas
- [ ] Implementar carga incremental (CDC) en `extract.py`
- [ ] Conectar `extract.py` a MariaDB real vía JDBC en vez de CSV simulados
- [ ] Agregar tests automatizados con `pytest` (conteos esperados, ausencia
      de duplicados, integridad referencial)
- [ ] Migrar Gold a Delta Lake en Microsoft Fabric Lakehouse

## Autor

**Diego Santiago Silván** — Estudiante de Ingeniería en Sistemas
Computacionales (ITVH), especialización en ciencia de datos.
[LinkedIn](https://www.linkedin.com/in/diego-santiago-b65a412a8) · [GitHub](https://github.com/DiegoSant10)
