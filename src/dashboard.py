"""
dashboard.py

Dashboard interactivo construido sobre la capa Gold del pipeline Medallion.
Lee directamente los Parquet de data/gold/ (sin conexion a base de datos),
demostrando que un modelo dimensional bien construido puede alimentar
analitica sin pasos adicionales.

Uso:
    streamlit run src/dashboard.py
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent.parent
GOLD_DIR = BASE_DIR / "data" / "gold"

st.set_page_config(
    page_title="HospitalDB Analytics",
    page_icon="🏥",
    layout="wide",
)


@st.cache_data
def cargar_datos():
    """
    Carga las 4 tablas de Gold. Cacheado con st.cache_data para que
    Streamlit no vuelva a leer los Parquet en cada interaccion del usuario
    (cada click en un filtro re-ejecuta el script completo por diseño de
    Streamlit; el cache evita relecturas innecesarias de disco).
    """
    fact_citas = pd.read_parquet(GOLD_DIR / "fact_citas")
    dim_paciente = pd.read_parquet(GOLD_DIR / "dim_paciente")
    dim_doctor = pd.read_parquet(GOLD_DIR / "dim_doctor")
    dim_tiempo = pd.read_parquet(GOLD_DIR / "dim_tiempo")

    # Union de todas las dimensiones al hecho, una sola vez, para no
    # repetir estos joins en cada grafico
    df = (
        fact_citas
        .merge(dim_doctor, on="id_doctor", suffixes=("", "_doctor"))
        .merge(dim_tiempo, on="id_tiempo", suffixes=("", "_tiempo"))
        .merge(dim_paciente, on="id_paciente", suffixes=("", "_paciente"))
    )
    return df, fact_citas, dim_doctor, dim_tiempo


def main():
    st.title("🏥 HospitalDB — Dashboard de Analítica")
    st.caption(
        "Datos sintéticos generados con Faker · Pipeline Medallion "
        "(Bronze → Silver → Gold) construido con PySpark"
    )

    df, fact_citas, dim_doctor, dim_tiempo = cargar_datos()

    # --- Filtros en la barra lateral ---
    st.sidebar.header("Filtros")

    especialidades = sorted(df["especialidad"].dropna().unique())
    especialidad_sel = st.sidebar.multiselect(
        "Especialidad", especialidades, default=especialidades
    )

    solo_datos_limpios = st.sidebar.checkbox(
        "Excluir citas con problemas de calidad de datos",
        value=False,
        help="Excluye citas marcadas con data_quality_flag en la capa Silver "
             "(ej. fecha de la cita no registrada).",
    )

    df_filtrado = df[df["especialidad"].isin(especialidad_sel)]
    if solo_datos_limpios:
        df_filtrado = df_filtrado[~df_filtrado["tiene_flag_calidad"]]

    # --- KPIs principales ---
    col1, col2, col3, col4 = st.columns(4)

    total_citas = len(df_filtrado)
    tiempo_espera_prom = df_filtrado["tiempo_espera_minutos"].mean()
    ingresos_totales = df_filtrado["costo"].sum()
    pct_con_flag = (df_filtrado["tiene_flag_calidad"].sum() / total_citas * 100) if total_citas > 0 else 0

    col1.metric("Total de citas", f"{total_citas:,}")
    col2.metric("Tiempo de espera promedio", f"{tiempo_espera_prom:.1f} min")
    col3.metric("Ingresos totales", f"${ingresos_totales:,.0f}")
    col4.metric("% citas con flag de calidad", f"{pct_con_flag:.1f}%")

    st.divider()

    # --- Fila 1: citas por mes + distribucion por estado ---
    col_izq, col_der = st.columns(2)

    with col_izq:
        st.subheader("Citas por mes")
        citas_por_mes = (
            df_filtrado[df_filtrado["id_tiempo"] != -1]
            .groupby(["anio", "mes"])
            .size()
            .reset_index(name="total_citas")
        )
        citas_por_mes["periodo"] = (
            citas_por_mes["anio"].astype(int).astype(str) + "-"
            + citas_por_mes["mes"].astype(int).astype(str).str.zfill(2)
        )
        fig_mes = px.line(citas_por_mes, x="periodo", y="total_citas", markers=True)
        fig_mes.update_layout(xaxis_title="Mes", yaxis_title="Citas")
        st.plotly_chart(fig_mes, use_container_width=True)

    with col_der:
        st.subheader("Distribución de citas por estado")
        dist_estado = df_filtrado["estado"].value_counts().reset_index()
        dist_estado.columns = ["estado", "total"]
        fig_estado = px.pie(dist_estado, names="estado", values="total", hole=0.4)
        st.plotly_chart(fig_estado, use_container_width=True)

    # --- Fila 2: tiempo de espera por especialidad + top doctores ---
    col_izq2, col_der2 = st.columns(2)

    with col_izq2:
        st.subheader("Tiempo de espera promedio por especialidad")
        espera_especialidad = (
            df_filtrado.groupby("especialidad")["tiempo_espera_minutos"]
            .mean()
            .round(1)
            .sort_values(ascending=True)
            .reset_index()
        )
        fig_espera = px.bar(
            espera_especialidad, x="tiempo_espera_minutos", y="especialidad",
            orientation="h",
        )
        fig_espera.update_layout(xaxis_title="Minutos", yaxis_title="")
        st.plotly_chart(fig_espera, use_container_width=True)

    with col_der2:
        st.subheader("Top 10 doctores por número de citas")
        top_doctores = (
            df_filtrado.groupby("nombre")
            .size()
            .sort_values(ascending=False)
            .head(10)
            .reset_index(name="total_citas")
        )
        fig_doctores = px.bar(top_doctores, x="total_citas", y="nombre", orientation="h")
        fig_doctores.update_layout(yaxis={"categoryorder": "total ascending"}, yaxis_title="")
        st.plotly_chart(fig_doctores, use_container_width=True)

    # --- Nota de calidad de datos, transparencia con el usuario del dashboard ---
    st.divider()
    with st.expander("ℹ️ Nota sobre calidad de datos"):
        st.write(
            f"""
            Este dashboard está construido sobre un pipeline con arquitectura
            Medallion (Bronze → Silver → Gold). En la capa Silver se detectaron
            y **marcaron** (no se eliminaron) registros con problemas de captura,
            como citas sin fecha registrada. Estas citas se enlazan a un registro
            especial "Fecha Desconocida" en la dimensión de tiempo, siguiendo la
            técnica estándar de modelado dimensional de Kimball.

            Actualmente **{pct_con_flag:.1f}%** de las citas mostradas tienen
            al menos un flag de calidad de datos. Usa el filtro en la barra
            lateral para excluirlas si tu análisis lo requiere.
            """
        )


if __name__ == "__main__":
    main()
