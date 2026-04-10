#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")

import streamlit as st
import pandas as pd
import plotly.express as px

from scripts.database import SessionLocal
from scripts.models import RegistroWorldBank

st.set_page_config(
    page_title="WorldBank ETL Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📊 Dashboard World Bank - ETL")
st.markdown("---")

db = SessionLocal()

try:
    rows = db.query(
        RegistroWorldBank.pais,
        RegistroWorldBank.anio,
        RegistroWorldBank.indicador,
        RegistroWorldBank.valor
    ).all()

    if not rows:
        st.warning("No hay datos en la BD. Ejecuta el extractor primero.")
        st.stop()

    df = pd.DataFrame(rows, columns=["Pais", "Anio", "Indicador", "Valor"])
    df = df.dropna(subset=["Valor"])

    # Sidebar filtros
    st.sidebar.title("🔧 Filtros")
    paises = st.sidebar.multiselect(
        "Países",
        options=sorted(df["Pais"].unique()),
        default=sorted(df["Pais"].unique())
    )

    indicadores = st.sidebar.multiselect(
        "Indicadores",
        options=sorted(df["Indicador"].unique()),
        default=sorted(df["Indicador"].unique())[:2]
    )

    df_f = df[df["Pais"].isin(paises) & df["Indicador"].isin(indicadores)].copy()

    if df_f.empty:
        st.warning("No hay datos con esos filtros.")
        st.stop()

    # KPIs
    col1, col2, col3 = st.columns(3)
    col1.metric("🌍 Países", df_f["Pais"].nunique())
    col2.metric("📌 Indicadores", df_f["Indicador"].nunique())
    col3.metric("📊 Registros", len(df_f))

    st.markdown("---")

    # 1) Barras comparativas (último año)
    ultimo_anio = int(df_f["Anio"].max())
    df_ultimo = df_f[df_f["Anio"] == ultimo_anio]

    st.subheader(f"📊 Comparación por país (Año {ultimo_anio})")
    fig_bar = px.bar(
        df_ultimo,
        x="Pais",
        y="Valor",
        color="Indicador",
        barmode="group",
        title="Último año disponible por país"
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("---")

    # 2) Línea temporal (tendencia)
    st.subheader("📈 Tendencia histórica (línea temporal)")
    fig_line = px.line(
        df_f.sort_values("Anio"),
        x="Anio",
        y="Valor",
        color="Pais",
        line_group="Indicador",
        facet_col="Indicador",
        markers=False,
        title="Evolución histórica por país"
    )
    st.plotly_chart(fig_line, use_container_width=True)

    st.markdown("---")

    st.subheader("📋 Datos")
    st.dataframe(
        df_f.sort_values(["Pais", "Indicador", "Anio"], ascending=[True, True, False]),
        use_container_width=True,
        height=520
    )

finally:
    db.close()