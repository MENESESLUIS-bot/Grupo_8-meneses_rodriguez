#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")

import streamlit as st
import pandas as pd
import plotly.express as px

from scripts.database import SessionLocal
from scripts.models import RegistroWorldBank

st.set_page_config(
    page_title="WorldBank Dashboard Avanzado",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Dashboard Avanzado - World Bank ETL")
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

    tab1, tab2, tab3 = st.tabs(["📊 Vista General", "📈 Histórico", "🔍 Análisis"])

    # =======================
    # TAB 1 - General
    # =======================
    with tab1:
        st.subheader("Vista general")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("🌍 Países", df["Pais"].nunique())
        col2.metric("📌 Indicadores", df["Indicador"].nunique())
        col3.metric("📊 Registros", len(df))
        col4.metric("🗓️ Año máx", int(df["Anio"].max()))

        st.markdown("---")

        paises = st.multiselect(
            "Países",
            options=sorted(df["Pais"].unique()),
            default=sorted(df["Pais"].unique())[:5]
        )

        indicadores = st.multiselect(
            "Indicadores",
            options=sorted(df["Indicador"].unique()),
            default=sorted(df["Indicador"].unique())[:2]
        )

        df_f = df[df["Pais"].isin(paises) & df["Indicador"].isin(indicadores)].copy()

        if df_f.empty:
            st.warning("No hay datos con esos filtros.")
        else:
            ultimo_anio = int(df_f["Anio"].max())
            df_ultimo = df_f[df_f["Anio"] == ultimo_anio]

            st.subheader(f"📊 Barras (Año {ultimo_anio})")
            fig_bar = px.bar(
                df_ultimo,
                x="Pais",
                y="Valor",
                color="Indicador",
                barmode="group",
                title="Comparación por país (último año)"
            )
            st.plotly_chart(fig_bar, use_container_width=True)

            st.dataframe(df_f.sort_values(["Pais", "Indicador", "Anio"]), use_container_width=True, height=420)

    # =======================
    # TAB 2 - Histórico
    # =======================
    with tab2:
        st.subheader("Histórico")

        min_anio = int(df["Anio"].min())
        max_anio = int(df["Anio"].max())

        col1, col2, col3 = st.columns(3)
        with col1:
            paises_h = st.multiselect(
                "Países",
                options=sorted(df["Pais"].unique()),
                default=sorted(df["Pais"].unique())[:3]
            )
        with col2:
            indicador_h = st.selectbox("Indicador", options=sorted(df["Indicador"].unique()))
        with col3:
            rango = st.slider("Rango de años", min_anio, max_anio, (max(max_anio - 30, min_anio), max_anio))

        df_h = df[
            (df["Pais"].isin(paises_h)) &
            (df["Indicador"] == indicador_h) &
            (df["Anio"] >= rango[0]) &
            (df["Anio"] <= rango[1])
        ].copy()

        if df_h.empty:
            st.warning("No hay datos con esos filtros.")
        else:
            df_h = df_h.sort_values(["Pais", "Anio"])
            fig_line = px.line(
                df_h,
                x="Anio",
                y="Valor",
                color="Pais",
                markers=True,
                title=f"{indicador_h} ({rango[0]} - {rango[1]})"
            )
            st.plotly_chart(fig_line, use_container_width=True)

            st.dataframe(df_h, use_container_width=True, height=420)

    # =======================
    # TAB 3 - Análisis
    # =======================
    with tab3:
        st.subheader("Análisis (Scatter + Boxplot)")

        # Detectar indicadores clave por nombre (funciona con tus nombres en español/inglés)
        posibles_gdp = [i for i in df["Indicador"].unique() if "GDP" in i or "PIB" in i]
        posibles_inf = [i for i in df["Indicador"].unique() if "Inflation" in i or "Inflacion" in i]

        if not posibles_gdp or not posibles_inf:
            st.warning("No encuentro indicadores de PIB o Inflación en tus datos.")
        else:
            gdp_ind = st.selectbox("Indicador PIB", posibles_gdp)
            inf_ind = st.selectbox("Indicador Inflación", posibles_inf)

            # Pivot por país-año para cruzar PIB e inflación
            df_pivot = df[df["Indicador"].isin([gdp_ind, inf_ind])].pivot_table(
                index=["Pais", "Anio"],
                columns="Indicador",
                values="Valor"
            ).reset_index()

            df_pivot = df_pivot.dropna()

            if df_pivot.empty:
                st.warning("No hay años donde existan PIB e Inflación al mismo tiempo.")
            else:
                st.markdown("### 🔵 Scatter: PIB vs Inflación")
                fig_scatter = px.scatter(
                    df_pivot,
                    x=gdp_ind,
                    y=inf_ind,
                    color="Pais",
                    hover_data=["Anio"],
                    title="Relación PIB vs Inflación"
                )
                st.plotly_chart(fig_scatter, use_container_width=True)

                st.markdown("---")
                st.markdown("### 📦 Boxplot: Distribución de inflación por país")
                df_inf = df[df["Indicador"] == inf_ind].dropna()
                fig_box = px.box(
                    df_inf,
                    x="Pais",
                    y="Valor",
                    title="Distribución de Inflación (Boxplot)"
                )
                st.plotly_chart(fig_box, use_container_width=True)

finally:
    db.close()