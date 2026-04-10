#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import func

from scripts.database import SessionLocal
from scripts.models import RegistroWorldBank, MetricasETL

st.set_page_config(
    page_title="World Bank Interactive Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📊 Dashboard Interactivo - World Bank ETL")
st.markdown("Analiza indicadores económicos almacenados en PostgreSQL.")
st.markdown("---")


@st.cache_data(ttl=300)
def cargar_datos():
    db = SessionLocal()
    try:
        rows = db.query(
            RegistroWorldBank.pais,
            RegistroWorldBank.pais_code,
            RegistroWorldBank.anio,
            RegistroWorldBank.indicador,
            RegistroWorldBank.indicador_code,
            RegistroWorldBank.valor,
            RegistroWorldBank.fuente,
            RegistroWorldBank.fecha_extraccion,
        ).all()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(
            rows,
            columns=[
                "Pais",
                "PaisCode",
                "Anio",
                "Indicador",
                "IndicadorCode",
                "Valor",
                "Fuente",
                "FechaExtraccion",
            ],
        )

        df = df.dropna(subset=["Valor"])
        if not df.empty:
            df["Anio"] = df["Anio"].astype(int)
            df = df.sort_values(["Pais", "Indicador", "Anio", "FechaExtraccion"], ascending=[True, True, True, False])
            df = df.drop_duplicates(subset=["PaisCode", "Anio", "IndicadorCode"], keep="first")

        return df
    finally:
        db.close()


@st.cache_data(ttl=300)
def cargar_metricas():
    db = SessionLocal()
    try:
        rows = db.query(
            MetricasETL.fecha_ejecucion,
            MetricasETL.registros_extraidos,
            MetricasETL.registros_guardados,
            MetricasETL.registros_fallidos,
            MetricasETL.tiempo_ejecucion_segundos,
            MetricasETL.estado,
            MetricasETL.mensaje,
        ).order_by(MetricasETL.fecha_ejecucion.desc()).all()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(
            rows,
            columns=[
                "FechaEjecucion",
                "Extraidos",
                "Guardados",
                "Fallidos",
                "TiempoSegundos",
                "Estado",
                "Mensaje",
            ],
        )
    finally:
        db.close()


def detectar_indicadores(df: pd.DataFrame):
    indicadores = sorted(df["Indicador"].dropna().unique())
    candidatos_pib = [i for i in indicadores if "GDP" in i or "PIB" in i]
    candidatos_inf = [i for i in indicadores if "Inflation" in i or "Inflacion" in i]
    candidatos_pob = [i for i in indicadores if "Population" in i or "Poblacion" in i]
    return indicadores, candidatos_pib, candidatos_inf, candidatos_pob


df = cargar_datos()
metricas_df = cargar_metricas()

if df.empty:
    st.warning("No hay datos en la base de datos. Ejecuta primero el extractor.")
    st.stop()

indicadores, candidatos_pib, candidatos_inf, candidatos_pob = detectar_indicadores(df)

st.sidebar.header("🔧 Filtros")

paises_disponibles = sorted(df["Pais"].unique())
indicadores_disponibles = indicadores
anios_disponibles = sorted(df["Anio"].unique())

paises_sel = st.sidebar.multiselect(
    "Países",
    options=paises_disponibles,
    default=paises_disponibles[: min(5, len(paises_disponibles))],
)

indicadores_sel = st.sidebar.multiselect(
    "Indicadores",
    options=indicadores_disponibles,
    default=indicadores_disponibles[: min(3, len(indicadores_disponibles))],
)

rango_anios = st.sidebar.slider(
    "Rango de años",
    min_value=min(anios_disponibles),
    max_value=max(anios_disponibles),
    value=(max(min(anios_disponibles), max(anios_disponibles) - 10), max(anios_disponibles)),
)

solo_ultimo = st.sidebar.checkbox("Mostrar solo último año por país/indicador", value=False)

filtro = (
    df["Pais"].isin(paises_sel)
    & df["Indicador"].isin(indicadores_sel)
    & df["Anio"].between(rango_anios[0], rango_anios[1])
)

df_f = df[filtro].copy()

if solo_ultimo and not df_f.empty:
    idx = df_f.groupby(["Pais", "Indicador"])["Anio"].idxmax()
    df_f = df_f.loc[idx].sort_values(["Pais", "Indicador"])

if df_f.empty:
    st.warning("No hay datos con los filtros seleccionados.")
    st.stop()

col1, col2, col3, col4 = st.columns(4)
col1.metric("🌍 Países", df_f["Pais"].nunique())
col2.metric("📌 Indicadores", df_f["Indicador"].nunique())
col3.metric("📊 Registros", len(df_f))
col4.metric("🗓️ Año máximo", int(df_f["Anio"].max()))

st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs(["📊 General", "📈 Histórico", "🔍 Relación", "🧾 Métricas ETL"])

with tab1:
    st.subheader("Comparación por país")
    ultimo_anio = int(df_f["Anio"].max())
    df_ultimo = df_f[df_f["Anio"] == ultimo_anio]

    fig_bar = px.bar(
        df_ultimo,
        x="Pais",
        y="Valor",
        color="Indicador",
        barmode="group",
        title=f"Comparación por país en {ultimo_anio}",
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    st.subheader("Tabla de datos filtrados")
    st.dataframe(
        df_f.sort_values(["Pais", "Indicador", "Anio"], ascending=[True, True, False]),
        use_container_width=True,
        height=420,
    )

with tab2:
    st.subheader("Tendencia histórica")
    fig_line = px.line(
        df_f.sort_values(["Indicador", "Pais", "Anio"]),
        x="Anio",
        y="Valor",
        color="Pais",
        line_group="Indicador",
        facet_col="Indicador",
        facet_col_wrap=1,
        markers=True,
        title="Evolución histórica por país e indicador",
    )
    st.plotly_chart(fig_line, use_container_width=True)

    st.subheader("Heatmap anual")
    indicador_heat = st.selectbox(
        "Indicador para heatmap",
        options=indicadores_disponibles,
        index=0,
        key="heatmap_indicador",
    )
    df_heat = df[
        (df["Indicador"] == indicador_heat)
        & (df["Pais"].isin(paises_sel))
        & (df["Anio"].between(rango_anios[0], rango_anios[1]))
    ].copy()

    if not df_heat.empty:
        pivot_heat = df_heat.pivot_table(index="Pais", columns="Anio", values="Valor", aggfunc="first")
        fig_heat = px.imshow(
            pivot_heat,
            aspect="auto",
            labels=dict(x="Año", y="País", color="Valor"),
            title=f"Heatmap de {indicador_heat}",
        )
        st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.info("No hay datos suficientes para el heatmap.")

with tab3:
    st.subheader("Relación entre indicadores")

    ind_x_default = candidatos_pib[0] if candidatos_pib else indicadores_disponibles[0]
    ind_y_default = candidatos_inf[0] if candidatos_inf else indicadores_disponibles[min(1, len(indicadores_disponibles)-1)]

    ind_x = st.selectbox("Indicador eje X", options=indicadores_disponibles, index=indicadores_disponibles.index(ind_x_default))
    ind_y = st.selectbox("Indicador eje Y", options=indicadores_disponibles, index=indicadores_disponibles.index(ind_y_default))

    df_rel = df[
        df["Indicador"].isin([ind_x, ind_y])
        & df["Pais"].isin(paises_sel)
        & df["Anio"].between(rango_anios[0], rango_anios[1])
    ].copy()

    pivot_rel = df_rel.pivot_table(
        index=["Pais", "Anio"],
        columns="Indicador",
        values="Valor",
        aggfunc="first",
    ).reset_index().dropna()

    if ind_x in pivot_rel.columns and ind_y in pivot_rel.columns and not pivot_rel.empty:
        fig_scatter = px.scatter(
            pivot_rel,
            x=ind_x,
            y=ind_y,
            color="Pais",
            hover_data=["Anio"],
            title=f"{ind_x} vs {ind_y}",
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.info("No hay suficientes datos coincidentes para comparar esos indicadores.")

    st.subheader("Boxplot por indicador")
    indicador_box = st.selectbox("Indicador para boxplot", options=indicadores_disponibles, key="boxplot_indicador")
    df_box = df[
        (df["Indicador"] == indicador_box)
        & (df["Pais"].isin(paises_sel))
        & (df["Anio"].between(rango_anios[0], rango_anios[1]))
    ].copy()

    if not df_box.empty:
        fig_box = px.box(
            df_box,
            x="Pais",
            y="Valor",
            title=f"Distribución de {indicador_box}",
        )
        st.plotly_chart(fig_box, use_container_width=True)
    else:
        st.info("No hay datos para el boxplot.")

with tab4:
    st.subheader("Historial de ejecuciones ETL")
    if metricas_df.empty:
        st.info("No hay métricas ETL registradas.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Último estado", metricas_df.iloc[0]["Estado"])
        c2.metric("Últimos guardados", int(metricas_df.iloc[0]["Guardados"]))
        c3.metric("Último tiempo (s)", f"{metricas_df.iloc[0]['TiempoSegundos']:.2f}")

        fig_metricas = px.bar(
            metricas_df.head(10),
            x="FechaEjecucion",
            y=["Extraidos", "Guardados", "Fallidos"],
            barmode="group",
            title="Últimas 10 ejecuciones del ETL",
        )
        st.plotly_chart(fig_metricas, use_container_width=True)

        st.dataframe(metricas_df, use_container_width=True, height=320)
