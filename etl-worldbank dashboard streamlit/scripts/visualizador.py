#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")

import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from sqlalchemy import desc

from scripts.database import SessionLocal
from scripts.models import RegistroWorldBank

load_dotenv()

LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "logs/etl.log")
VIEW_LAST_YEARS = int(os.getenv("VIEW_LAST_YEARS", "10"))
OUTPUT_DIR = os.getenv("VISUAL_OUTPUT_DIR", "data/visualizaciones")

os.makedirs("logs", exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE_PATH), logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


def cargar_datos() -> pd.DataFrame | None:
    """Carga datos desde PostgreSQL."""
    db = SessionLocal()
    try:
        rows = db.query(
            RegistroWorldBank.pais,
            RegistroWorldBank.pais_code,
            RegistroWorldBank.anio,
            RegistroWorldBank.indicador,
            RegistroWorldBank.indicador_code,
            RegistroWorldBank.valor,
            RegistroWorldBank.fecha_extraccion
        ).all()

        if not rows:
            logger.warning("⚠ No hay datos en PostgreSQL.")
            return None

        df = pd.DataFrame(
            rows,
            columns=[
                "pais",
                "pais_code",
                "anio",
                "indicador",
                "indicador_code",
                "valor",
                "fecha_extraccion"
            ]
        )

        if df.empty:
            logger.warning("⚠ El DataFrame está vacío.")
            return None

        df["anio"] = df["anio"].astype(int)

        # Conservar solo el registro más reciente por país-año-indicador
        df = df.sort_values(
            ["pais", "anio", "indicador", "fecha_extraccion"],
            ascending=[True, True, True, False]
        )
        df = df.drop_duplicates(
            subset=["pais", "anio", "indicador"],
            keep="first"
        )

        logger.info(f"✅ Registros listos para visualización: {len(df)}")
        return df

    except Exception as e:
        logger.error(f"❌ Error cargando datos desde la BD: {e}")
        return None
    finally:
        db.close()


def detectar_indicadores(df: pd.DataFrame):
    indicadores = sorted(df["indicador"].dropna().unique())

    pib = next((i for i in indicadores if "GDP" in i or "PIB" in i), None)
    inflacion = next((i for i in indicadores if "Inflation" in i or "Inflacion" in i), None)
    poblacion = next((i for i in indicadores if "Population" in i or "Poblacion" in i), None)

    return pib, inflacion, poblacion


def filtrar_ultimos_anios(df: pd.DataFrame, years: int) -> pd.DataFrame:
    max_year = int(df["anio"].max())
    min_year = max_year - (years - 1)
    return df[df["anio"].between(min_year, max_year)].copy()


def grafica_linea_historica(df: pd.DataFrame, indicador: str, nombre_archivo: str):
    datos = df[df["indicador"] == indicador].copy()
    if datos.empty:
        logger.warning(f"⚠ No hay datos para línea histórica de {indicador}")
        return

    plt.figure(figsize=(12, 7))
    for pais in sorted(datos["pais"].unique()):
        bloque = datos[datos["pais"] == pais].sort_values("anio")
        plt.plot(bloque["anio"], bloque["valor"], marker="o", label=pais)

    plt.title(f"Evolución histórica - {indicador}")
    plt.xlabel("Año")
    plt.ylabel("Valor")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    ruta = os.path.join(OUTPUT_DIR, nombre_archivo)
    plt.savefig(ruta, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"✅ Gráfica guardada: {ruta}")


def grafica_barras_ultimo_anio(df: pd.DataFrame, indicador: str, nombre_archivo: str):
    datos = df[df["indicador"] == indicador].copy()
    if datos.empty:
        logger.warning(f"⚠ No hay datos para barras de {indicador}")
        return

    ultimo_anio = int(datos["anio"].max())
    datos = datos[datos["anio"] == ultimo_anio].sort_values("valor", ascending=False)

    plt.figure(figsize=(12, 7))
    plt.bar(datos["pais"], datos["valor"])
    plt.title(f"Comparación por país - {indicador} ({ultimo_anio})")
    plt.xlabel("País")
    plt.ylabel("Valor")
    plt.xticks(rotation=45, ha="right")
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    ruta = os.path.join(OUTPUT_DIR, nombre_archivo)
    plt.savefig(ruta, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"✅ Gráfica guardada: {ruta}")


def grafica_heatmap(df: pd.DataFrame, indicador: str, nombre_archivo: str):
    datos = df[df["indicador"] == indicador].copy()
    if datos.empty:
        logger.warning(f"⚠ No hay datos para heatmap de {indicador}")
        return

    pivot = datos.pivot_table(index="pais", columns="anio", values="valor", aggfunc="first")
    if pivot.empty:
        logger.warning(f"⚠ Pivot vacío para heatmap de {indicador}")
        return

    plt.figure(figsize=(12, 7))
    plt.imshow(pivot, aspect="auto")
    plt.title(f"Heatmap - {indicador}")
    plt.xlabel("Año")
    plt.ylabel("País")
    plt.xticks(range(len(pivot.columns)), pivot.columns, rotation=45)
    plt.yticks(range(len(pivot.index)), pivot.index)
    plt.colorbar(label="Valor")
    plt.tight_layout()
    ruta = os.path.join(OUTPUT_DIR, nombre_archivo)
    plt.savefig(ruta, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"✅ Gráfica guardada: {ruta}")


def grafica_scatter_pib_vs_inflacion(df: pd.DataFrame, pib: str, inflacion: str, nombre_archivo: str):
    datos = df[df["indicador"].isin([pib, inflacion])].copy()
    if datos.empty:
        logger.warning("⚠ No hay datos para scatter PIB vs Inflación")
        return

    pivot = datos.pivot_table(
        index=["pais", "anio"],
        columns="indicador",
        values="valor",
        aggfunc="first"
    ).reset_index().dropna()

    if pivot.empty or pib not in pivot.columns or inflacion not in pivot.columns:
        logger.warning("⚠ No hay datos coincidentes para scatter PIB vs Inflación")
        return

    plt.figure(figsize=(12, 7))
    for pais in sorted(pivot["pais"].unique()):
        bloque = pivot[pivot["pais"] == pais]
        plt.scatter(bloque[pib], bloque[inflacion], label=pais)

    plt.title(f"Relación entre {pib} e {inflacion}")
    plt.xlabel(pib)
    plt.ylabel(inflacion)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    ruta = os.path.join(OUTPUT_DIR, nombre_archivo)
    plt.savefig(ruta, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"✅ Gráfica guardada: {ruta}")


def grafica_boxplot(df: pd.DataFrame, indicador: str, nombre_archivo: str):
    datos = df[df["indicador"] == indicador].copy()
    if datos.empty:
        logger.warning(f"⚠ No hay datos para boxplot de {indicador}")
        return

    grupos = []
    etiquetas = []
    for pais in sorted(datos["pais"].unique()):
        serie = datos[datos["pais"] == pais]["valor"].dropna()
        if not serie.empty:
            grupos.append(serie)
            etiquetas.append(pais)

    if not grupos:
        logger.warning(f"⚠ No hay grupos válidos para boxplot de {indicador}")
        return

    plt.figure(figsize=(12, 7))
    plt.boxplot(grupos, tick_labels=etiquetas)
    plt.title(f"Distribución por país - {indicador}")
    plt.xlabel("País")
    plt.ylabel("Valor")
    plt.xticks(rotation=45, ha="right")
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    ruta = os.path.join(OUTPUT_DIR, nombre_archivo)
    plt.savefig(ruta, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"✅ Gráfica guardada: {ruta}")


def ranking_ultimo_anio(df: pd.DataFrame, indicador: str):
    datos = df[df["indicador"] == indicador].copy()
    if datos.empty:
        logger.warning(f"⚠ No hay datos para ranking de {indicador}")
        return

    ultimo_anio = int(datos["anio"].max())
    ranking = datos[datos["anio"] == ultimo_anio].sort_values("valor", ascending=False)

    print("\n" + "=" * 70)
    print(f"RANKING ÚLTIMO AÑO - {indicador} ({ultimo_anio})")
    print("=" * 70)
    print(ranking[["pais", "anio", "valor"]].to_string(index=False))
    print("=" * 70)


def mostrar_resumen(df: pd.DataFrame, pib: str | None, inflacion: str | None, poblacion: str | None):
    print("\n" + "=" * 70)
    print("VISUALIZACIÓN ECONÓMICA - WORLD BANK")
    print("=" * 70)
    print(f"Países disponibles: {df['pais'].nunique()}")
    print(f"Indicadores disponibles: {df['indicador'].nunique()}")
    print(f"Años disponibles: {df['anio'].min()} - {df['anio'].max()}")
    print(f"Registros: {len(df)}")
    print(f"Indicador PIB detectado: {pib}")
    print(f"Indicador Inflación detectado: {inflacion}")
    print(f"Indicador Población detectado: {poblacion}")
    print(f"Salida gráficas: {OUTPUT_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    logger.info("🚀 Iniciando proceso ETL - VISUALIZE")

    df = cargar_datos()

    if df is None or df.empty:
        logger.warning("⚠ No hay datos para visualizar.")
        raise SystemExit(1)

    df = filtrar_ultimos_anios(df, VIEW_LAST_YEARS)
    pib, inflacion, poblacion = detectar_indicadores(df)

    mostrar_resumen(df, pib, inflacion, poblacion)

    if pib:
        grafica_linea_historica(df, pib, "linea_historica_pib.png")
        grafica_barras_ultimo_anio(df, pib, "barras_ultimo_anio_pib.png")
        grafica_heatmap(df, pib, "heatmap_pib.png")
        ranking_ultimo_anio(df, pib)

    if inflacion:
        grafica_linea_historica(df, inflacion, "linea_historica_inflacion.png")
        grafica_boxplot(df, inflacion, "boxplot_inflacion.png")
        ranking_ultimo_anio(df, inflacion)

    if poblacion:
        grafica_linea_historica(df, poblacion, "linea_historica_poblacion.png")
        grafica_barras_ultimo_anio(df, poblacion, "barras_ultimo_anio_poblacion.png")

    if pib and inflacion:
        grafica_scatter_pib_vs_inflacion(df, pib, inflacion, "scatter_pib_vs_inflacion.png")

    logger.info("🏁 Visualización finalizada")