#!/usr/bin/env python3

import os
import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

FINAL_EXCEL_PATH = os.getenv("FINAL_EXCEL_PATH")
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH")

# Puedes controlar esto sin tocar código, si lo pones en .env:
# VIEW_LAST_YEARS=10
VIEW_LAST_YEARS = int(os.getenv("VIEW_LAST_YEARS", "10"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE_PATH), logging.StreamHandler()]
)

def visualize():
    logging.info("📊 Cargando archivo final...")

    if not os.path.exists(FINAL_EXCEL_PATH):
        logging.error(f"No existe el archivo: {FINAL_EXCEL_PATH}")
        return

    df = pd.read_excel(FINAL_EXCEL_PATH)
    if df.empty:
        logging.warning("El archivo final está vacío.")
        return

    logging.info(f"Total registros: {len(df)}")

    # Países disponibles
    paises = df["pais"].dropna().unique()
    print("\n🌎 Países disponibles:")
    print(paises)
    print(f"\nTotal países: {len(paises)}")

    # Determinar rango de años a mostrar
    max_year = int(df["anio"].max())
    min_year = max_year - (VIEW_LAST_YEARS - 1)

    print(f"\n📅 Mostrando últimos {VIEW_LAST_YEARS} años: {min_year}–{max_year}")

    # Filtrar a últimos N años
    df_last = df[df["anio"].between(min_year, max_year)].copy()

    # Mostrar por país (últimos N años)
    for pais in paises:
        bloque = df_last[df_last["pais"] == pais].sort_values("anio")
        print("\n==============================")
        print(f"📌 {pais}")
        print("==============================")
        print(bloque.to_string(index=False))

    # Resumen: último año disponible por país
    print("\n==============================")
    print("📌 Resumen: último anio disponible por país")
    print("==============================")
    idx = df.sort_values("anio").groupby("pais")["anio"].idxmax()
    resumen = df.loc[idx].sort_values("pais")
    print(resumen.to_string(index=False))

    print("\n🏁 Visualización completa")

if __name__ == "__main__":
    logging.info("🚀 Iniciando proceso ETL - VISUALIZE")
    visualize()
    logging.info("🏁 Visualización finalizada")