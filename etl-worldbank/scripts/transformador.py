#!/usr/bin/env python3

import os
import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
TRANSFORMED_DATA_PATH = os.getenv("TRANSFORMED_DATA_PATH")
FINAL_EXCEL_PATH = os.getenv("FINAL_EXCEL_PATH")
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE_PATH), logging.StreamHandler()]
)

def transform_data():
    logging.info("🔄 Cargando datos crudos...")

    if not os.path.exists(RAW_DATA_PATH):
        logging.error(f"No existe el archivo: {RAW_DATA_PATH}")
        return None

    df = pd.read_csv(RAW_DATA_PATH)
    if df.empty:
        logging.warning("El RAW está vacío.")
        return None

    logging.info(f"Registros cargados: {len(df)}")

    df["año"] = df["año"].astype(int)
    df = df.sort_values(["pais", "año"])

    df_pivot = df.pivot_table(
        index=["pais", "año"],
        columns="indicador",
        values="valor",
        aggfunc="first"
    ).reset_index()

    return df_pivot

if __name__ == "__main__":
    logging.info("🚀 Iniciando proceso ETL - TRANSFORM")
    df_final = transform_data()

    if df_final is not None:
        os.makedirs("data", exist_ok=True)
        df_final.to_csv(TRANSFORMED_DATA_PATH, index=False)
        df_final.to_excel(FINAL_EXCEL_PATH, index=False)

        logging.info(f"✅ CSV transformado guardado en {TRANSFORMED_DATA_PATH}")
        logging.info(f"✅ Excel final guardado en {FINAL_EXCEL_PATH}")
        logging.info(f"📊 Total registros transformados: {len(df_final)}")

    logging.info("🏁 Transformación finalizada")