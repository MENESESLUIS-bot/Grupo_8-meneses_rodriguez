#!/usr/bin/env python3
import os
import requests
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

# ✅ BD
from scripts.database import SessionLocal
from scripts.models import RegistroWorldBank

load_dotenv()

BASE_URL = os.getenv("BASE_URL")
COUNTRIES = os.getenv("COUNTRIES").split(",")
INDICATORS = os.getenv("INDICATORS").split(",")
FORMAT = os.getenv("FORMAT")
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH")

os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE_PATH), logging.StreamHandler()]
)

def extract_data():
    all_data = []

    print("\n🌍 Países que se van a consultar:")
    print(COUNTRIES)

    for country in COUNTRIES:
        for indicator in INDICATORS:
            page = 1
            total_pages = 1

            while page <= total_pages:
                url = f"{BASE_URL}/country/{country}/indicator/{indicator}?format={FORMAT}&page={page}"
                try:
                    r = requests.get(url, timeout=20)
                    r.raise_for_status()
                except requests.RequestException as e:
                    logging.error(f"Error consultando {country}-{indicator} (page={page}): {e}")
                    break

                data = r.json()
                if len(data) < 2 or data[1] is None:
                    break

                total_pages = int(data[0].get("pages", 1))

                for item in data[1]:
                    if item and item.get("value") is not None:
                        all_data.append({
                            "pais": item["country"]["value"],
                            "pais_code": item["country"]["id"] if item.get("country") else country,
                            "anio": int(item["date"]),
                            "indicador": item["indicator"]["value"],
                            "indicador_code": item["indicator"]["id"] if item.get("indicator") else indicator,
                            "valor": float(item["value"]) if item["value"] is not None else None,
                            "fecha_extraccion": datetime.utcnow()
                        })

                page += 1

    df = pd.DataFrame(all_data)

    print(f"\n📊 Total registros descargados: {len(df)}")
    if not df.empty:
        print(f"🌎 Países encontrados: {df['pais'].unique()}")

    return df


def load_to_db(df: pd.DataFrame):
    """Carga el dataframe a PostgreSQL en la tabla worldbank_registros."""
    if df.empty:
        logging.warning("⚠ No hay datos para cargar a la BD.")
        return

    db = SessionLocal()
    try:
        # ✅ Inserción masiva
        records = []
        for row in df.itertuples(index=False):
            records.append(
                RegistroWorldBank(
                    pais=row.pais,
                    pais_code=getattr(row, "pais_code", None),
                    anio=int(row.anio),
                    indicador=row.indicador,
                    indicador_code=getattr(row, "indicador_code", None),
                    valor=float(row.valor) if row.valor is not None else None,
                    fecha_extraccion=row.fecha_extraccion
                )
            )

        db.bulk_save_objects(records)
        db.commit()
        logging.info(f"✅ Cargados en BD: {len(records)} registros")

    except Exception as e:
        db.rollback()
        logging.error(f"❌ Error cargando a BD: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    logging.info("🚀 Iniciando extracción")
    df = extract_data()

    if df.empty:
        logging.warning("⚠ No se descargaron datos.")
    else:
        # ✅ Guardar RAW CSV
        df_out = df.copy()
        # para CSV, datetime a string
        df_out["fecha_extraccion"] = df_out["fecha_extraccion"].astype(str)
        df_out.to_csv(RAW_DATA_PATH, index=False)
        logging.info(f"✅ Archivo RAW guardado en {RAW_DATA_PATH}")

        # ✅ Cargar a PostgreSQL
        load_to_db(df)

    logging.info("🏁 Extracción finalizada")