#!/usr/bin/env python3

import os
import requests
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

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
                            "año": int(item["date"]),
                            "indicador": item["indicator"]["value"],
                            "valor": item["value"],
                            "fecha_extraccion": datetime.now().isoformat()
                        })

                page += 1

    df = pd.DataFrame(all_data)

    print(f"\n📊 Total registros descargados: {len(df)}")
    if not df.empty:
        print(f"🌎 Países encontrados: {df['pais'].unique()}")

    return df

if __name__ == "__main__":
    logging.info("🚀 Iniciando extracción")
    df = extract_data()

    if df.empty:
        logging.warning("⚠ No se descargaron datos.")
    else:
        df.to_csv(RAW_DATA_PATH, index=False)
        logging.info(f"✅ Archivo RAW guardado en {RAW_DATA_PATH}")

    logging.info("🏁 Extracción finalizada")