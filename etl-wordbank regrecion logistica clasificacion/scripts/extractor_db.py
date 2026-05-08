#!/usr/bin/env python3

import os
import time
import logging
from datetime import datetime, UTC

import requests
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError

from scripts.database import SessionLocal
from scripts.models import RegistroWorldBank, MetricasETL

load_dotenv()

LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "logs/etl.log")
BASE_URL = os.getenv("BASE_URL")
COUNTRIES = [c.strip() for c in os.getenv("COUNTRIES", "").split(",") if c.strip()]
INDICATORS = [i.strip() for i in os.getenv("INDICATORS", "").split(",") if i.strip()]
FORMAT = os.getenv("FORMAT", "json")

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class WorldBankETLDB:
    def __init__(self):
        self.db = SessionLocal()
        self.tiempo_inicio = time.time()

        self.registros_extraidos = 0
        self.registros_guardados = 0
        self.registros_fallidos = 0
        self.registros_duplicados = 0

        if not BASE_URL:
            raise ValueError("BASE_URL no configurada en .env")
        if not COUNTRIES:
            raise ValueError("COUNTRIES no configurado en .env")
        if not INDICATORS:
            raise ValueError("INDICATORS no configurado en .env")

    def extraer_api(self, country: str, indicator: str):
        """Extrae datos paginados desde la API del World Bank."""
        resultados = []
        page = 1
        total_pages = 1

        while page <= total_pages:
            url = (
                f"{BASE_URL}/country/{country}/indicator/{indicator}"
                f"?format={FORMAT}&page={page}"
            )

            try:
                response = requests.get(url, timeout=20)
                response.raise_for_status()
                data = response.json()

                if len(data) < 2 or data[1] is None:
                    logger.warning(
                        f"⚠️ Respuesta vacía o inesperada para {country} - {indicator} - página {page}"
                    )
                    break

                total_pages = int(data[0].get("pages", 1))

                for item in data[1]:
                    if item and item.get("value") is not None:
                        resultados.append(item)

                logger.info(
                    f"✅ Extraída página {page}/{total_pages} para {country} - {indicator}"
                )
                page += 1

            except requests.RequestException as e:
                logger.error(
                    f"❌ Error consultando API para {country} - {indicator} - página {page}: {e}"
                )
                self.registros_fallidos += 1
                break
            except Exception as e:
                logger.error(
                    f"❌ Error inesperado procesando API para {country} - {indicator}: {e}"
                )
                self.registros_fallidos += 1
                break

        return resultados

    def procesar_item(self, item: dict, country_fallback: str, indicator_fallback: str):
        """Transforma un registro JSON de la API a diccionario listo para BD."""
        try:
            return {
                "pais": item["country"]["value"] if item.get("country") else country_fallback,
                "pais_code": item["country"]["id"] if item.get("country") else country_fallback,
                "anio": int(item["date"]),
                "indicador": item["indicator"]["value"] if item.get("indicator") else indicator_fallback,
                "indicador_code": item["indicator"]["id"] if item.get("indicator") else indicator_fallback,
                "valor": float(item["value"]) if item.get("value") is not None else None,
                "fuente": "World Bank",
                "fecha_extraccion": datetime.now(UTC),
            }
        except Exception as e:
            logger.error(f"❌ Error procesando item: {e}")
            self.registros_fallidos += 1
            return None

    def existe_registro(self, datos: dict) -> bool:
        """Verifica si ya existe el registro para evitar duplicados."""
        existente = (
            self.db.query(RegistroWorldBank)
            .filter(
                RegistroWorldBank.pais_code == datos["pais_code"],
                RegistroWorldBank.anio == datos["anio"],
                RegistroWorldBank.indicador_code == datos["indicador_code"],
            )
            .first()
        )
        return existente is not None

    def guardar_registro(self, datos: dict) -> bool:
        """Guarda un registro en PostgreSQL."""
        try:
            if self.existe_registro(datos):
                self.registros_duplicados += 1
                logger.info(
                    f"⏭️ Duplicado omitido: {datos['pais']} - {datos['anio']} - {datos['indicador']}"
                )
                return False

            registro = RegistroWorldBank(
                pais=datos["pais"],
                pais_code=datos["pais_code"],
                anio=datos["anio"],
                indicador=datos["indicador"],
                indicador_code=datos["indicador_code"],
                valor=datos["valor"],
                fuente=datos["fuente"],
                fecha_extraccion=datos["fecha_extraccion"],
            )

            self.db.add(registro)
            self.db.commit()
            self.registros_guardados += 1

            logger.info(
                f"💾 Guardado en BD: {datos['pais']} - {datos['anio']} - {datos['indicador']}"
            )
            return True

        except SQLAlchemyError as e:
            self.db.rollback()
            self.registros_fallidos += 1
            logger.error(f"❌ Error SQL guardando registro: {e}")
            return False
        except Exception as e:
            self.db.rollback()
            self.registros_fallidos += 1
            logger.error(f"❌ Error inesperado guardando registro: {e}")
            return False

    def guardar_metricas(self, estado: str, mensaje: str):
        """Guarda métricas de ejecución del ETL."""
        try:
            tiempo_ejecucion = time.time() - self.tiempo_inicio

            metricas = MetricasETL(
                registros_extraidos=self.registros_extraidos,
                registros_guardados=self.registros_guardados,
                registros_fallidos=self.registros_fallidos,
                tiempo_ejecucion_segundos=tiempo_ejecucion,
                estado=estado,
                mensaje=mensaje,
            )

            self.db.add(metricas)
            self.db.commit()

            logger.info("📈 Métricas guardadas correctamente")

        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ Error guardando métricas: {e}")

    def ejecutar(self) -> bool:
        """Ejecuta el pipeline ETL completo contra PostgreSQL."""
        try:
            logger.info(
                f"🚀 Iniciando ETL World Bank para {len(COUNTRIES)} países y {len(INDICATORS)} indicadores"
            )

            for country in COUNTRIES:
                for indicator in INDICATORS:
                    items = self.extraer_api(country, indicator)

                    for item in items:
                        datos = self.procesar_item(item, country, indicator)
                        if datos is not None:
                            self.registros_extraidos += 1
                            self.guardar_registro(datos)

            if self.registros_fallidos == 0:
                estado = "SUCCESS"
            elif self.registros_guardados > 0:
                estado = "PARTIAL"
            else:
                estado = "FAILED"

            mensaje = (
                f"Extraídos: {self.registros_extraidos}, "
                f"Guardados: {self.registros_guardados}, "
                f"Duplicados: {self.registros_duplicados}, "
                f"Fallidos: {self.registros_fallidos}"
            )

            self.guardar_metricas(estado, mensaje)
            logger.info(f"🏁 ETL finalizado con estado {estado}")
            return estado != "FAILED"

        except Exception as e:
            logger.error(f"❌ Error general en ETL: {e}")
            self.guardar_metricas("FAILED", f"Error general: {e}")
            return False

        finally:
            self.db.close()

    def mostrar_resumen(self):
        """Muestra resumen final de la carga en PostgreSQL."""
        db = SessionLocal()
        try:
            total_registros = db.query(RegistroWorldBank).count()
            total_metricas = db.query(MetricasETL).count()

            print("\n" + "=" * 60)
            print("RESUMEN ETL - DATOS EN POSTGRESQL")
            print("=" * 60)
            print(f"Registros extraídos:   {self.registros_extraidos}")
            print(f"Registros guardados:   {self.registros_guardados}")
            print(f"Registros duplicados:  {self.registros_duplicados}")
            print(f"Registros fallidos:    {self.registros_fallidos}")
            print(f"Total en BD:           {total_registros}")
            print(f"Ejecuciones ETL:       {total_metricas}")
            print("=" * 60 + "\n")

        except Exception as e:
            logger.error(f"❌ Error mostrando resumen: {e}")
        finally:
            db.close()


if __name__ == "__main__":
    etl = WorldBankETLDB()
    exito = etl.ejecutar()
    etl.mostrar_resumen()
    raise SystemExit(0 if exito else 1)