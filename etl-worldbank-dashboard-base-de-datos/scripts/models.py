#!/usr/bin/env python3
from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime
from scripts.database import Base

class RegistroWorldBank(Base):
    __tablename__ = "worldbank_registros"

    id = Column(Integer, primary_key=True, index=True)

    pais = Column(String, index=True, nullable=False)        # "Colombia"
    pais_code = Column(String, index=True, nullable=True)    # "COL" (si lo tienes)
    anio = Column(Integer, index=True, nullable=False)       # 1960..2024

    indicador = Column(String, index=True, nullable=False)   # "GDP (current US$)"
    indicador_code = Column(String, index=True, nullable=True)  # "NY.GDP.MKTP.CD" (si lo tienes)

    valor = Column(Float, nullable=True)                     # puede venir null
    fuente = Column(String, nullable=True, default="WorldBank")

    fecha_extraccion = Column(DateTime, default=datetime.utcnow, index=True)


class MetricasETL(Base):
    __tablename__ = "etl_metricas"

    id = Column(Integer, primary_key=True, index=True)

    fecha_ejecucion = Column(DateTime, default=datetime.utcnow, index=True)
    estado = Column(String, nullable=False, default="OK")  # OK / ERROR

    registros_extraidos = Column(Integer, default=0)
    registros_guardados = Column(Integer, default=0)
    registros_fallidos = Column(Integer, default=0)

    tiempo_ejecucion_segundos = Column(Float, default=0.0)
    detalle_error = Column(String, nullable=True)