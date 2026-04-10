#!/usr/bin/env python3

from datetime import datetime, UTC
from sqlalchemy import Column, Integer, String, Float, DateTime, Index
from scripts.database import Base


class RegistroWorldBank(Base):
    """Modelo para datos extraídos desde la API del World Bank"""
    __tablename__ = "worldbank_registros"

    id = Column(Integer, primary_key=True, autoincrement=True)

    pais = Column(String(100), nullable=False, index=True)
    pais_code = Column(String(10), nullable=True, index=True)

    anio = Column(Integer, nullable=False, index=True)

    indicador = Column(String(255), nullable=False, index=True)
    indicador_code = Column(String(50), nullable=True, index=True)

    valor = Column(Float, nullable=True)

    fuente = Column(String(100), nullable=False, default="World Bank")

    fecha_extraccion = Column(DateTime, default=lambda: datetime.now(UTC), index=True)
    fecha_creacion = Column(DateTime, default=lambda: datetime.now(UTC))

    __table_args__ = (
        Index("idx_pais_anio", "pais", "anio"),
        Index("idx_pais_indicador_anio", "pais", "indicador", "anio"),
    )

    def __repr__(self):
        return (
            f"<RegistroWorldBank(id={self.id}, pais='{self.pais}', "
            f"anio={self.anio}, indicador='{self.indicador}', valor={self.valor})>"
        )


class MetricasETL(Base):
    """Modelo para registrar métricas de cada ejecución del ETL"""
    __tablename__ = "etl_metricas"

    id = Column(Integer, primary_key=True, autoincrement=True)

    fecha_ejecucion = Column(DateTime, default=lambda: datetime.now(UTC), index=True)

    registros_extraidos = Column(Integer, nullable=False, default=0)
    registros_guardados = Column(Integer, nullable=False, default=0)
    registros_fallidos = Column(Integer, nullable=False, default=0)

    tiempo_ejecucion_segundos = Column(Float, nullable=False, default=0.0)

    estado = Column(String(50), nullable=False)  # SUCCESS, PARTIAL, FAILED
    mensaje = Column(String(500), nullable=True)

    def __repr__(self):
        return (
            f"<MetricasETL(id={self.id}, fecha_ejecucion={self.fecha_ejecucion}, "
            f"estado='{self.estado}', guardados={self.registros_guardados})>"
        )