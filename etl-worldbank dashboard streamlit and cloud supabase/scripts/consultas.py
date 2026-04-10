#!/usr/bin/env python3
import sys
sys.path.insert(0, ".")

import pandas as pd
from sqlalchemy import func

from scripts.database import SessionLocal
from scripts.models import RegistroWorldBank, MetricasETL

db = SessionLocal()


def indicadores_disponibles():
    """Muestra los indicadores disponibles en la base de datos."""
    registros = (
        db.query(
            RegistroWorldBank.indicador,
            func.count(RegistroWorldBank.id).label("total_registros")
        )
        .group_by(RegistroWorldBank.indicador)
        .order_by(RegistroWorldBank.indicador)
        .all()
    )

    df = pd.DataFrame(registros, columns=["Indicador", "Total Registros"])
    print("\n📊 INDICADORES DISPONIBLES:")
    print(df.to_string(index=False))


def ultimo_valor_por_pais():
    """Muestra el último valor disponible por país e indicador."""
    subq = (
        db.query(
            RegistroWorldBank.pais,
            RegistroWorldBank.indicador,
            func.max(RegistroWorldBank.anio).label("max_anio")
        )
        .group_by(RegistroWorldBank.pais, RegistroWorldBank.indicador)
        .subquery()
    )

    registros = (
        db.query(
            RegistroWorldBank.pais,
            RegistroWorldBank.indicador,
            RegistroWorldBank.anio,
            RegistroWorldBank.valor
        )
        .join(
            subq,
            (RegistroWorldBank.pais == subq.c.pais) &
            (RegistroWorldBank.indicador == subq.c.indicador) &
            (RegistroWorldBank.anio == subq.c.max_anio)
        )
        .order_by(RegistroWorldBank.pais, RegistroWorldBank.indicador)
        .all()
    )

    df = pd.DataFrame(registros, columns=["Pais", "Indicador", "Anio", "Valor"])
    print("\n🌍 ÚLTIMO VALOR DISPONIBLE POR PAÍS E INDICADOR:")
    print(df.to_string(index=False))


def pais_con_mayor_pib():
    """Identifica el país con mayor PIB en el último año disponible."""
    posibles_pib = (
        db.query(RegistroWorldBank.indicador)
        .filter(
            (RegistroWorldBank.indicador.ilike("%GDP%")) |
            (RegistroWorldBank.indicador.ilike("%PIB%"))
        )
        .distinct()
        .all()
    )

    posibles_pib = [x[0] for x in posibles_pib]

    if not posibles_pib:
        print("\n💰 No se encontró un indicador de PIB en la BD.")
        return

    indicador_pib = posibles_pib[0]

    max_anio = (
        db.query(func.max(RegistroWorldBank.anio))
        .filter(RegistroWorldBank.indicador == indicador_pib)
        .scalar()
    )

    registro = (
        db.query(
            RegistroWorldBank.pais,
            RegistroWorldBank.valor,
            RegistroWorldBank.anio
        )
        .filter(
            RegistroWorldBank.indicador == indicador_pib,
            RegistroWorldBank.anio == max_anio
        )
        .order_by(RegistroWorldBank.valor.desc())
        .first()
    )

    if registro:
        print(
            f"\n💰 PAÍS CON MAYOR PIB: {registro.pais} "
            f"con {registro.valor:.2f} en {registro.anio}"
        )


def pais_con_mayor_inflacion():
    """Identifica el país con mayor inflación en el último año disponible."""
    posibles_inf = (
        db.query(RegistroWorldBank.indicador)
        .filter(
            (RegistroWorldBank.indicador.ilike("%Inflation%")) |
            (RegistroWorldBank.indicador.ilike("%Inflacion%"))
        )
        .distinct()
        .all()
    )

    posibles_inf = [x[0] for x in posibles_inf]

    if not posibles_inf:
        print("\n📈 No se encontró un indicador de inflación en la BD.")
        return

    indicador_inf = posibles_inf[0]

    max_anio = (
        db.query(func.max(RegistroWorldBank.anio))
        .filter(RegistroWorldBank.indicador == indicador_inf)
        .scalar()
    )

    registro = (
        db.query(
            RegistroWorldBank.pais,
            RegistroWorldBank.valor,
            RegistroWorldBank.anio
        )
        .filter(
            RegistroWorldBank.indicador == indicador_inf,
            RegistroWorldBank.anio == max_anio
        )
        .order_by(RegistroWorldBank.valor.desc())
        .first()
    )

    if registro:
        print(
            f"\n📈 PAÍS CON MAYOR INFLACIÓN: {registro.pais} "
            f"con {registro.valor:.2f}% en {registro.anio}"
        )


def metricas_etl():
    """Muestra métricas de las últimas ejecuciones del ETL."""
    metricas = (
        db.query(MetricasETL)
        .order_by(MetricasETL.fecha_ejecucion.desc())
        .limit(5)
        .all()
    )

    print("\n📋 ÚLTIMAS 5 EJECUCIONES DEL ETL:")
    if not metricas:
        print("No hay métricas registradas.")
        return

    for m in metricas:
        print(
            f" - {m.fecha_ejecucion} | {m.estado} | "
            f"Extraídos: {m.registros_extraidos} | "
            f"Guardados: {m.registros_guardados} | "
            f"Fallidos: {m.registros_fallidos} | "
            f"Tiempo: {m.tiempo_ejecucion_segundos:.2f}s"
        )


if __name__ == "__main__":
    try:
        print("\n" + "=" * 60)
        print("ANÁLISIS DE DATOS - WORLD BANK EN POSTGRESQL")
        print("=" * 60)

        indicadores_disponibles()
        ultimo_valor_por_pais()
        pais_con_mayor_pib()
        pais_con_mayor_inflacion()
        metricas_etl()

        print("\n" + "=" * 60 + "\n")

    finally:
        db.close()