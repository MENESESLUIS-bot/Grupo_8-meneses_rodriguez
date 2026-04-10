#!/usr/bin/env python3
import sys
import logging

sys.path.insert(0, ".")

from scripts.database import test_connection, engine

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("🔍 PRUEBA DE CONEXIÓN - ETL WORLDBANK")
    print("=" * 60)

    if test_connection():
        print("✅ Conexión exitosa a PostgreSQL")
        print(f"📌 Base de datos : {engine.url.database}")
        print(f"🌐 Host          : {engine.url.host}")
        print(f"🔌 Puerto        : {engine.url.port}")
        print(f"👤 Usuario       : {engine.url.username}")
    else:
        print("❌ No se pudo conectar a PostgreSQL")
        print("\n⚠️ Verifica lo siguiente:")
        print("  - PostgreSQL está corriendo")
        print("  - Variables en .env son correctas")
        print("  - La base de datos 'etl_worldbank' existe")
        print("  - El usuario tiene permisos sobre esa BD")
        print("  - El puerto 5432 está disponible")

    print("=" * 60 + "\n")