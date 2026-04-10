#!/usr/bin/env python3
import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker, declarative_base

load_dotenv()

logger = logging.getLogger(__name__)

# ==========================
# CONFIG DINÁMICA (Cloud + Local)
# ==========================
def _get_db_config():
    # 🔥 Intento 1: Streamlit Cloud (st.secrets)
    try:
        import streamlit as st

        host = st.secrets.get("DB_HOST", "")
        if host and host != "aws-1-us-east-1.pooler.supabase.com":
            return {
                "host":     host,
                "port":     st.secrets.get("DB_PORT", "6543"),
                "user":     st.secrets.get("DB_USER", "postgres.lulgtoondddxbalvnfgi"),
                "password": st.secrets.get("DB_PASSWORD", "luis3227684880"),
                "dbname":   st.secrets.get("DB_NAME", "postgres"),
            }
    except Exception:
        pass  # No está en Streamlit

    # 🔧 Intento 2: .env (local o fallback)
    return {
        "host":     os.getenv("DB_HOST", "aws-1-us-east-1.pooler.supabase.com"),
        "port":     os.getenv("DB_PORT", "6543"),
        "user":     os.getenv("DB_USER", "postgres.lulgtoondddxbalvnfgi"),
        "password": os.getenv("DB_PASSWORD", "luis3227684880"),
        "dbname":   os.getenv("DB_NAME", "postgres"),
    }


# ==========================
# CREAR URL
# ==========================
config = _get_db_config()

DATABASE_URL = (
    f"postgresql+psycopg2://{config['user']}:{config['password']}"
    f"@{config['host']}:{config['port']}/{config['dbname']}"
)

logger.info(f"🔌 Conectando a DB: {config['host']}:{config['port']}")

# ==========================
# ENGINE
# ==========================
engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True
)

# ==========================
# ORM
# ==========================
Base = declarative_base()

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

metadata = MetaData()

# ==========================
# UTILIDADES
# ==========================
def reflect_metadata():
    try:
        metadata.reflect(bind=engine)
        logger.info("✅ Metadata reflejada correctamente")
    except Exception as e:
        logger.warning(f"⚠️ No se pudo reflejar metadata: {str(e)}")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection():
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            logger.info("✅ Conexión a PostgreSQL exitosa")
            return True
    except Exception as e:
        logger.error(f"❌ Error conectando a PostgreSQL: {str(e)}")
        return False


def create_all_tables():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Tablas creadas correctamente")
    except Exception as e:
        logger.error(f"❌ Error creando tablas: {str(e)}")