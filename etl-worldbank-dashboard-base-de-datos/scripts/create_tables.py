#!/usr/bin/env python3
from scripts.database import engine, Base
import scripts.models  # asegura que registre los modelos

def main():
    Base.metadata.create_all(bind=engine)
    print("✅ Tablas creadas/actualizadas (si no existían)")

if __name__ == "__main__":
    main()