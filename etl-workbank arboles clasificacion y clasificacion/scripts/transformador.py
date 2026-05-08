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

    # =========================
    # LIMPIEZA INICIAL
    # =========================
    df = df.dropna(subset=["pais", "anio", "indicador"])
    df["anio"] = df["anio"].astype(int)

    # Ordenar para interpolación correcta
    df = df.sort_values(["pais", "anio"])

    # =========================
    # PIVOT
    # =========================
    df_pivot = df.pivot_table(
        index=["pais", "anio"],
        columns="indicador",
        values="valor",
        aggfunc="first"
    ).reset_index()

    logging.info("📊 Pivot realizado correctamente")

    # =========================
    # RENOMBRAR COLUMNAS
    # =========================
    rename_map = {}

    for col in df_pivot.columns:
        if "GDP" in col:
            rename_map[col] = "pib"
        elif "Inflation" in col:
            rename_map[col] = "inflacion"
        elif "Population" in col:
            rename_map[col] = "poblacion"

    df_model = df_pivot.rename(columns=rename_map).copy()

    # =========================
    # IMPUTACIÓN (CLAVE)
    # =========================
    for col in ["pib", "inflacion", "poblacion"]:
        if col in df_model.columns:
            df_model[col] = df_model.groupby("pais")[col].transform(
                lambda x: x.interpolate()
            )

    logging.info("🧠 Imputación por interpolación aplicada")

    # =========================
    # RELLENO FINAL (bordes)
    # =========================
    for col in ["pib", "inflacion", "poblacion"]:
        if col in df_model.columns:
            df_model[col] = df_model[col].fillna(method="bfill").fillna(method="ffill")

    # =========================
    # EVITAR CEROS / NEGATIVOS
    # =========================
    for col in ["pib", "inflacion", "poblacion"]:
        if col in df_model.columns:
            df_model[col] = df_model[col].clip(lower=0.01)

    logging.info("🧹 Limpieza final aplicada")

    # =========================
    # VALIDACIÓN FINAL
    # =========================
    nulls = df_model.isnull().sum().sum()

    if nulls > 0:
        logging.warning(f"⚠️ Aún hay {nulls} valores nulos")
    else:
        logging.info("✅ Dataset sin valores nulos")

    return df_model