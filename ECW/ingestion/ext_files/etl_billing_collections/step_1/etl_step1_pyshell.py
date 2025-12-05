import sys, time, json, traceback, os
import io
import openpyxl
import boto3, botocore
import pandas as pd
import numpy as np
import pg8000
from awsglue.utils import getResolvedOptions
import functions as f
import params as p

gp = p.GLOBLAL_PARAMS()
s3 = boto3.client("s3")

def main():
    start_time = time.time()
    print(f"✅ openpyxl version: {openpyxl.__version__}")

    # -------- Pre-validación para evitar exit(2) silencioso --------
    required = [
        "DB_SECRET_NAME",
        "AWS_REGION"
    ]
    missing = [k for k in required if f"--{k}" not in sys.argv]
    if missing:
        raise RuntimeError(
            f"❌ Faltan parámetros del Job: {missing}\n🧰 sys.argv: {sys.argv}"
        )

    # -------- Args del Job --------
    args = getResolvedOptions(sys.argv, required)
    secret = f.get_secret(args["DB_SECRET_NAME"], args["AWS_REGION"])

    s3_bucket = gp.s3_bucket.rstrip("/")
    s3_input_prefix = gp.s3_input_prefix.lstrip("/")
    s3_archive_prefix = gp.s3_archive_prefix.lstrip("/")
    s3_extension = ".xlsx"

    # -------- Buscar archivos .xlsx en el input (con paginación) --------
    print("🔎 Buscando archivos en S3...")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_input_prefix)
    s3_files = []

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(s3_extension) and not key.endswith("/"):
                s3_files.append(key)

    if not s3_files:
        raise FileNotFoundError(
            "❌ No se encontraron archivos .xlsx en S3 con el prefijo especificado."
        )
    elif len(s3_files) > 1:
        raise RuntimeError(
            f"❌ Hay más de un archivo .xlsx en la carpeta de entrada de S3: {s3_files}"
        )
    else:
        s3_key = s3_files[0]
        print(f"📥 Archivo encontrado en S3: s3://{s3_bucket}/{s3_key}")

    # -------- Leer archivo desde S3 --------
    obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    file = obj["Body"].read()
    print("✅ Archivo leído correctamente desde S3")

    # -------- Conexiones a BD --------
    conn_raw = None
    conn_raw_dev = None
    conn_dm = None

    try:
        print("🔌 Abriendo conexiones a PostgreSQL...")
        conn_raw = f.connect_to_postgres(secret, gp.DB_NAME)
        conn_raw_dev = f.connect_to_postgres(secret, gp.DB_NAME_DEV)
        conn_dm = f.connect_to_postgres(secret, gp.DB_NAME_DM_PROD)

        print("📥 Leyendo parámetros y catálogos desde BD...")
        reading_parameters = f.get_reading_parameters(conn_raw)
        merchant_info = f.get_merchant_info(conn_raw)
        facilities = f.get_facilities(conn_dm)

    finally:
        # Cerrar conexiones
        if conn_raw:
            conn_raw.close()
            print("🔒 Conexión RAW cerrada.")
        if conn_raw_dev:
            conn_raw_dev.close()
            print("🔒 Conexión RAW_DEV cerrada.")
        if conn_dm:
            conn_dm.close()
            print("🔒 Conexión DM cerrada.")

    # --------------------- Transforms --------------------->
    print("⚙️ Iniciando transformaciones...")

    # Validar hojas disponibles
    sheets_in_bank_file = pd.ExcelFile(io.BytesIO(file)).sheet_names
    sheets_bank_df = pd.DataFrame({"sheet_file": sheets_in_bank_file})
    sheets_bank_df["read"] = 1

    reading_parameters = pd.merge(
        reading_parameters,
        sheets_bank_df,
        left_on=["sheetname"],
        right_on=["sheet_file"],
        how="left",
    )
    reading_parameters["read"] = np.where(
        reading_parameters["read"] == 1, 1, 0
    )

    sheets_for_read_cnt = reading_parameters["read"].sum()
    sheets_registered_cnt = reading_parameters.shape[0]

    if sheets_for_read_cnt != sheets_registered_cnt:
        print("⚠️ Warning: Sheets Missing")

    reading_parameters = reading_parameters[reading_parameters["read"] == 1]

    # Leer cada hoja y transformar
    dfs_list = []
    for index in reading_parameters.index:
        sheetname = reading_parameters.loc[index, "sheetname"]
        facility_group_id = reading_parameters.loc[index, "facility_group_id"]
        check_account = reading_parameters.loc[index, "check_account"]
        lecture_parameters = reading_parameters.loc[index, "lecture_parameters"]
        read_columns = lecture_parameters["read_columns"]
        rename_columns = lecture_parameters["rename_columns"]

        df = pd.read_excel(
            io.BytesIO(file),
            sheet_name=sheetname,
            usecols=read_columns
        )
        df["sheetname"] = sheetname
        df["sheet_facility_group_id"] = facility_group_id
        df["check_account"] = check_account
        df.rename(columns=rename_columns, inplace=True)
        dfs_list.append(df)

    dfs = pd.concat(dfs_list).reset_index(drop=True)

    # Conversión de columnas
    dfs["amount"] = pd.to_numeric(dfs["amount"], errors="coerce")
    dfs["Debit"] = pd.to_numeric(dfs["Debit"], errors="coerce")
    dfs["Credit"] = pd.to_numeric(dfs["Credit"], errors="coerce")

    sheet_facility_group_ids = [4, 5, 11, 12, 13, 14, 15]
    mask_all = dfs["amount"].isna() & dfs["sheet_facility_group_id"].isin(
        sheet_facility_group_ids
    )
    dfs.loc[mask_all, "amount"] = (
        dfs.loc[mask_all, "Debit"].fillna(0)
        + dfs.loc[mask_all, "Credit"].fillna(0)
    )
    dfs.drop(["Debit", "Credit"], axis=1, inplace=True)

    # Merchant ID
    pattern = r"(IND ID:\d+)"
    dfs["merchant_id"] = (
        dfs["description"]
        .str.extract(pat=pattern, expand=False)
        .str.split(":", expand=True)[1]
    )
    dfs["merchant_id"] = dfs["merchant_id"].fillna("No ID")
    dfs = pd.merge(dfs, merchant_info, on=["merchant_id"], how="left")

    dfs["merch_facility_group_id"] = np.where(
        dfs["check_account"] == 1,
        dfs["merch_facility_group_id"],
        np.nan
    )
    dfs["merch_facility_group_id"] = np.where(
        (dfs["check_account"] == 1) & (dfs["merch_facility_group_id"].isnull()),
        0,
        dfs["merch_facility_group_id"],
    )

    dfs["facility_group_id"] = np.where(
        dfs["merch_facility_group_id"].isnull(),
        dfs["sheet_facility_group_id"],
        dfs["merch_facility_group_id"],
    )

    dfs["payment_id"] = dfs["payment_id"].fillna(0)
    dfs["sheet_date"] = dfs["sheet_date"].dt.date
    dfs["merch_facility_group_id"] = dfs["merch_facility_group_id"].astype("Int64")
    dfs["facility_group_id"] = dfs["facility_group_id"].astype("Int64")
    dfs["payment_id"] = dfs["payment_id"].astype("Int64")

    # Reemplazo de nombres de hoja
    dfs["sheetname"] = dfs["sheetname"].replace({
        "WEISS": "008 MFM-Merit-JW",
        "Safely": "009 Harwood",
        "FAIRWAY": "010 CFW",
        "BUCKNER": "011 Buckner",
    })
    merchant_info.rename(
        columns={"merch_facility_group_id": "facility_group_id"}, inplace=True
    )
    dfs = pd.merge(dfs, merchant_info, on="facility_group_id", how="left")

    dfs["facility_y"] = dfs["facility_y"].fillna(dfs["sheetname"])

    # Columnas finales
    final_cols = [
        "sheet_date",
        "description",
        "amount",
        "payment_id",
        "Manual_Distrib",
        "sheetname",
        "sheet_facility_group_id",
        "check_account",
        "merchant_id_y",
        "facility_y",
        "merch_facility_group_id",
        "facility_group_id",
    ]

    dfs = dfs[final_cols]
    dfs.rename(
        columns={"merchant_id_y": "merchant_id", "facility_y": "facility"},
        inplace=True,
    )

    # Reemplazar 'facility' con 'Manual_Distrib' solo donde payment_id == 4
    if "Manual_Distrib" in dfs.columns:
        dfs.loc[
            (dfs["payment_id"] == 4) & (dfs["Manual_Distrib"].notna()),
            "facility",
        ] = dfs["Manual_Distrib"]
    else:
        print("⚠️ La columna 'Manual_Distrib' no está presente en el DataFrame.")

    dfs = dfs.drop(columns=["facility_group_id"])  # quitar vieja
    dfs = dfs.merge(
        facilities.rename(columns={"facility_group_name": "facility"}),
        on="facility",
        how="left",
    )
    dfs["facility_group_id"] = dfs["facility_group_id"].fillna(0).astype("int64")
    dfs = dfs.drop(columns=["Manual_Distrib"])

    # Elimina columnas antiguas para evitar conflictos en el merge
    dfs = dfs.drop(columns=["merchant_id"])

    # Realiza el merge sobre la columna 'facility'
    dfs = dfs.merge(
        merchant_info[["facility", "merchant_id", "facility_group_id"]],
        on="facility",
        how="left",
    )

    dfs = dfs.drop(columns=["merch_facility_group_id"])
    dfs = dfs.rename(
        columns={
            "facility_group_id_x": "facility_group_id",
            "facility_group_id_y": "merch_facility_group_id",
        }
    )

    # Cast
    dfs["merch_facility_group_id"] = dfs["merch_facility_group_id"].astype("Int64")
    dfs["facility_group_id"] = dfs["facility_group_id"].astype("Int64")
    dfs["sheet_date"] = pd.to_datetime(dfs["sheet_date"])

    # Orden final
    dfs = dfs[
        [
            "sheet_date",
            "description",
            "amount",
            "payment_id",
            "sheetname",
            "sheet_facility_group_id",
            "check_account",
            "merchant_id",
            "facility",
            "merch_facility_group_id",
            "facility_group_id",
        ]
    ]

    # --------- Carga de resultados a la tabla temporal en DEV (tmp) ---------
    print("⬆️ Cargando información a tabla temporal...")
    f.update_table(
        secret=secret,
        table_name="tmp_bank.billing_collections_tmp",
        df=dfs,
        db_name=gp.DB_NAME,
    )

    # --------- Upgrade a tabla final ---------
    print("🔁 Actualizando tabla final...")
    f.upgrade_table(
        host=secret["host"],
        database=gp.DB_NAME,
        user=secret["username"],
        pwd=secret["password"],
        port=int(secret.get("port", 5432)),
        table_source="tmp_bank.billing_collections_tmp",
        table_target="raw_bank.billing_collections",
        by_field="sheet_date",
    )

    # === Mover archivo procesado a carpeta de histórico ===
    archive_key = s3_key.replace(s3_input_prefix, s3_archive_prefix)
    s3.copy_object(
        Bucket=s3_bucket,
        CopySource={"Bucket": s3_bucket, "Key": s3_key},
        Key=archive_key,
    )
    s3.delete_object(Bucket=s3_bucket, Key=s3_key)
    print(f"📦 Archivo archivado en S3: s3://{s3_bucket}/{archive_key}")

    # Tiempo de ejecución
    elapsed = time.time() - start_time
    minutes, seconds = divmod(int(elapsed), 60)

    mensaje_time = (
        f"✅ Test Billing Collections Step 1 Finalizado. "
        f"Tiempo de ejecución: {minutes} minutos y {seconds} segundos"
    )
    asunto_2 = "OK "
    f.enviar_correo_sns(asunto_2, mensaje_time)  # Asumiendo que la tienes importada


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Manejo global de errores
        print("❌ Error no controlado en el Job:")
        print(str(e))
        traceback.print_exc()

        # Si quieres enviar correo también en error:
        try:
            mensaje_error = (
                f"❌ Error en Billing Collections Step 1:\n{str(e)}\n\n"
                f"Traceback:\n{traceback.format_exc()}"
            )
            f.enviar_correo_sns("ERROR Billing Collections", mensaje_error)
        except Exception:
            print("⚠️ No se pudo enviar notificación de error.")

        # Re-lanzar para que Glue marque el Job como FAILED
        raise
