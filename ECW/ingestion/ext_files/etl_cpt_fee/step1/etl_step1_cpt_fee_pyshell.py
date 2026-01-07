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

s3 = boto3.client("s3")

def main():
    start_time = time.time()
    print(f"✅ openpyxl version: {openpyxl.__version__}")

    # -------- Pre-validación para evitar exit(2) silencioso --------
    required = [
        "DB_SECRET_NAME",
        "AWS_REGION",
        "ENV"
    ]
    missing = [k for k in required if f"--{k}" not in sys.argv]
    if missing:
        raise RuntimeError(
            f"❌ Faltan parámetros del Job: {missing}\n🧰 sys.argv: {sys.argv}"
        )

    # -------- Args del Job --------
    args = getResolvedOptions(sys.argv, required)
    secret = f.get_secret(args["DB_SECRET_NAME"], args["AWS_REGION"])
    enviaroment =  args["ENV"]
    os.environ["ENV"] = args["ENV"] 
    print("Enviaroment: " + enviaroment)
    gp = p.GLOBLAL_PARAMS(enviaroment)

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

    try:
        print("🔌 Abriendo conexiones a PostgreSQL...")
        conn_raw = f.connect_to_postgres(secret, gp.DB_NAME)

        print("📥 Leyendo parámetros y catálogos desde BD...")
        df_fee = f.get_fees(conn_raw)

    finally:
        # Cerrar conexiones
        if conn_raw:
            conn_raw.close()
            print("🔒 Conexión RAW cerrada.")

    # --------------------- Transforms --------------------->
    print("⚙️ Iniciando transformaciones...")
    df_file = pd.read_excel(io.BytesIO(file), sheet_name=0)
    df_file["cpt_code"] = df_file["cpt_code"].astype(str)

    # === Paso 2: Transformar columnas a formato largo ===
    fixed_columns = [
        "cpt_description",
        "cpt_code",
        "cpt_group",
        "mod_1",
        "mod_2",
        "resource_provider_type"
    ]

    df_file = pd.melt(
        df_file,
        id_vars=fixed_columns,
        var_name="fee_plan",
        value_name="fee"
    )

    df_merged = df_file.merge(df_fee, how='left', on='fee_plan')
    df_merged = df_merged.reset_index(drop=True)
    df_merged.insert(0, "id_cpt_fee", df_merged.index + 1)  # <-- aquí se crea la columna
    df_merged["id_fee_plan"] = (
        pd.to_numeric(df_merged["id_fee_plan"], errors="coerce")
        .round(0)
        .astype("Int64")
    )
    df_merged = df_merged.where(pd.notnull(df_merged), None)

    
    columns_order = [
        "id_cpt_fee",
        "cpt_description",
        "cpt_code",
        "cpt_group",
        "mod_1",
        "mod_2",
        "resource_provider_type",
        "fee_plan",
        "fee",
        "id_fee_plan",
    ]

    df_merged = df_merged[columns_order]
    print("✅ Preview:")
    print(df_merged.head(3))
    print("✅ Rows:", len(df_merged))

    # --------- Carga de resultados a la tabla temporal en DEV (tmp) ---------
    print("⬆️ Cargando información a tabla temporal...")
    f.update_table(
        secret=secret,
        table_name="aux.cpt_fee",
        df=df_merged,
        db_name=gp.DB_NAME,
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
        f"✅ Step 1 aux.cpt_fee ingesta. "
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
                f"❌ Error en Step 1 CPT Fees:\n{str(e)}\n\n"
                f"Traceback:\n{traceback.format_exc()}"
            )
            f.enviar_correo_sns("Error en Step 1 CPT Fees", mensaje_error)
        except Exception:
            print("⚠️ No se pudo enviar notificación de error.")

        # Re-lanzar para que Glue marque el Job como FAILED
        raise
