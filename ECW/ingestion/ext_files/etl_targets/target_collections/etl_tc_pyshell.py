import sys, time, traceback, os, io
import boto3
import pandas as pd
import openpyxl
from awsglue.utils import getResolvedOptions

import functions as f
import params as p

s3 = boto3.client("s3")


def main():
    start_time = time.time()
    print(f"✅ openpyxl version: {openpyxl.__version__}")

    # -------- Pre-validación para evitar exit(2) silencioso --------
    required = ["DB_SECRET_NAME", "AWS_REGION", "ENV"]
    missing = [k for k in required if f"--{k}" not in sys.argv]
    if missing:
        raise RuntimeError(f"❌ Faltan parámetros del Job: {missing}\n🧰 sys.argv: {sys.argv}")

    # -------- Args del Job --------
    args = getResolvedOptions(sys.argv, required)
    secret = f.get_secret(args["DB_SECRET_NAME"], args["AWS_REGION"])

    environment = args["ENV"]
    os.environ["ENV"] = environment
    print(f"Environment: {environment}")

    gp = p.GLOBLAL_PARAMS(environment)

    try:
        # --- S3 params ---
        s3_bucket = gp.s3_bucket.strip("/")          # solo nombre bucket
        s3_input_prefix = gp.s3_input_prefix.lstrip("/")
        s3_archive_prefix = gp.s3_archive_prefix.lstrip("/")
        s3_extension = ".xlsx"

        # -------- Buscar EXACTAMENTE 1 archivo .xlsx en input_files (con paginación) --------
        print("🔎 Buscando archivo .xlsx en S3...")
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_input_prefix)

        s3_files = []
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.lower().endswith(s3_extension) and not key.endswith("/"):
                    s3_files.append(key)

        if not s3_files:
            raise FileNotFoundError(f"❌ No se encontraron archivos {s3_extension} en s3://{s3_bucket}/{s3_input_prefix}")
        if len(s3_files) > 1:
            raise RuntimeError(f"❌ Hay más de un archivo {s3_extension} en input_files: {s3_files}")

        s3_key = s3_files[0]
        print(f"📥 Archivo encontrado: s3://{s3_bucket}/{s3_key}")

        # -------- Leer archivo desde S3 --------
        obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        file_bytes = obj["Body"].read()
        print("✅ Archivo leído correctamente desde S3")

        # -------- Validar hojas disponibles --------
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
        sheets_in_file = xls.sheet_names
        sheets_df = pd.DataFrame({"sheet_file": sheets_in_file})
        sheets_df["read"] = 1
        print("📄 Sheets detectadas:", sheets_in_file)

        # -------- Leer la hoja correcta a DataFrame --------
        # Ajusta esto a tu lógica real:
        # - Si siempre es "Sheet1", déjalo fijo
        # - Si quieres detectar una hoja específica, cambia la regla
        sheet_to_read = "Sheet1" if "Sheet1" in sheets_in_file else sheets_in_file[0]
        print(f"📌 Leyendo hoja: {sheet_to_read}")

        dfs = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_to_read, dtype=str)

        # -------- Limpieza + cast + orden (tu función) --------
        # Asegúrate que esta función:
        # - elimine columnas Unnamed
        # - elimine filas vacías
        # - castee tipos y ordene columnas
        dfs = f.cast_and_order_target_collections(dfs)
        print(dfs.head())
        print(f"✅ Rows after clean: {len(dfs)}")

        # -------- Cargar a DB --------
        table = "aux.target_collections"
        f.only_load_to_database(
            secret=secret,
            table_name=table,
            df=dfs,
            db_name=gp.DB_NAME,
        )
        print("✅ aux.target_collections loaded")

        # -------- Mover archivo procesado a history_files con fecha --------
        run_date = time.strftime("%Y-%m-%d")
        base, ext = os.path.splitext(os.path.basename(s3_key))
        archived_filename = f"{base}_{run_date}{ext}"
        archive_key = f"{s3_archive_prefix.rstrip('/')}/{archived_filename}"

        s3.copy_object(
            Bucket=s3_bucket,
            CopySource={"Bucket": s3_bucket, "Key": s3_key},
            Key=archive_key,
        )
        s3.delete_object(Bucket=s3_bucket, Key=s3_key)
        print(f"📦 Archivo archivado en S3: s3://{s3_bucket}/{archive_key}")

        # -------- Tiempo de ejecución + OK SNS --------
        elapsed = time.time() - start_time
        minutes, seconds = divmod(int(elapsed), 60)

        mensaje_time = (
            "✅ Target collections finalizado. "
            f"Tiempo de ejecución: {minutes} minutos y {seconds} segundos"
        )
        f.enviar_correo_sns("OK Target collections", mensaje_time)

    except Exception as e:
        print("❌ Falló Target collections")
        print(str(e))
        traceback.print_exc()

        elapsed = time.time() - start_time
        minutes, seconds = divmod(int(elapsed), 60)

        msg = (
            "❌ Target collections falló.\n"
            f"Error: {e}\n"
            f"Tiempo transcurrido: {minutes}m {seconds}s\n"
        )
        try:
            f.enviar_correo_sns("FAIL Target collections", msg)
        except Exception as sns_e:
            print("⚠️ No se pudo enviar correo SNS:", str(sns_e))

        raise


if __name__ == "__main__":
    main()
