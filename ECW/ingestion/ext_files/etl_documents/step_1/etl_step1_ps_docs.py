import sys, time, json, traceback, os
import io
import boto3, botocore

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as Fnx

from awsglue.job import Job

import functions as f
import params as p

s3 = boto3.client("s3")

def main():
    start_time = time.time()

    # -------- Pre-validación --------
    required = ["DB_SECRET_NAME", "AWS_REGION", "ENV", "JOB_NAME"]
    missing = [k for k in required if f"--{k}" not in sys.argv]
    if missing:
        raise RuntimeError(
            f"❌ Faltan parámetros del Job: {missing}\n🧰 sys.argv: {sys.argv}"
        )
    
    # -------- Args --------
    args = getResolvedOptions(sys.argv, required)
    secret = f.get_secret(args["DB_SECRET_NAME"], args["AWS_REGION"])
    enviaroment = args["ENV"]
    os.environ["ENV"] = enviaroment
    
    print("Environment:", enviaroment)
    gp = p.GLOBLAL_PARAMS(enviaroment)

    # -------- Inicializar Spark / Glue --------
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # -------- Logger JSON --------
    log = glueContext.get_logger()
    log_level = args.get("log_level", "INFO").upper()
    spark.sparkContext.setLogLevel(log_level)

    def jlog(level: str, event: str, **kwargs):
        payload = {"event": event, **kwargs}
        msg = json.dumps(payload, default=str)
        (log.error if level == "error" else log.info)(msg)

    def timer():
        t0 = time.time()
        return lambda: round(time.time() - t0, 2)

    jlog("info", "job_start", job_name=args["JOB_NAME"], log_level=log_level)

    try:
        # -------- Setup S3 --------
        s3_bucket = gp.s3_bucket.rstrip("/")
        s3_input_prefix = gp.s3_input_prefix_staging.lstrip("/")
        s3_archive_prefix = gp.s3_archive_prefix__staging.lstrip("/")
        s3_extension = ".csv"
    
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
            raise FileNotFoundError("❌ No se encontraron archivos .CSV en S3.")
        elif len(s3_files) > 1:
            raise RuntimeError(f"❌ Hay más de un archivo CSV: {s3_files}")

        s3_key = s3_files[0]
        print(f"📥 Archivo encontrado: s3://{s3_bucket}/{s3_key}")
        s3_file_input = f"s3://{s3_bucket}/{s3_key}"

        # -------- Leer TSV UTF-16 --------
        t = timer()
        jlog("info", "read_start", path=s3_file_input, options=gp.READ_OPTIONS)

        df = f.read_tsv_utf16(spark, s3_file_input,gp.READ_OPTIONS)
        df = df.withColumn("process_date", Fnx.current_date())

        # Casteo
        
        df_cast = (
            df
            .withColumnRenamed("Assigned To", "assigned_to")
            .withColumnRenamed("Scanned By", "scanned_by")
            .withColumnRenamed("Scan Date", "scan_dt")
            .withColumnRenamed("Document ID", "document_id")
            .withColumnRenamed("Patient Acct No + Name", "patient_id_name")
            .withColumnRenamed("Document Type", "document_type")
            .withColumnRenamed("Custom Name", "custom_name")
            .withColumnRenamed("Document Facility Name", "document_facility_name")
            .withColumnRenamed("Appointment Practice Name", "appointment_facility_name")
            .withColumnRenamed("Reviewed", "reviewed")
            .withColumnRenamed("Reviewer Name", "reviewer_name")
            .withColumnRenamed("Reviewed Date", "reviewed_dt")
            .withColumnRenamed("High Priority", "high_priority")
            .withColumnRenamed("Publish To EHX", "publish_to_ehx")
            .withColumnRenamed("Migrated", "migrated")
        )

        print("📐 Schema del DF:")
        df_cast.printSchema()

        row_count = df_cast.count()
        print(f"📊 Total de registros: {row_count}")

        num_parts = f.calc_num_partitions(df_cast, target_size_mb=256)
        print(f"🔢 Particiones recomendadas: {num_parts}")  

        s3_bucket = gp.s3_bucket.rstrip("/")
        s3_output_prefix = gp.s3_input_prefix_raw.strip("/")

        output_path = f"s3://{s3_bucket}/{s3_output_prefix}"

        print(f"💾 Guardando DataFrame en Parquet en: {output_path}")

        (
            df_cast
            .repartition(num_parts, "process_date")
            .write
            .mode("overwrite")
            .partitionBy("process_date") 
            .format("parquet")
            .save(output_path)
        )

        # -------- Mover a history --------
        t = timer()
        try:
            t = timer()
            
            # URI completa del archivo original
            s3_input = f"s3://{s3_bucket}/{s3_key}"

            archivado_uri = f.move_to_history_with_date(
                s3_input=s3_input,
                history_prefix=s3_archive_prefix,
                base_name="Documents",
                tz_name="America/Mexico_City",
                keep_extension=True
            )

            # eliminar archivo original
            f.delete_input_file(s3_input)

            jlog("info", "archive_done", archived_uri=archivado_uri, elapsed_s=t())

        except Exception as arch_e:
            jlog("error", "archive_failed", error=str(arch_e), stack=traceback.format_exc())



        # Aquí continúa tu ETL...

    except Exception as e:
        jlog("error", "job_failed", error=str(e), traceback=traceback.format_exc())
        raise  # Glue debe marcar FAIL

    finally:
        # ---- Commit del job ----
        try:
            job.commit()
            jlog("info", "job_commit_success")
        except Exception as e:
            jlog("error", "job_commit_failed", error=str(e))

        # ---- Cerrar Spark ----
        try:
            sc.stop()
            jlog("info", "spark_context_stopped")
        except Exception as e:
            jlog("error", "spark_context_stop_failed", error=str(e))

        # ---- Log de fin de ejecución ----
        jlog("info", "job_end", duration=round(time.time() - start_time, 2))


if __name__ == "__main__":
    main()
