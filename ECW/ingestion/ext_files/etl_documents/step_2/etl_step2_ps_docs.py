import sys
import time
import json
import traceback
import os
from datetime import datetime

import boto3
import botocore

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
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
        raise RuntimeError(f"❌ Faltan parámetros del Job: {missing}\n🧰 sys.argv: {sys.argv}")

    # -------- Args --------
    args = getResolvedOptions(sys.argv, required)
    secret = f.get_secret(args["DB_SECRET_NAME"], args["AWS_REGION"])  # por si lo usas después

    environment = args["ENV"]
    os.environ["ENV"] = environment

    print("Environment:", environment)
    gp = p.GLOBLAL_PARAMS(environment)

    # -------- Inicializar Spark / Glue --------
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # -------- Logger JSON --------
    log = glueContext.get_logger()
    log_level = args.get("log_level", "INFO").upper()
    spark.sparkContext.setLogLevel(log_level)

    def jlog(level: str, event: str, **kwargs):
        payload = {"level": level.upper(), "event": event, **kwargs}
        msg = json.dumps(payload, default=str)
        if level.lower() == "error":
            log.error(msg)
        else:
            log.info(msg)

    jlog("info", "job_start", job_name=args["JOB_NAME"], log_level=log_level)

    try:
        process_date = datetime.now().strftime("%Y-%m-%d")

        # -------- Setup S3 --------
        s3_bucket = gp.s3_bucket.rstrip("/")
        s3_input_prefix = gp.s3_input_prefix_raw.strip("/")
        s3_archive_prefix = gp.s3_archive_prefix_raw.strip("/")

        partition_prefix = f"{s3_input_prefix}/process_date={process_date}/"
        partition_path = f"s3://{s3_bucket}/{partition_prefix}"

        print(f"📅 Ejecutando para process_date={process_date}")
        print(f"📂 Verificando insumo en: {partition_path}")

        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=partition_prefix, MaxKeys=1)
        if "Contents" not in response:
            raise FileNotFoundError(
                f"❌ No existe insumo para process_date={process_date} "
                f"en s3://{s3_bucket}/{partition_prefix}"
            )

        print("✅ Insumo encontrado, iniciando lectura...")

        # ----------------------------------------------------
        # 📥 Leer parquet de la partición
        # ----------------------------------------------------
        df = spark.read.parquet(partition_path)

        df.printSchema()
        print(f"✅ Registros leídos (raw): {df.count()}")

        df = (
            df.withColumn("scan_dt", F.to_date(F.col("scan_dt")))
              .withColumn("reviewed_dt", F.to_date(F.col("reviewed_dt")))
        )

        df = (
            df.withColumn("review_flag", F.when(F.col("reviewed_dt").isNull(), F.lit(0)).otherwise(F.lit(1)))
              .withColumn("incref", F.when(F.col("assigned_to") == F.lit("Incoming, Referrals"), F.lit(1)).otherwise(F.lit(0)))
        )

        # ---- reviewed_days = business days between scan_dt and reviewed_dt ----
        # dayofweek(): 1=Dom, 7=Sáb => hábil = 2..6
        business_days_count_expr = F.expr("""
            size(
              filter(
                sequence(scan_dt, reviewed_dt),
                d -> dayofweek(d) BETWEEN 2 AND 6
              )
            )
        """)

        df = df.withColumn(
            "reviewed_days",
            F.when(F.col("review_flag") == 1, business_days_count_expr).otherwise(F.lit(0))
        )

        df = df.withColumn(
            "sla",
            F.when(
                (F.col("review_flag") == 1) & (F.col("incref") == 1) & (F.col("reviewed_days") <= 1),
                F.lit(1)
            ).when(
                (F.col("review_flag") == 1) & (F.col("incref") == 0) & (F.col("reviewed_days") <= 3),
                F.lit(1)
            ).otherwise(F.lit(0))
        )

        df = (
            df.withColumn("patient_id", F.regexp_extract(F.col("patient_id_name"), r"^(\d+)", 1).cast("long"))
              .drop("patient_id_name")
        )

        print("✅ Transform DF")
        df.printSchema()
        print(f"✅ Registros (final): {df.count()}")

        # === Merge con calendar_week ===
        print("🗓️ Cargando calendario de semanas...")
        

        # aquí iría tu write / load / etc.
        # df.write.mode("overwrite").parquet(...)

        jlog("info", "job_success", process_date=process_date, elapsed_sec=round(time.time() - start_time, 2))
        job.commit()

    except Exception as e:
        jlog(
            "error",
            "job_failed",
            error=str(e),
            traceback=traceback.format_exc(),
            elapsed_sec=round(time.time() - start_time, 2),
        )
        raise

    finally:
        # En Glue normalmente no es necesario parar Spark explícitamente,
        # pero no estorba dejar evidencia de cierre.
        print("🏁 Fin del job")


if __name__ == "__main__":
    main()
