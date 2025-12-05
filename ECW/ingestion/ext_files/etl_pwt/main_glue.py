# main_glue.py (Glue 4.0)
import sys, time, json, traceback
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

import boto3

from params_glue import (
    JDBC_OPTIONS_BASE, TABLE_RAW, TABLE_PWT_PAT, TABLE_PWT_SUMMARY,
    CALENDAR_DETAIL_SQL, FACILITY_INFO_SQL, XLSX_COLS, RENAME_MAP
)
from functions_glue import (
    read_xlsx_with_spark,
    move_to_history_and_delete, write_jdbc_overwrite, read_jdbc_sql
)
from transformations_pwt import normalize_and_cast, make_raw_output, build_pwt_pat, build_pwt_summary

# ---------- Args del Job ----------
ARGS = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        # S3
        "s3_bucket", "s3_input_prefix", "s3_history_prefix",
        # JDBC RAW
        "jdbc_host_raw", "jdbc_port", "jdbc_db_raw", "jdbc_user", "jdbc_password",
        # JDBC DM
        "jdbc_host_dm", "jdbc_db_dm",
        # SNS
        "sns_topic_arn"
    ]
)

def jdbc_url(host, port, db):
    return f"jdbc:postgresql://{host}:{port}/{db}"

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(ARGS["JOB_NAME"], ARGS)

    s3 = boto3.client("s3")
    bucket = ARGS["s3_bucket"]
    in_prefix = ARGS["s3_input_prefix"]
    hist_prefix = ARGS["s3_history_prefix"]

    url_raw = jdbc_url(ARGS["jdbc_host_raw"], ARGS["jdbc_port"], ARGS["jdbc_db_raw"])
    url_dm  = jdbc_url(ARGS["jdbc_host_dm"],  ARGS["jdbc_port"], ARGS["jdbc_db_dm"])
    user = ARGS["jdbc_user"]
    pwd  = ARGS["jdbc_password"]

    start = time.time()
    rows_in = 0
    try:
        # 1) Detectar XLSX
        raw_in = read_xlsx_with_spark(spark, bucket, in_prefix)

        # 3) Normalizar y castear
        pwt = normalize_and_cast(raw_in, RENAME_MAP)
        rows_in = pwt.count()

        # 4) Escribir a RAW (Postgres)
        raw_out = make_raw_output(pwt)
        write_jdbc_overwrite(raw_out, url_raw, TABLE_RAW, user, pwd)

        # 5) Tablas auxiliares (JDBC lecturas)
        calendar_df = read_jdbc_sql(spark, url_raw, user, pwd, CALENDAR_DETAIL_SQL)
        facility_df = read_jdbc_sql(spark, url_raw, user, pwd, FACILITY_INFO_SQL)

        # 6) PWT_PAT
        pwt_pat = build_pwt_pat(pwt, facility_df, calendar_df)
        write_jdbc_overwrite(pwt_pat, url_dm, TABLE_PWT_PAT, user, pwd)

        # 7) PWT_SUMMARY
        pwt_summary = build_pwt_summary(pwt, facility_df, calendar_df)
        write_jdbc_overwrite(pwt_summary, url_dm, TABLE_PWT_SUMMARY, user, pwd)

        # 8) Mover a history + borrar
        for k in keys:
            move_to_history_and_delete(s3, bucket, k, hist_prefix)

        # 9) SNS éxito
        elapsed = time.time() - start
        minutes = int(elapsed // 60); seconds = int(elapsed % 60)
        msg = f"✅ ETL PWT completado en {minutes}m {seconds}s. Filas entrada: {rows_in}"
        boto3.client("sns").publish(TopicArn=ARGS["sns_topic_arn"], Subject="OK Update Patient Wait Time", Message=msg)

    except Exception as e:
        exc = traceback.format_exc()
        boto3.client("sns").publish(TopicArn=ARGS["sns_topic_arn"], Subject="NOK Update Patient Wait Time", Message=f"❌ Error en ETL PWT:\n{exc}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
