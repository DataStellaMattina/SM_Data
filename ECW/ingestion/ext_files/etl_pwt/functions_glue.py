# functions_glue.py
import io, os, boto3, datetime, traceback
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


def read_xlsx_with_spark(spark, bucket: str, input_prefix: str):
    """
    Lee todos los .xlsx de un prefijo con spark-excel.
    Requiere el JAR: com.crealytics:spark-excel_2.12:3.3.2 (Glue 4.0) o 3.5.2 (Glue 5.0).
    """
    path = f"s3://{bucket}/{input_prefix}/*.xlsx"
    df = (
        spark.read
             .format("com.crealytics.spark.excel")
             .option("header", "true")
             .option("inferSchema", "true")
             # .option("dataAddress", "'Sheet1'!A1")  # si necesitas hoja/rango específico
             .load(path)
             .withColumn("source_file", F.input_file_name())
    )
    return df


def list_xlsx_keys(s3_client, bucket: str, prefix: str):
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = []
    if "Contents" in resp:
        for o in resp["Contents"]:
            k = o["Key"]
            if k.endswith(".xlsx") and not k.endswith("/"):
                keys.append(k)
    return keys

def read_multiple_xlsx_to_spark_df(spark, s3_client, bucket: str, keys: list, xlsx_cols: list) -> DataFrame:
    """
    Lee varios .xlsx desde S3 con pandas y combina a un Spark DF.
    Evita dependencias JAR de spark-excel.
    """
    pdfs = []
    for k in keys:
        obj = s3_client.get_object(Bucket=bucket, Key=k)
        data = obj["Body"].read()
        pdf = pd.read_excel(io.BytesIO(data), usecols=xlsx_cols)
        pdf["source_file"] = os.path.basename(k)
        pdfs.append(pdf)
    if not pdfs:
        raise Exception("No hay .xlsx para leer")
    pdf_all = pd.concat(pdfs, ignore_index=True)
    # tips de tipos básicos; Spark hará el cast después
    return spark.createDataFrame(pdf_all)

def move_to_history_and_delete(s3_client, bucket: str, key: str, history_prefix: str):
    file_name = os.path.basename(key)
    hist_key = f"{history_prefix}/{file_name}"
    s3_client.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=hist_key)
    s3_client.delete_object(Bucket=bucket, Key=key)
    return hist_key

def write_jdbc_overwrite(df: DataFrame, url: str, table: str, user: str, password: str, batchsize: int = 10000):
    (
        df.write
          .format("jdbc")
          .option("url", url)
          .option("dbtable", table)
          .option("user", user)
          .option("password", password)
          .option("driver", "org.postgresql.Driver")
          .option("batchsize", str(batchsize))
          .option("truncate", "true")
          .mode("overwrite")
          .save()
    )

def read_jdbc_sql(spark, url: str, user: str, pwd: str, query: str) -> DataFrame:
    return (
        spark.read
             .format("jdbc")
             .option("url", url)
             .option("user", user)
             .option("password", pwd)
             .option("driver", "org.postgresql.Driver")
             .option("query", query)
             .load()
    )

def to_date_safe(col):
    return F.to_date(F.col(col))  # input pandas->spark ya vendrá en timestamp/str

def to_timestamp_safe(col):
    return F.to_timestamp(F.col(col))

def compute_minutes_in_status(col_time_in_status: str):
    # "49 minutes" → 49
    return F.regexp_extract(F.col(col_time_in_status), r"(\\d+)", 1).cast("int")

def build_visit_ids(df: DataFrame) -> DataFrame:
    # visit_type_id simula tu np.arange ordenado, usando dense_rank-1
    w = Window.orderBy("visit_type")
    df = df.withColumn("visit_type_id", (F.dense_rank().over(w) - F.lit(1)))
    # primeros 3 chars de facility_group_name (se obtendrá al hacer el join con facility_info)
    # aquí creamos columnas base; la parte de group_name ocurre después del join
    return df

def attach_visit_keys(df: DataFrame) -> DataFrame:
    # requiere facility_group_name y visit_type_id ya presentes
    # appointment_date a string ISO sin guiones para preservar compatibilidad con pandas
    return (
        df.withColumn("appointment_date_str", F.date_format("appointment_date", "yyyy-MM-dd"))
          .withColumn("fg3", F.substring(F.col("facility_group_name"), 1, 3))
          .withColumn(
              "visit_id",
              F.concat_ws("", F.col("patient_id").cast("string"),
                              F.col("appointment_date_str"),
                              F.col("fg3"),
                              F.col("visit_type_id").cast("string"))
          )
          .withColumn(
              "visit_id_group",
              F.concat_ws("", F.col("patient_id").cast("string"),
                              F.col("appointment_date_str"),
                              F.col("fg3"))
          )
    )

def compute_patient_time_and_pwt(pat_df: DataFrame) -> DataFrame:
    """
    Replica tu lógica:
      patient_time = (end_time - start_time) en minutos
      pwt = (max_end - min_start) + minutes_in_status del registro con mayor end_time, por visit_id_group
    """
    # start/end por visit_id
    w_visit = Window.partitionBy("visit_id").orderBy(F.col("log_time").asc())
    w_visit_desc = Window.partitionBy("visit_id").orderBy(F.col("log_time").desc())

    df1 = (pat_df
        .withColumn("start_time", F.first("log_time").over(w_visit))
        .withColumn("end_time",   F.first("log_time").over(w_visit_desc))
        .withColumn("minutes_in_status_new", F.first("minutes_in_status").over(w_visit_desc))
        .dropDuplicates(["visit_id"])
    )

    # calendar + otros campos ya deberían venir unidos antes de llamar a esta función
    df1 = df1.withColumn("patient_time",
                         F.round((F.col("end_time").cast("long") - F.col("start_time").cast("long")) / 60.0).cast("int"))

    # pwt por grupo: min start, max end y minutes del registro con mayor end_time
    w_group = Window.partitionBy("visit_id_group")
    # extra_minutes: tomar el minutes_in_status_new de la fila con end_time max
    rn = F.row_number().over(Window.partitionBy("visit_id_group").orderBy(F.col("end_time").desc()))
    df2 = (df1
        .withColumn("rn", rn)
        .withColumn("min_start", F.min("start_time").over(w_group))
        .withColumn("max_end",   F.max("end_time").over(w_group))
        .withColumn("extra_minutes_candidate",
                    F.when(F.col("rn") == 1, F.col("minutes_in_status_new")).otherwise(F.lit(None)))
        .withColumn("extra_minutes", F.max("extra_minutes_candidate").over(w_group))
        .withColumn("pwt",
            ((F.col("max_end").cast("long") - F.col("min_start").cast("long")) / 60.0) + F.col("extra_minutes").cast("double"))
        .drop("rn", "extra_minutes_candidate")
    )
    return df2
