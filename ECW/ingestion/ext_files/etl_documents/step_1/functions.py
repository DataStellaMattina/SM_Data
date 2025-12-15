# functions.py
import re
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import json
import traceback
import datetime
import math
from datetime import datetime
from zoneinfo import ZoneInfo


s3 = boto3.client("s3")

# -------- Secrets Manager --------
def get_secret(secret_name, region_name):
    sm = boto3.client("secretsmanager", region_name=region_name)
    return json.loads(sm.get_secret_value(SecretId=secret_name)["SecretString"])

# ---------- Utilidades ----------
def build_postgres_jdbc_url(secret: dict, db: str) -> str:
    endpoint = secret["host"]
    port = secret.get("port", 5432)

    # db = nombre de la base que quieres usar (argumento)
    return f"jdbc:postgresql://{endpoint}:{port}/{db}"


def read_tsv_utf16(spark, s3_path: str, read_options: dict) -> DataFrame:
    reader = spark.read
    for k, v in read_options.items():
        reader = reader.option(k, v)
    return reader.csv(s3_path)

# ---------- Transformaciones ----------
def rename_columns(df: DataFrame, rename_map: dict) -> DataFrame:
    for old_col, new_col in rename_map.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)
    return df

def cast_columns(df: DataFrame, schema_map: dict, date_fmt: str) -> DataFrame:
    # Crea columnas faltantes como null con el tipo correcto
    for col, dtype in schema_map.items():
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast(dtype))

    # Castea / parsea fechas
    for col, dtype in schema_map.items():
        if col in df.columns:
            if isinstance(dtype, T.DateType):
                df = df.withColumn(col, F.to_date(F.col(col), date_fmt))
            elif isinstance(dtype, T.TimestampType):
                df = df.withColumn(col, F.to_timestamp(F.col(col), date_fmt))
            else:
                df = df.withColumn(col, F.col(col).cast(dtype))

    # Mantén solo columnas declaradas
    df = df.select(*schema_map.keys())
    return df

def clean_text_col(col):
    # elimina BOM, zero-width y no imprimibles; luego trim
    cleaned = F.regexp_replace(col, u"[\uFEFF\u200B\u200C\u200D\u2060]", "")
    cleaned = F.regexp_replace(cleaned, r"[\x00-\x1F\x7F]", "")
    return F.trim(cleaned)

def apply_text_cleaning(df: DataFrame, text_cols: list[str]) -> DataFrame:
    out = df
    for c in text_cols:
        if c in out.columns:
            out = out.withColumn(c, clean_text_col(F.col(c)))
    return out

def normalize_yes_no(df: DataFrame, cols: list[str]) -> DataFrame:
    out = df
    for c in cols:
        if c in out.columns:
            out = out.withColumn(
                c,
                F.when(F.lower(F.col(c)).startswith("y"), F.lit("Yes"))
                 .when(F.lower(F.col(c)).startswith("n"), F.lit("No"))
                 .otherwise(F.col(c))
            )
    return out

# ---------- Escritura JDBC ----------
def insert_overwrite_jdbc(
    df: DataFrame,
    jdbc_url: str,
    user: str,
    password: str,
    schema: str,
    table: str,
    batchsize: int = 10000,
    num_partitions: int | None = None,
    partition_column: str | None = None,
    lower_bound: int | None = None,
    upper_bound: int | None = None,
):
    writer = (
        df.write
          .format("jdbc")
          .mode("overwrite")
          .option("truncate", "true")  # TRUNCATE antes de insertar
          .option("url", jdbc_url)
          .option("dbtable", f"{schema}.{table}")
          .option("user", user)
          .option("password", password)
          .option("driver", "org.postgresql.Driver")
          .option("batchsize", str(batchsize))
    )

    if num_partitions and partition_column and lower_bound is not None and upper_bound is not None:
        writer = (
            writer
            .option("numPartitions", str(num_partitions))
            .option("partitionColumn", partition_column)
            .option("lowerBound", str(lower_bound))
            .option("upperBound", str(upper_bound))
        )

    writer.save()

#sns
def _region_from_topic_arn(topic_arn: str) -> str:
    """
    ARN típico: arn:aws:sns:<region>:<account-id>:<topic-name>
    """
    try:
        return topic_arn.split(":")[3]
    except Exception:
        # fallback (deja que boto3 use la región del entorno/rol)
        return None

def enviar_correo_sns(topic_arn: str, subject: str, message: str) -> str | None:
    """
    Publica un mensaje en un tópico SNS.
    - subject: máx 100 chars
    - message: string (puede ser JSON si gustas)

    Returns:
        message_id (str) si success, o None si falla.
    """
    try:
        region = _region_from_topic_arn(topic_arn)
        sns_client = boto3.client("sns", region_name=region) if region else boto3.client("sns")

        # Límite SNS de 100 chars para Subject
        subject = (subject or "")[:100]

        resp = sns_client.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message,
        )
        mid = resp.get("MessageId")
        print(f"[INFO] SNS publicado: topic={topic_arn} message_id={mid}")
        return mid

    except NoCredentialsError:
        print("[ERROR] SNS: No se encontraron credenciales de AWS.")
    except PartialCredentialsError:
        print("[ERROR] SNS: Credenciales de AWS incompletas.")
    except Exception as ex:
        print(f"[ERROR] SNS publish falló: {str(ex)}")
    return None

#S3 move 
# -------------------- Helpers ya existentes --------------------

def _parse_s3_uri(s3_uri: str):
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"URI inválida: {s3_uri}")
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    return bucket, key

def _object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def _normalize_history(bucket_src: str, history_prefix: str):
    """
    Acepta:
      - history_prefix como 's3://bucket/prefix'  -> usa ese bucket/prefix
      - o como 'prefix/sin/bucket'                -> usa bucket_src y ese prefix
    Devuelve: (bucket_dst, key_prefix_sin_slash_final)
    """
    if history_prefix.startswith("s3://"):
        b, p = _parse_s3_uri(history_prefix)
        return b, p.rstrip("/")
    else:
        return bucket_src, history_prefix.rstrip("/")

# -------------------- BORRADO (ya existente) --------------------

def delete_input_file(s3_input: str, purge_versions: bool = False) -> bool:
    bucket, key = _parse_s3_uri(s3_input)

    if not purge_versions and not _object_exists(bucket, key):
        print(f"[WARN] No existe: s3://{bucket}/{key}")
        return False

    # ¿Tiene versioning?
    try:
        ver = s3.get_bucket_versioning(Bucket=bucket)
        versioning_enabled = ver.get("Status") == "Enabled"
    except ClientError:
        versioning_enabled = False

    # Soft delete
    if not purge_versions or not versioning_enabled:
        try:
            s3.delete_object(Bucket=bucket, Key=key)
            print(f"[INFO] Borrado (soft): s3://{bucket}/{key}")
            return True
        except ClientError as e:
            print(f"[ERROR] delete_object: {e}")
            raise

    # Purga completa
    deleted_any = False
    paginator = s3.get_paginator("list_object_versions")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=key):
            to_delete = []
            for v in page.get("Versions", []):
                if v["Key"] == key:
                    to_delete.append({"Key": key, "VersionId": v["VersionId"]})
            for dm in page.get("DeleteMarkers", []):
                if dm["Key"] == key:
                    to_delete.append({"Key": key, "VersionId": dm["VersionId"]})

            if to_delete:
                for i in range(0, len(to_delete), 1000):
                    chunk = {"Objects": to_delete[i:i+1000], "Quiet": True}
                    s3.delete_objects(Bucket=bucket, Delete=chunk)
                deleted_any = True

        if deleted_any:
            print(f"[INFO] Purga completa de versiones: s3://{bucket}/{key}")
        else:
            print(f"[WARN] No se encontraron versiones para: s3://{bucket}/{key}")

        return deleted_any

    except ClientError as e:
        print(f"[ERROR] purge_versions: {e}")
        raise

# -------------------- MOVE (ajustada: sin borrar fuente) --------------------

def move_to_history_with_date(
    s3_input: str,
    history_prefix: str,
    base_name: str = "Documents",
    tz_name: str = "America/Mexico_City",
    keep_extension: bool = True,
    delete_source: bool = False  # <--- NUEVO (por compatibilidad, default False)
) -> str:
    """
    Copia s3_input -> history_prefix/{base_name}_MM.dd.YYYY[.ext]
    (no borra la fuente a menos que delete_source=True)
    """
    bucket_src, key_src = _parse_s3_uri(s3_input)
    bucket_dst, history_key_prefix = _normalize_history(bucket_src, history_prefix)

    # extensión
    ext = ""
    if keep_extension:
        m = re.search(r"(\.[A-Za-z0-9]+)$", key_src)
        ext = m.group(1) if m else ".csv"

    stamp = datetime.now(ZoneInfo(tz_name)).strftime("%m.%d.%Y")
    base_dest_name = f"{base_name}_{stamp}{ext}"
    key_dst = f"{history_key_prefix}/{base_dest_name}"

    # evitar overwrite
    if _object_exists(bucket_dst, key_dst):
        v = 2
        while True:
            candidate = f"{history_key_prefix}/{base_name}_{stamp}_v{v}{ext}"
            if not _object_exists(bucket_dst, candidate):
                key_dst = candidate
                break
            v += 1

    print(f"[INFO] move_to_history src_bucket={bucket_src} src_key={key_src}")
    print(f"[INFO] move_to_history dst_bucket={bucket_dst} dst_key={key_dst}")

    # copiar
    s3.copy_object(
        Bucket=bucket_dst,
        CopySource={"Bucket": bucket_src, "Key": key_src},
        Key=key_dst,
    )

    # (opcional) borrar aquí, pero preferimos hacerlo en el wrapper después de verificar
    if delete_source:
        s3.delete_object(Bucket=bucket_src, Key=key_src)

    print(f"[INFO] Copiado a s3://{bucket_dst}/{key_dst}")
    return f"s3://{bucket_dst}/{key_dst}"

# -------------------- WRAPPER: mover y luego borrar --------------------

def move_to_history_then_delete(
    s3_input: str,
    history_prefix: str,
    base_name: str = "patient_enable",
    tz_name: str = "America/Mexico_City",
    keep_extension: bool = True,
    verify_copy: bool = True,
    purge_versions_on_delete: bool = False
) -> str:
    """
    1) Copia el archivo a history con nombre con fecha.
    2) (Opcional) Verifica que la copia exista y pese lo mismo.
    3) Borra el archivo original en input (opcionalmente purgando versiones).
    Devuelve el S3 URI destino en history.
    """
    # 1) Copiar (sin borrar fuente)
    dest_uri = move_to_history_with_date(
        s3_input=s3_input,
        history_prefix=history_prefix,
        base_name=base_name,
        tz_name=tz_name,
        keep_extension=keep_extension,
        delete_source=False  # 🔒 no borramos aún
    )

    if verify_copy:
        # Verificar existencia y tamaño
        src_bucket, src_key = _parse_s3_uri(s3_input)
        dst_bucket, dst_key = _parse_s3_uri(dest_uri)

        try:
            src_head = s3.head_object(Bucket=src_bucket, Key=src_key)
            dst_head = s3.head_object(Bucket=dst_bucket, Key=dst_key)
            src_size = src_head["ContentLength"]
            dst_size = dst_head["ContentLength"]
            if src_size != dst_size:
                raise RuntimeError(
                    f"Tamaños no coinciden (src={src_size}, dst={dst_size}) para {s3_input} -> {dest_uri}"
                )
            print(f"[INFO] Verificación OK: tamaños coinciden ({src_size} bytes).")
        except ClientError as e:
            # Si falla la verificación, NO borramos el original
            print(f"[ERROR] Verificando copia: {e}")
            raise

    # 3) Borrar fuente
    delete_input_file(s3_input, purge_versions=purge_versions_on_delete)
    print(f"[INFO] Move+Delete completado: {s3_input} -> {dest_uri}")
    return dest_uri

def estimate_df_size_mb(
    df: DataFrame,
    sample_frac: float = 0.01,
    max_sample_rows: int = 10000
) -> float:
    """
    Estima el tamaño del DataFrame en MB usando una muestra.
    """
    # Tomamos una muestra (hasta max_sample_rows)
    sample_df = df.sample(False, sample_frac).limit(max_sample_rows)

    rows = list(sample_df.toLocalIterator())
    n = len(rows)
    if n == 0:
        return 0.0

    total_bytes = 0
    for row in rows:
        as_dict = row.asDict(recursive=True)
        as_json = json.dumps(as_dict, default=str)
        total_bytes += len(as_json.encode("utf-8"))

    avg_row_bytes = total_bytes / n

    # Conteo total de filas del DF completo
    total_rows = df.count()

    estimated_total_bytes = avg_row_bytes * total_rows
    estimated_total_mb = estimated_total_bytes / (1024 ** 2)

    return round(estimated_total_mb, 2)


def calc_num_partitions(
    df: DataFrame,
    target_size_mb: int = 256,
    sample_frac: float = 0.01,
    max_sample_rows: int = 10000
) -> int:
    """
    Calcula el número recomendado de particiones para un DF
    en función de un tamaño objetivo (MB) por partición.
    """
    est_mb = estimate_df_size_mb(
        df,
        sample_frac=sample_frac,
        max_sample_rows=max_sample_rows
    )

    if est_mb == 0:
        # fallback: al menos 1 partición
        return 1

    num_partitions = math.ceil(est_mb / float(target_size_mb))
    return max(1, int(num_partitions))
