# main.py (Glue 4.0 + CloudWatch logging mejorado)
import sys, time, json, traceback
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import params as P
import functions as Fnx

# ---------------- Args del Job ----------------
ARGS = getResolvedOptions(
    sys.argv,
    [
        # RAW
        "JOB_NAME",
        "s3_input",
        "jdbc_endpoint",
        "jdbc_port",
        "jdbc_dbname",
        "db_user",
        "db_password",
        "target_schema",
        "target_table",

        # DM
        "jdbc_dbname_dm",
        "target_schema_dm",
        "target_table_dm",

        # S3 history + SNS
        "s3_output",         # prefijo history (carpeta destino)
        "sns_topic",

        # Opcionales
        "date_format",       # yyyy-MM-dd
        "batchsize",         # 10000
        "num_partitions",    # 0 (sin particionar) o 4/8...
        "partition_column",  # ej: patient_id
        "lower_bound",       # ej: 1
        "upper_bound",       # ej: 10000000
        "log_level",         # DEBUG|INFO|WARN|ERROR
    ]
)

# ---------------- Contexto Glue ----------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(ARGS["JOB_NAME"], ARGS)

# ---------------- Logger JSON ----------------
log = glueContext.get_logger()
log_level = ARGS.get("log_level", "INFO").upper()
spark.sparkContext.setLogLevel(log_level)

def jlog(level: str, event: str, **kwargs):
    payload = {"event": event, **kwargs}
    msg = json.dumps(payload, default=str)
    if level == "error":
        log.error(msg)
    else:
        log.info(msg)

def timer():
    t0 = time.time()
    return lambda: round(time.time() - t0, 2)

jlog("info", "job_start", job_name=ARGS["JOB_NAME"], log_level=log_level)

# ---------------- Parámetros ----------------
# RAW JDBC
jdbc_url = Fnx.build_postgres_jdbc_url(
    endpoint=ARGS["jdbc_endpoint"],
    port=int(ARGS["jdbc_port"]),
    dbname=ARGS["jdbc_dbname"],
)
# DM JDBC
jdbc_url_dm = Fnx.build_postgres_jdbc_url(
    endpoint=ARGS["jdbc_endpoint"],
    port=int(ARGS["jdbc_port"]),
    dbname=ARGS["jdbc_dbname_dm"],
)

# credenciales
db_user = ARGS["db_user"]
db_password = ARGS["db_password"]

# tablas
target_schema     = ARGS["target_schema"]
target_table      = ARGS["target_table"]
target_schema_dm  = ARGS["target_schema_dm"]
target_table_dm   = ARGS["target_table_dm"]

# S3 / SNS
s3_input   = ARGS["s3_input"]
s3_history = ARGS["s3_output"]     # prefijo carpeta history_files
sns_topic  = ARGS["sns_topic"]

# tuning
date_fmt  = ARGS.get("date_format", P.DATE_FORMAT)
batchsize = int(ARGS.get("batchsize", "10000"))
num_parts = int(ARGS.get("num_partitions", "0")) or None
pcol      = ARGS.get("partition_column", "patient_id")
lower_bd  = int(ARGS.get("lower_bound", "1")) if num_parts else None
upper_bd  = int(ARGS.get("upper_bound", "10000000")) if num_parts else None

# Seguridad: validar particionado
if num_parts and lower_bd is None or upper_bd is None:
    jlog("error", "partition_bounds_missing",
         num_partitions=num_parts, lower_bound=lower_bd, upper_bound=upper_bd)
    raise ValueError("Si usas num_partitions > 0 debes proveer lower_bound y upper_bound.")

t_job = timer()
archivado_uri = None
rows = None

try:
    # -------- Read (TSV UTF-16) --------
    t = timer()
    jlog("info", "read_start", path=s3_input, options=P.READ_OPTIONS)
    df = Fnx.read_tsv_utf16(spark, s3_input, P.READ_OPTIONS)

    jlog("info", "read_done", cols=len(df.columns), elapsed_s=t())

    # -------- Rename --------
    t = timer()
    df = Fnx.rename_columns(df, P.RENAME_MAP)
    jlog("info", "rename_done", elapsed_s=t())

    # -------- Cast --------
    t = timer()
    df = Fnx.cast_columns(df, P.SCHEMA_MAP, date_fmt)
    jlog("info", "cast_done", elapsed_s=t())

    # -------- Limpieza --------
    t = timer()
    df = Fnx.apply_text_cleaning(df, P.TEXT_COLS)
    df = Fnx.normalize_yes_no(df, P.YN_COLS)
    jlog("info", "clean_done", elapsed_s=t())

    # -------- Persist para evitar recomputes --------
    df = df.persist()

    # -------- Conteo --------
    t = timer()
    rows = df.count()
    jlog("info", "count_done", rows=rows, elapsed_s=t())

    if rows == 0:
        jlog("info", "empty_input", message="No hay filas que escribir; se omite carga.")
        # Puedes salir temprano o seguir con DM/archivado a conveniencia.

    # Verificar columna de partición exista si se va a usar
    if num_parts and pcol not in df.columns:
        jlog("error", "partition_column_missing", partition_column=pcol, available_cols=df.columns)
        raise ValueError(f"La columna de partición '{pcol}' no existe en el DataFrame.")

    # -------- WRITE RAW --------
    t = timer()
    Fnx.insert_overwrite_jdbc(
        df=df,
        jdbc_url=jdbc_url,
        user=db_user,
        password=db_password,
        schema=target_schema,
        table=target_table,
        batchsize=batchsize,
        num_partitions=num_parts,
        partition_column=pcol if num_parts else None,
        lower_bound=lower_bd,
        upper_bound=upper_bd,
    )
    jlog("info", "write_done_raw",
         target=f"{target_schema}.{target_table}",
         rows=rows, elapsed_s=t())

    # -------- WRITE DM --------
    t = timer()
    Fnx.insert_overwrite_jdbc(
        df=df,
        jdbc_url=jdbc_url_dm,
        user=db_user,
        password=db_password,
        schema=target_schema_dm,
        table=target_table_dm,
        batchsize=batchsize,
        num_partitions=num_parts,
        partition_column=pcol if num_parts else None,
        lower_bound=lower_bd,
        upper_bound=upper_bd,
    )
    jlog("info", "write_done_dm",
         target=f"{target_schema_dm}.{target_table_dm}",
         rows=rows, elapsed_s=t())

    # -------- Mover a history --------
    t = timer()
    try:
        # Mueve a history con fecha y luego borra de input (flujo atómico)
        archivado_uri = Fnx.move_to_history_then_delete(
            s3_input=s3_input,                 # URI completa s3://...
            history_prefix=s3_history,         # prefijo carpeta history_files o URI completa
            base_name="patient_enable",
            tz_name="America/Mexico_City",
            keep_extension=True,
            verify_copy=True,                  # recomienda dejar True
            purge_versions_on_delete=False     # pon True si tu bucket tiene versioning y quieres purgar
        )
    
        # Un solo log de éxito (ya no llamamos delete_input_file por separado)
        jlog("info", "archive_done", archived_uri=archivado_uri, elapsed_s=t())
    
    except Exception as arch_e:
        # No tumbar el job por un fallo de archivado; solo loggear
        jlog("error", "archive_failed", error=str(arch_e), stack=traceback.format_exc())

    # -------- Notificación SNS (éxito) --------
    try:
        mensaje_ok = (
            "Job Etl_patient_enable ✅\n\n"
            "RAW Ebo Report:\n"
            "\t• tbl: patient_enable\n"
            "Data Mart:\n"
            "\t• tbl: dim_patient_enable\n\n"
            "Estado: listo."
        )
        _mid = Fnx.enviar_correo_sns(
            topic_arn=sns_topic,
            subject="✅ End Update Glue ETL Patient Enable " ,
            message=mensaje_ok
        )
        jlog("info", "sns_success", topic_arn=sns_topic, message_id=_mid)
    except Exception as sns_e:
        jlog("error", "sns_failed", topic_arn=sns_topic, error=str(sns_e))


except Exception as e:
    # -------- Log error + SNS --------
    # Log de error del job
    err_stack = traceback.format_exc()
    jlog("error", "job_error", error=str(e), stack=err_stack)

    # Notificación SNS de fallo (no olvides pasar sns_topic)
    try:
        mensaje_error = (
            "Job Etl_patient_enable ❌\n\n"
            f"Error: {str(e)}\n\n"
            "Stacktrace:\n"
            f"{err_stack}"
        )
        _mid = Fnx.enviar_correo_sns(
            topic_arn=sns_topic,
            subject="❌ Glue Job Fallido - Patient Enable ",
            message=mensaje_error
        )
        jlog("info", "sns_failure_notice_sent", topic_arn=sns_topic, message_id=_mid)
    except Exception as sns_e:
        jlog("error", "sns_failure_notice_failed", topic_arn=sns_topic, error=str(sns_e))

    raise


finally:
    job.commit()
    jlog("info", "job_commit")
