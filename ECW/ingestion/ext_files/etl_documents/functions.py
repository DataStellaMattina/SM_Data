import sys, time, json, traceback
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pg8000
import boto3, botocore
import functions as f
import params as p


s3 = boto3.client("s3")

def main():
    start_time = time.time()

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

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    job.commit()
    
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
    
    # -------- Conexiones a BD --------
    conn_raw = None
    conn_dm = None

    try:
        print("🔌 Abriendo conexiones a PostgreSQL...")
        conn_raw = f.connect_to_postgres(secret, gp.DB_NAME)
        conn_dm = f.connect_to_postgres(secret, gp.DB_NAME_DM)

        print("📥 Leyendo parámetros y catálogos desde BD...")
        calendar_detail = f.get_calendar_detail(conn_raw)
        billing_coll_rep = f.get_billing_coll_rep(conn_raw)
        target_coll = f.get_target_coll(conn_raw)
        sheet_coll_rep = f.get_sheet_coll_rep(conn_raw)
        facility_group_info = f.get_facilities(conn_dm)

    finally:
        # Cerrar conexiones
        if conn_raw:
            conn_raw.close()
            print("🔒 Conexión RAW cerrada.")
        if conn_dm:
            conn_dm.close()
            print("🔒 Conexión DM cerrada.")
