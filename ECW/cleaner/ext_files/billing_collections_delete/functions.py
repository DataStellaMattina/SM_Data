import sys, time, json, traceback, os
from datetime import datetime
import io
import openpyxl
import pandas as pd
import numpy as np
import pg8000
from awsglue.utils import getResolvedOptions
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


# -------- Secrets Manager --------
def get_secret(secret_name, region_name):
    sm = boto3.client("secretsmanager", region_name=region_name)
    return json.loads(sm.get_secret_value(SecretId=secret_name)["SecretString"])

# -------- Conexión a Postgres --------
def connect_to_postgres(secret: dict, db_name: str):
    """Crea conexión a PostgreSQL con manejo de SSL (si aplica)."""
    try:
        # Puedes habilitar SSL si tu RDS lo requiere:
        # ssl_ctx = ssl.create_default_context()

        conn = pg8000.connect(
            user=secret["username"],
            password=secret["password"],
            host=secret["host"],
            port=int(secret.get("port", 5432)),
            database=db_name,
            # ssl_context=ssl_ctx
        )
        print("✅ Conexión establecida correctamente a PostgreSQL.")
        return conn
    except Exception as e:
        print(f"❌ Error al conectar a PostgreSQL: {e}")
        raise

# -------- Función genérica para ejecutar consultas --------
def run_query(conn, query: str, params=None, desc: str = "") -> pd.DataFrame:
    """Ejecuta una consulta SQL y retorna un DataFrame."""
    try:
        df = pd.read_sql(query, conn, params=params)
        print(f"🟩 {desc or 'Consulta ejecutada'}: {len(df)} registros obtenidos.")
        return df
    except Exception as e:
        print(f"❌ Error en la consulta ({desc}): {e}")
        raise


def run_execute(conn, sql: str, params=None, desc: str = ""):
    """Ejecuta SQL que no retorna resultados (DELETE, DO, CALL, etc)."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        print(f"🟩 {desc or 'SQL ejecutado correctamente'}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error ejecutando SQL ({desc}): {e}")
        raise

def truncate_table(conn, table_name: str):
    """
    Ejecuta TRUNCATE TABLE sobre la tabla indicada usando una conexión abierta.
    No cierra la conexión, solo el cursor.
    """
    cur = conn.cursor()
    try:
        truncate_query = f"TRUNCATE TABLE {table_name};"
        print(f"🧹 Ejecutando: {truncate_query}")
        cur.execute(truncate_query)
        conn.commit()
        print(f"✅ TRUNCATE completado para {table_name}")
    finally:
        cur.close()


def load_to_database(conn, table_name: str, df: pd.DataFrame, batch_size: int = 100_000):
    """
    Inserta un DataFrame en PostgreSQL usando INSERT ... VALUES en batches.
    Usa pg8000 (paramstyle 'format' = %s).
    """
    if df.empty:
        print(f"⚠️ DataFrame vacío, no se inserta nada en {table_name}.")
        return

    cur = conn.cursor()
    try:
        # Nombres de columnas (con comillas dobles por si hay mayúsculas / caracteres raros)
        cols = list(df.columns)
        cols_sql = ", ".join([f'"{c}"' for c in cols])

        # Placeholder para pg8000 (paramstyle 'format' -> %s)
        placeholders = ", ".join(["%s"] * len(cols))
        insert_sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders})"

        print(f"📥 Insertando {len(df)} registros en {table_name} ...")

        # Convertir filas a tuplas de Python, convirtiendo NaN -> None y tipos numpy/pandas a tipos nativos
        def normalize_value(v):
            # NaN / NaT
            if pd.isna(v):
                return None
            # Timestamps de pandas
            if isinstance(v, pd.Timestamp):
                return v.to_pydatetime()
            # numpy genéricos (int64, float64, etc.)
            if isinstance(v, np.generic):
                return v.item()
            return v

        records = [
            tuple(normalize_value(v) for v in row)
            for row in df.itertuples(index=False, name=None)
        ]

        # Ejecutar en batches para no saturar el driver
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            cur.executemany(insert_sql, batch)
            print(f"   🧱 Batch insertado: {i} - {i + len(batch)}")

        conn.commit()
        print(f"✅ Insert finalizado en {table_name}")
    finally:
        cur.close()

def load_to_database_copy(conn, table_name: str, df: pd.DataFrame):
    """
    Inserta un DataFrame usando COPY FROM STDIN con pg8000 + CSV en memoria.
    MUCHÍSIMO más rápido que muchos INSERT.
    """
    if df.empty:
        print(f"⚠️ DataFrame vacío, no se inserta nada en {table_name}.")
        return

    cols = list(df.columns)
    cols_sql = ", ".join(f'"{c}"' for c in cols)

    # Preparar CSV en memoria (sin header)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=False)  # sep="," por default
    csv_buffer.seek(0)

    sql_copy = f"""
        COPY {table_name} ({cols_sql})
        FROM STDIN
        WITH (FORMAT CSV)
    """

    cur = conn.cursor()
    try:
        print(f"🚀 COPY de {len(df)} registros a {table_name} ...")
        cur.execute(sql_copy, stream=csv_buffer)
        conn.commit()
        print(f"✅ COPY finalizado en {table_name}")
    finally:
        cur.close()


def update_table(secret: dict, table_name: str, df: pd.DataFrame, db_name: str):
    """
    Orquesta: conecta a la BD, hace TRUNCATE y luego bulk INSERT del DataFrame.
    Cierra la conexión al final.
    """
    conn = connect_to_postgres(secret, db_name)
    try:
        truncate_table(conn, table_name)
        load_to_database_copy(conn, table_name, df)
    finally:
        conn.close()
        print(f"🔒 Conexión cerrada para {db_name}")


def upgrade_table(
    host: str,
    database: str,
    user: str,
    pwd: str,
    port: int,
    table_source: str,
    table_target: str,
    by_field: str,
):
    """
    Hace 'upgrade' de tabla final tomando los datos de una tabla fuente:
      1) DELETE en la tabla target por las claves (by_field) presentes en la source.
      2) INSERT INTO target SELECT * FROM source.

    Se asume que table_source y table_target tienen la misma estructura de columnas.
    """
    print(f"🚀 Iniciando upgrade_table de {table_source} -> {table_target} por [{by_field}]")

    conn = pg8000.connect(
        user=user,
        password=pwd,
        host=host,
        port=port,
        database=database,
    )

    cur = conn.cursor()
    try:
        # IMPORTANTE: si by_field es fecha, esto funcionará bien y usará los valores distintos de la fuente
        sql = f"""
        BEGIN;

        DELETE FROM {table_target}
        WHERE {by_field} IN (
            SELECT DISTINCT {by_field}
            FROM {table_source}
        );

        INSERT INTO {table_target}
        SELECT *
        FROM {table_source};

        COMMIT;
        """
        print("🧾 Ejecutando sentencia de upgrade_table...")
        cur.execute(sql)
        conn.commit()
        print(f"✅ Upgrade completado en {table_target}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error en upgrade_table: {e}")
        raise
    finally:
        cur.close()
        conn.close()
        print("🔒 Conexión cerrada en upgrade_table")

# =========================
# RAW
# =========================

def count_billing_collections_det(conn, start_date: str, end_date: str) -> int:
    sql = """
    SELECT COUNT(*) AS cnt
    FROM raw_bank.billing_collections_det
    WHERE sheet_date BETWEEN %s AND %s;
    """
    df = run_query(conn, sql, params=[start_date, end_date], desc="Count billing_collections_det")
    return int(df.iloc[0]["cnt"])


def count_billing_collections(conn, start_date: str, end_date: str) -> int:
    sql = """
    SELECT COUNT(*) AS cnt
    FROM raw_bank.billing_collections
    WHERE sheet_date BETWEEN %s AND %s;
    """
    df = run_query(conn, sql, params=[start_date, end_date], desc="Count billing_collections")
    return int(df.iloc[0]["cnt"])


def count_bill_facility_coll(conn, start_date: str, end_date: str) -> int:
    sql = """
    SELECT COUNT(*) AS cnt
    FROM raw_bank.bill_facility_coll
    WHERE sheet_date BETWEEN %s AND %s;
    """
    df = run_query(conn, sql, params=[start_date, end_date], desc="Count bill_facility_coll")
    return int(df.iloc[0]["cnt"])


def count_bill_sheet_coll(conn, start_date: str, end_date: str) -> int:
    sql = """
    SELECT COUNT(*) AS cnt
    FROM raw_bank.bill_sheet_coll
    WHERE sheet_date BETWEEN %s AND %s;
    """
    df = run_query(conn, sql, params=[start_date, end_date], desc="Count bill_sheet_coll")
    return int(df.iloc[0]["cnt"])


# =========================
# DELETES RAW (4 funciones)
# =========================

def delete_billing_collections_det(conn, start_date: str, end_date: str) -> int:
    sql = """
    DELETE FROM raw_bank.billing_collections_det
    WHERE sheet_date BETWEEN %s AND %s;
    """
    run_execute(conn, sql, params=[start_date, end_date], desc="Delete billing_collections_det")
    # opcional: si quieres filas afectadas exactas, hay que usar cur.rowcount
    return 1


def delete_billing_collections(conn, start_date: str, end_date: str) -> int:
    sql = """
    DELETE FROM raw_bank.billing_collections
    WHERE sheet_date BETWEEN %s AND %s;
    """
    run_execute(conn, sql, params=[start_date, end_date], desc="Delete billing_collections")
    return 1


def delete_bill_facility_coll(conn, start_date: str, end_date: str) -> int:
    sql = """
    DELETE FROM raw_bank.bill_facility_coll
    WHERE sheet_date BETWEEN %s AND %s;
    """
    run_execute(conn, sql, params=[start_date, end_date], desc="Delete bill_facility_coll")
    return 1


def delete_bill_sheet_coll(conn, start_date: str, end_date: str) -> int:
    sql = """
    DELETE FROM raw_bank.bill_sheet_coll
    WHERE sheet_date BETWEEN %s AND %s;
    """
    run_execute(conn, sql, params=[start_date, end_date], desc="Delete bill_sheet_coll")
    return 1


# =========================
# Orquestador RAW
# =========================

def delete_bc_raw_range(conn, start_date: str, end_date: str) -> tuple[bool, str]:
    """
    1) Cuenta
    2) Borra (en el orden que tú definas)
    """
    try:
        c1 = count_billing_collections_det(conn, start_date, end_date)
        c2 = count_billing_collections(conn, start_date, end_date)
        c3 = count_bill_facility_coll(conn, start_date, end_date)
        c4 = count_bill_sheet_coll(conn, start_date, end_date)

        msg = (
            "📌 Counts:\n"
            f"  - billing_collections_det : {c1}\n"
            f"  - billing_collections     : {c2}\n"
            f"  - bill_facility_coll      : {c3}\n"
            f"  - bill_sheet_coll         : {c4}"
        )

        print(msg)

        # OJO: el orden puede importar si hay FK (hijos -> padres)
        delete_billing_collections_det(conn, start_date, end_date)
        delete_billing_collections(conn, start_date, end_date)
        delete_bill_facility_coll(conn, start_date, end_date)
        delete_bill_sheet_coll(conn, start_date, end_date)

        return True, msg

    except Exception as e:
        err_msg = f"❌ Error en delete_bc_raw_range: {e}"
        print(err_msg)
        return False, err_msg

# =========================
# DM
# =========================
def count_report_sheet_coll(conn, start_date: str, end_date: str) -> int:
    sql = """
    SELECT COUNT(*) AS cnt
    FROM public.report_sheet_coll
    WHERE sheet_date BETWEEN %s AND %s;
    """
    df = run_query(conn, sql, params=[start_date, end_date], desc="Count report_sheet_coll")
    return int(df.iloc[0]["cnt"])


def count_report_billing_coll(conn, start_date: str, end_date: str) -> int:
    sql = """
    SELECT COUNT(*) AS cnt
    FROM public.report_billing_coll
    WHERE bank_dept_dt BETWEEN %s AND %s;
    """
    df = run_query(conn, sql, params=[start_date, end_date], desc="Count report_billing_coll")
    return int(df.iloc[0]["cnt"])

# DELETES (DM)
def delete_report_sheet_coll(conn, start_date: str, end_date: str) -> None:
    sql = """
    DELETE FROM public.report_sheet_coll
    WHERE sheet_date BETWEEN %s AND %s;
    """
    run_execute(conn, sql, params=[start_date, end_date], desc="Delete report_sheet_coll")


def delete_report_billing_coll(conn, start_date: str, end_date: str) -> None:
    sql = """
    DELETE FROM public.report_billing_coll
    WHERE bank_dept_dt BETWEEN %s AND %s;
    """
    run_execute(conn, sql, params=[start_date, end_date], desc="Delete report_billing_coll")


# =========================
# Orquestador DM
# =========================

def delete_bc_dm_range(conn, start_date: str, end_date: str) -> tuple[bool, str]:
    try:
        c_sheet = count_report_sheet_coll(conn, start_date, end_date)
        c_bill  = count_report_billing_coll(conn, start_date, end_date)

        msg = (
            "📌 DELETE REPORTS RANGE\n"
            f"🗓️ Fechas seleccionadas: {start_date} - {end_date}\n"
            "📊 Conteo previo:\n"
            f"  - report_sheet_coll   : {c_sheet}\n"
            f"  - report_billing_coll : {c_bill}"
        )

        print(msg)

        # delete (si hay FK, ajusta orden)
        delete_report_sheet_coll(conn, start_date, end_date)
        delete_report_billing_coll(conn, start_date, end_date)

        ok_msg = msg + "\n✅ Deletes ejecutados correctamente."
        print(ok_msg)
        return True, ok_msg

    except Exception as e:
        err_msg = (
            "❌ Error en delete_reports_range\n"
            f"🗓️ Rango: {start_date} - {end_date}\n"
            f"Detalles: {e}"
        )
        print(err_msg)
        return False, err_msg



def format_pg_notices(title: str, notices: list[str]) -> str:
    """
    Limpia y formatea NOTICEs de PostgreSQL para logs / correo.
    """
    if not notices:
        return f"📭 {title}\nSin mensajes."

    lines = [f"📢 {title}"]
    for n in notices:
        clean = n.replace("NOTICE:", "").strip()
        lines.append(f" - {clean}")

    return "\n".join(lines)

def parse_yyyy_mm_dd(s: str, arg_name: str) -> str:
    """
    Valida 'YYYY-MM-DD' y regresa el mismo string (DB lo castea a DATE).
    """
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        raise ValueError(
            f"❌ Formato inválido para {arg_name}='{s}'. Usa YYYY-MM-DD (ej: 2025-12-30)"
        )

#---> SNS
def enviar_correo_sns(asunto_2, mensaje_time):

    asunto = asunto_2  
    mensaje =  mensaje_time
    #destinatario = 'data@stellamattina.com'

    try:
        # Configurar el cliente SNS
        sns_client = boto3.client('sns', region_name='us-east-2')

        # Enviar el correo electrónico
        response = sns_client.publish(
            TopicArn='arn:aws:sns:us-east-2:558259274809:End_update_daily',
            Subject=asunto,
            Message=mensaje,
            MessageAttributes={
                'AWS.SNS.SMS.SMSType': {
                    'DataType': 'String',
                    'StringValue': 'Transactional'
                }
            }
        )

        print(f'Correo enviado correctamente. Message ID: {response.get("MessageId")}')

    except NoCredentialsError:
        print('No se encontraron credenciales de AWS.')
    except PartialCredentialsError:
        print('Faltan credenciales de AWS.')
    except Exception as ex:
        print(f'Ocurrió un error al enviar correo: {str(ex)}')

    pass