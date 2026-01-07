import sys, time, json, traceback, os
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
def run_query(conn, query: str, desc: str = "") -> pd.DataFrame:
    """Ejecuta una consulta SQL y retorna un DataFrame."""
    try:
        df = pd.read_sql(query, conn)
        print(f"🟩 {desc or 'Consulta ejecutada'}: {len(df)} registros obtenidos.")
        return df
    except Exception as e:
        print(f"❌ Error en la consulta ({desc}): {e}")
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

def load_to_database(conn, table_name: str, df: pd.DataFrame):
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

def load_to_database_batch(conn, table_name: str, df: pd.DataFrame, batch_size: int = 10_000):
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


def update_table(secret: dict, table_name: str, df: pd.DataFrame, db_name: str):
    """
    Orquesta: conecta a la BD, hace TRUNCATE y luego bulk INSERT del DataFrame.
    Cierra la conexión al final.
    """
    conn = connect_to_postgres(secret, db_name)
    try:
        truncate_table(conn, table_name)
        load_to_database(conn, table_name, df)
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

# -------- Consultas específicas --------
def get_cpt_id(conn):
    query = """
        SELECT cpt_id, cpt_code FROM public.dim_cpt;
    """
    return run_query(conn, query, "get public.dim_cpt")

def get_aux_cpt_fee(conn):
    query = """SELECT 
                    cpt_description,
                    cpt_code,
                    cpt_group,
                    mod_1,
                    mod_2,
                    resource_provider_type,
                    fee_plan,
                    fee,
                    id_fee_plan
            FROM aux.cpt_fee;"""
    return run_query(conn, query, "get aux.cpt_fee")

def enviar_correo_sns(asunto_2, mensaje_time):

    asunto = asunto_2
    mensaje =  mensaje_time

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