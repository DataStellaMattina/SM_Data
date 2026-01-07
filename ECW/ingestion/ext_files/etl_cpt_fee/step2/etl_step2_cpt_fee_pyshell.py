import sys, time, json, traceback, os
import io
import boto3, botocore
import pandas as pd
import numpy as np
import pg8000
from awsglue.utils import getResolvedOptions
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

    # -------- Conexiones a BD --------
    conn_raw = None
    conn_dm = None

    try:
        print("🔌 Abriendo conexiones a PostgreSQL...")
        conn_raw = f.connect_to_postgres(secret, gp.DB_NAME)
        conn_dm = f.connect_to_postgres(secret, gp.DB_NAME_DM)

        print("📥 Leyendo parámetros y catálogos desde BD...")
        df_cpt_fee = f.get_aux_cpt_fee(conn_raw)
        df_cpt_id = f.get_cpt_id(conn_dm)

    finally:
        # Cerrar conexiones
        if conn_raw:
            conn_raw.close()
            print("🔒 Conexión RAW cerrada.")
        if conn_dm:
            conn_dm.close()
            print("🔒 Conexión DM cerrada.")

    # --------------------- Transforms --------------------->
    print("⚙️ Iniciando transformaciones...")
    df_cpt_id["cpt_code"] = df_cpt_id["cpt_code"].astype(str)
    df_final = df_cpt_fee.merge(df_cpt_id, how='left', on='cpt_code')
    # Casteo
    df_final["id_fee_plan"] = df_final["id_fee_plan"].astype("Int64")
    df_final["cpt_id"] = df_final["cpt_id"].astype("Int64")

    # Reordenar columnas
    column_order = [
        'cpt_description', 'cpt_code', 'cpt_group',
        'fee_plan', 'fee', 'id_fee_plan', 'cpt_id',
        'mod_1', 'mod_2', 'resource_provider_type'
    ]
    df_final = df_final[column_order]

    print("✅ Preview:")
    print(df_final.head(3))
    print("✅ Rows:", len(df_final))

    # --------- Carga de resultados a la tabla--------
    print("⬆️ Cargando información a tabla...")
    f.update_table(
        secret=secret,
        table_name="public.dim_cpt_fee",
        df=df_final,
        db_name=gp.DB_NAME_DM,
    )

    # Tiempo de ejecución
    elapsed = time.time() - start_time
    minutes, seconds = divmod(int(elapsed), 60)

    mensaje_time = (
        f"✅ Step 2 dim_cpt_fee. "
        f"Tiempo de ejecución: {minutes} minutos y {seconds} segundos"
    )
    asunto_2 = "OK Step 2 dim_cpt_fee."
    f.enviar_correo_sns(asunto_2, mensaje_time)  # Asumiendo que la tienes importada


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Manejo global de errores
        print("❌ Error no controlado en el Job:")
        print(str(e))
        traceback.print_exc()

        # Si quieres enviar correo también en error:
        try:
            mensaje_error = (
                f"❌ Error en dim_cpt_fee Step 2\n{str(e)}\n\n"
                f"Traceback:\n{traceback.format_exc()}"
            )
            f.enviar_correo_sns("ERROR dim_cpt_fee Step 2 ", mensaje_error)
        except Exception:
            print("⚠️ No se pudo enviar notificación de error.")

        # Re-lanzar para que Glue marque el Job como FAILED
        raise
