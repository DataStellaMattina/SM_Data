import sys, time, traceback, os
from datetime import datetime, date

import boto3
from awsglue.utils import getResolvedOptions

import functions as f
import params as p

s3 = boto3.client("s3")

def main():
    start_time = time.time()
    final_message = ""  # para que exista aunque algo falle

    # -------- Obligatorios --------
    required = ["DB_SECRET_NAME", "AWS_REGION", "ENV", "START_DATE", "END_DATE"]
    missing = [k for k in required if f"--{k}" not in sys.argv]
    if missing:
        raise RuntimeError(f"❌ Faltan parámetros del Job: {missing}\n🧰 sys.argv: {sys.argv}")

    # -------- Args del Job --------
    args = getResolvedOptions(sys.argv, required)

    secret = f.get_secret(args["DB_SECRET_NAME"], args["AWS_REGION"])
    env = args["ENV"]
    os.environ["ENV"] = env
    print("Environment:", env)

    # -------- Fechas --------
    start_date = f.parse_yyyy_mm_dd(args["START_DATE"], "START_DATE")
    end_date = f.parse_yyyy_mm_dd(args["END_DATE"], "END_DATE")

    print(f"📅 Rango seleccionado: {start_date} - {end_date}")

    gp = p.GLOBLAL_PARAMS(env)

    # -------- Conexiones a BD --------
    conn_raw = None
    conn_dm = None

    try:
        print("🔌 Abriendo conexiones a PostgreSQL...")
        conn_raw = f.connect_to_postgres(secret, gp.DB_NAME)
        conn_dm = f.connect_to_postgres(secret, gp.DB_NAME_DM)

        print("🧨 Ejecutando DELETEs + NOTICEs...")

        # start_date y end_date pueden ser None o string
        if start_date and end_date:
            # ---- RAW ----
            ok_raw, counts_msg_raw = f.delete_bc_raw_range(conn_raw, start_date, end_date)
            notices_raw = [counts_msg_raw]
            if not ok_raw:
                notices_raw.insert(0, "❌ Falló DELETE RAW Billing Collections")

            msg_raw = f.format_pg_notices(
                title="DELETE RAW Billing Collections",
                notices=notices_raw
            )
            print(msg_raw)

            # ---- DM ----
            ok_dm, counts_msg_dm = f.delete_bc_dm_range(conn_dm, start_date, end_date)
            notices_dm = [counts_msg_dm]
            if not ok_dm:
                notices_dm.insert(0, "❌ Falló DELETE RAW Billing Collections")

            msg_dm = f.format_pg_notices(
                title="DELETE DM Billing Collections",
                notices=notices_dm
            )
            print(msg_dm)

            # ---- Mensaje final ----
            final_message = "\n\n".join([msg_raw, msg_dm])
        else:
            print("⚠️ Variables START_DATE y END_DATE vacías o no enviadas. No se ejecuta DELETE.")
            final_message = "No se ejecutó DELETE: faltan START_DATE/END_DATE."


    finally:
        if conn_raw:
            conn_raw.close()
            print("🔒 Conexión RAW cerrada.")
        if conn_dm:
            conn_dm.close()
            print("🔒 Conexión DM cerrada.")

    # -------- SNS OK --------
    elapsed = time.time() - start_time
    minutes, seconds = divmod(int(elapsed), 60)

    mensaje_ok = (
        f"✅ Billing Collections Delete Finalizado\n"
        f"⏱️ Tiempo de ejecución: {minutes} minutos y {seconds} segundos\n\n"
        f"{final_message}"
    )

    f.enviar_correo_sns("OK | Billing Collections Delete", mensaje_ok)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ Error no controlado en el Job:")
        print(str(e))
        traceback.print_exc()

        try:
            mensaje_error = (
                f"❌ Error en Billing Collections Delete\n{str(e)}\n\n"
                f"Traceback:\n{traceback.format_exc()}"
            )
            f.enviar_correo_sns("ERROR | Billing Collections Delete", mensaje_error)
        except Exception:
            print("⚠️ No se pudo enviar notificación de error.")

        raise
