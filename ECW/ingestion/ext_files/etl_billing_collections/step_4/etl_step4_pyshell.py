import sys, time, json, traceback, os
import io
import boto3, botocore
import pandas as pd
import numpy as np
import pg8000
from awsglue.utils import getResolvedOptions
import functions as f
import params as p
import datetime


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

    # --------------------- Transforms --------------------->
    print("⚙️ Iniciando transformaciones...")
    today = datetime.datetime.today().date()
    facility_groups = billing_coll_rep['facility_group_id'].unique()
    fgroups = facility_groups 
    calendar_detail_list = []
    for fgroup in fgroups:
        calendar_detail_fac = calendar_detail.copy()
        calendar_detail_fac['facility_group_id'] = fgroup
        calendar_detail_list.append(calendar_detail_fac)
    
    calendar_detail_by_fac = pd.concat(calendar_detail_list).reset_index().drop(columns=['index'])
    
    billing_coll_rep = pd.merge(calendar_detail_by_fac ,billing_coll_rep,on=['bank_dept_dt','facility_group_id'],how='left')
    billing_coll_rep['amount'] = billing_coll_rep['amount'].fillna(0)
    billing_coll_rep['deduct'] = billing_coll_rep['deduct'].fillna(0)
    billing_coll_rep['net_amount'] = billing_coll_rep['amount'] - billing_coll_rep['deduct']
    billing_coll_rep = pd.merge(billing_coll_rep,facility_group_info,on=['facility_group_id'],how='left')

    current_week_id = billing_coll_rep.loc[billing_coll_rep['bank_dept_dt'] == today,'bank_dept_calendar_week_id'].values[0]
    current_year = billing_coll_rep.loc[billing_coll_rep['bank_dept_dt'] == today,'bank_dept_year'].values[0]
    current_period = billing_coll_rep.loc[billing_coll_rep['bank_dept_dt']== today,'bank_dept_period'].values[0]

    billing_coll_rep['week_closed'] = np.where(billing_coll_rep['bank_dept_calendar_week_id'] < current_week_id,1,0)
    billing_coll_rep = pd.merge(billing_coll_rep,target_coll,on=['bank_dept_dt','facility_group_id'],how='left')
    billing_coll_rep['daily_coll_target'] = billing_coll_rep['daily_coll_target'].fillna(0)
    
    billing_coll_rep['daily_coll_target_86'] = billing_coll_rep['daily_coll_target']*0.86
    billing_coll_rep['daily_coll_target_100'] = billing_coll_rep['daily_coll_target']*1
    billing_coll_rep['daily_coll_target_112'] = billing_coll_rep['daily_coll_target']*1.12
   
    billing_coll_rep['current_year'] = np.where((billing_coll_rep['bank_dept_year']==current_year) ,1,0)
    billing_coll_rep['current_period'] = np.where((billing_coll_rep['bank_dept_period']==current_period),1,0)
    billing_coll_rep['current_week'] = np.where((billing_coll_rep['bank_dept_calendar_week_id']==current_week_id),1,0)
    

    # --------- Carga de resultados a la tabla--------
    print("⬆️ Cargando información a tabla...")
    f.update_table(
        secret=secret,
        table_name="public.report_billing_coll",
        df=billing_coll_rep,
        db_name=gp.DB_NAME_DM,
    )

    f.update_table(
        secret=secret,
        table_name="public.report_sheet_coll",
        df=sheet_coll_rep,
        db_name=gp.DB_NAME_DM,
    )

    # Tiempo de ejecución
    elapsed = time.time() - start_time
    minutes, seconds = divmod(int(elapsed), 60)

    mensaje_time = (
        f"✅ Test Billing Collections Step 4 Finalizado. "
        f"Tiempo de ejecución: {minutes} minutos y {seconds} segundos"
    )
    asunto_2 = "OK "
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
                f"❌ Error en Billing Collections Step 4\n{str(e)}\n\n"
                f"Traceback:\n{traceback.format_exc()}"
            )
            f.enviar_correo_sns("ERROR Billing Collections Step 4 ", mensaje_error)
        except Exception:
            print("⚠️ No se pudo enviar notificación de error.")

        # Re-lanzar para que Glue marque el Job como FAILED
        raise
