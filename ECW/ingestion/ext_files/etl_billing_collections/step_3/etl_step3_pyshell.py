import sys, time, json, traceback, os
import io
import boto3, botocore
import pandas as pd
import numpy as np
import pg8000
from awsglue.utils import getResolvedOptions
import functions as f
import params as p
from sheet_wsc import build_collections_wsc
from sheet_merit import build_collections_merit
from sheet_galloway import build_collections_galloway
from sheet_chase import build_collections_chase
from sheet_mayfield import build_collections_mayfield
from sheet_MFM_Merit_JW import build_collections_merit_JW
from sheet_harwood import build_collections_harwood
from sheet_cfw_010 import build_collections_cfw
from sheet_Buckner_011 import build_collections_buckner
from sheet_forest_018 import build_collections_forest

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
        dates = f.get_dates_from_bill(conn_raw)
        facilities_from_coll = f.get_facilities_from_bill(conn_raw)

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
    print("Sheet wsc")
    wsc_facility_collections,wsc_sheet_collections_wide = build_collections_wsc()
    print("Sheet merit")
    merit_facility_collections,merit_sheet_collections_wide = build_collections_merit()
    print("Sheet galloway")
    galloway_facility_collections,galloway_sheet_collections_wide = build_collections_galloway()
    print("Sheet chase")
    chase_facility_collections,chase_sheet_collections_wide = build_collections_chase()
    print("Sheet mayfield")
    mayfield_facility_collections,mayfield_sheet_collections_wide = build_collections_mayfield()
    print("Sheet merit_JW")
    merit_facility_collections_JW,merit_sheet_collections_wide_JW = build_collections_merit_JW()
    print("Sheet harwood")
    hw_facility_collections,hw_sheet_collections_wide = build_collections_harwood()
    print("Sheet cfw_010")
    cfw_facility_collections,cfw_sheet_collections_wide = build_collections_cfw()
    print("Sheet Buckner_011")
    buckner_facility_collections,buckner_sheet_collections_wide = build_collections_buckner()
    print("Sheet forest 018")
    forest_facility_collections,forest_sheet_collections_wide = build_collections_forest()

    

    print("⚙️ concat collections ...")
    facility_collections = pd.concat([wsc_facility_collections,
                                      merit_facility_collections,
                                      galloway_facility_collections,
                                      chase_facility_collections,
                                      mayfield_facility_collections,
                                      merit_facility_collections_JW,
                                      hw_facility_collections,
                                      cfw_facility_collections,
                                      buckner_facility_collections,
                                      forest_facility_collections
                                    ]).reset_index().drop(columns=['index'])
    
    facility_collections['dist_facility_group_id'] = facility_collections['dist_facility_group_id'].astype(int)
    
    sheet_collections = pd.concat([wsc_sheet_collections_wide,
                                   merit_sheet_collections_wide,
                                   galloway_sheet_collections_wide,
                                   chase_sheet_collections_wide,
                                   mayfield_sheet_collections_wide,
                                   merit_sheet_collections_wide_JW,
                                   hw_sheet_collections_wide,
                                   cfw_sheet_collections_wide,
                                   buckner_sheet_collections_wide,
                                   forest_sheet_collections_wide
                                ]).reset_index().drop(columns=['index'])

    # --------- Carga de resultados a la tabla--------
    print("⬆️ Cargando información a las tablas...")

    f.update_table(
        secret=secret,
        table_name="raw_bank.bill_facility_coll",
        df=facility_collections,
        db_name=gp.DB_NAME,
    )

    f.update_table(
        secret=secret,
        table_name="raw_bank.bill_sheet_coll",
        df=sheet_collections,
        db_name=gp.DB_NAME,
    )

    dates_list = []
    for facility_id in list(facilities_from_coll['facility_group_id']):
        dates_fac = dates.copy()
        dates_fac['facility_group_id'] = facility_id
        dates_list.append(dates_fac)
    
    dates_base_bill = pd.concat(dates_list).reset_index().drop(columns=['index'])
    dates_base_bill = dates_base_bill[ ~(pd.to_datetime(dates_base_bill['date']).dt.weekday.isin([5,6]))]

    f.update_table(
        secret=secret,
        table_name="raw_bank.dates_base_bill",
        df=dates_base_bill,
        db_name=gp.DB_NAME,
    )

    # Tiempo de ejecución
    elapsed = time.time() - start_time
    minutes, seconds = divmod(int(elapsed), 60)

    mensaje_time = (
        f"✅ Test Billing Collections Step 3 Finalizado. "
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
                f"❌ Error en Billing Collections Step 3\n{str(e)}\n\n"
                f"Traceback:\n{traceback.format_exc()}"
            )
            f.enviar_correo_sns("ERROR Billing Collections Step 3", mensaje_error)
        except Exception:
            print("⚠️ No se pudo enviar notificación de error.")

        # Re-lanzar para que Glue marque el Job como FAILED
        raise
