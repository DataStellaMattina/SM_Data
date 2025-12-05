# -*- coding: utf-8 -*-
import params as p
import functions as f
import pandas as pd
import numpy as np
import sys, time, json, traceback, os
import io
import boto3, botocore
import pg8000
from awsglue.utils import getResolvedOptions

s3 = boto3.client("s3")

def build_collections_galloway():
    sheet_name = '0383 GALLOWAY - INCOMINGS'
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
    print("Enviaroment: " + enviaroment + " Galloway")
    gp = p.GLOBLAL_PARAMS(enviaroment)

    # -------- Conexiones a BD --------
    conn_raw = None
    conn_dm = None

    try:
        print("🔌 Abriendo conexiones a PostgreSQL...")
        conn_raw = f.connect_to_postgres(secret, gp.DB_NAME)
        conn_dm = f.connect_to_postgres(secret, gp.DB_NAME_DM)

        print("📥 Leyendo parámetros y catálogos desde BD...")
        galloway_sheet = f.get_collections(sheet_name,conn_raw)
        galloway_sheet_w_pmt = f.get_collections_det(sheet_name,conn_raw)

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
    galloway_sheet_w_pmt['posted_db'] = np.where(galloway_sheet_w_pmt['posted_payment'].isnull(),0,1)
    galloway_sheet_w_pmt['posted_nodb'] = np.where(galloway_sheet_w_pmt['payment_id'].isin([3,4]),1,0)
    
    ####### DISTRIBUTED, UNPOSTED, POSTED NODB
    galloway_distributed = galloway_sheet_w_pmt[galloway_sheet_w_pmt['posted_db']==1].reset_index().drop(columns=['index'])
    galloway_unposted = galloway_sheet_w_pmt[(galloway_sheet_w_pmt['posted_db']==0) & (galloway_sheet_w_pmt['posted_nodb']==0)].reset_index().drop(columns=['index'])
    galloway_posted_nodb = galloway_sheet_w_pmt[(galloway_sheet_w_pmt['posted_db']==0) & (galloway_sheet_w_pmt['posted_nodb']==1)].reset_index().drop(columns=['index'])
    
    ####### DISTRIBUTED
    galloway_distributed['dist_payment'] = galloway_distributed['posted_payment']
    galloway_distributed['dist_facility_group_id'] = 5
    
    galloway_distributed_dt_fac = galloway_distributed.groupby(by=['sheet_date','dist_facility_group_id'])[['dist_payment']].sum().reset_index()
    
    galloway_distributed_dt = galloway_distributed_dt_fac.groupby(by=['sheet_date'])[['dist_payment']].sum().reset_index()
    
    ##### amount
    galloway_amount_dt = galloway_sheet.groupby(by=['sheet_date'])[['amount']].sum().reset_index()
    
    ############### ALLOCATED
    galloway_allocated_dt = pd.merge(galloway_amount_dt, galloway_distributed_dt,on=['sheet_date'],how='outer')
    galloway_allocated_dt = galloway_allocated_dt.fillna(0)
    galloway_allocated_dt['allocated_pmt'] = galloway_allocated_dt['amount'] - galloway_allocated_dt['dist_payment']
    
    galloway_allocated_dt_fac = galloway_allocated_dt.copy()
    galloway_allocated_dt_fac['dist_facility_group_id'] = 5
    
    ############ FACILITY COLLECTIONS
    facility_collections = pd.merge(galloway_allocated_dt_fac[['sheet_date','allocated_pmt','dist_facility_group_id']],galloway_distributed_dt_fac,how='outer',on=['sheet_date','dist_facility_group_id'])
    facility_collections = facility_collections.fillna(0)
    facility_collections['amount'] = facility_collections['allocated_pmt'] + facility_collections ['dist_payment']
    
    #sheet_amount_dt = wsc_amount_dt.copy()
    #sheet_dist_dt = wsc_distributed.groupby(by=['sheet_date'])[['dist_payment']].sum().reset_index()
    #sheet_unposted = wsc_unposted.groupby(by=['sheet_date'])[['amount']].sum().reset_index().rename(columns={'amount':'unposted_pmt'})
    #sheet_posted_nodb = wsc_posted_nodb.groupby(by=['sheet_date'])[['amount']].sum().reset_index().rename(columns={'amount':'posted_nodb_pmt'})
    
    ############# SHEET COLLECTIONS
    sheet_amount_dt = galloway_amount_dt.copy()
    sheet_amount_dt['type'] = 'amount'
    sheet_dist_dt = galloway_distributed.groupby(by=['sheet_date'])[['dist_payment']].sum().reset_index().rename(columns={'dist_payment':'amount'})
    sheet_dist_dt['type'] = 'dist_payment'
    
    galloway_unposted['type'] = np.where(galloway_unposted['payment_id']==1,'unposted_workload','unposted_lag')
    sheet_unposted = galloway_unposted.groupby(by=['sheet_date','type'])[['amount']].sum().reset_index()
    
    sheet_posted_nodb = galloway_posted_nodb.groupby(by=['sheet_date'])[['amount']].sum().reset_index()
    sheet_posted_nodb['type'] = 'posted_nodb_pmt'
    
    sheet_collections_long = pd.concat([sheet_amount_dt,
                                   sheet_dist_dt,
                                   sheet_unposted,
                                   sheet_posted_nodb]).reset_index().drop(columns=['index'])
    
    sheet_collections_wide = sheet_collections_long.pivot(index="sheet_date", columns="type", values="amount").reset_index()
    sheet_collections_wide = sheet_collections_wide.fillna(0)
    sheet_collections_wide['allocated_pmt'] = sheet_collections_wide['amount'] - sheet_collections_wide['dist_payment']
    sheet_collections_wide['posted_pmt'] = sheet_collections_wide['dist_payment'] + sheet_collections_wide['posted_nodb_pmt']
    sheet_collections_wide['unposted_pmt'] = sheet_collections_wide['unposted_workload'] + sheet_collections_wide['unposted_lag']

    facility_collections['sheet_name'] = sheet_name
    sheet_collections_wide['sheet_name'] = sheet_name

    return [facility_collections,sheet_collections_wide]