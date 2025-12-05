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

def build_collections_cfw():
    sheet_name = '010 CFW'
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
        hw_sheet = f.get_collections(sheet_name,conn_raw)

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
    ####### DISTRIBUTED, UNPOSTED, POSTED NODB
    facility_collections = hw_sheet.groupby(by=['sheet_date'])[['amount']].sum().reset_index()
    facility_collections['dist_payment'] = facility_collections['amount']
    facility_collections['dist_facility_group_id']=15
    facility_collections['allocated_pmt']=0
    
    cols_fc = ['sheet_date', 
               'allocated_pmt',
               'dist_facility_group_id',
               'dist_payment', 
               'amount']
    
    facility_collections = facility_collections[cols_fc]
    
    sheet_collections_wide = facility_collections.drop(columns=['dist_facility_group_id'])
    #sheet_date
    #amount	
    #dist_payment	
    #posted_nodb_pmt	
    #unposted_lag
    #	unposted_workload	
    #    allocated_pmt#
    #	posted_pmt
    #	unposted_pmt
    
    sheet_collections_wide['posted_nodb_pmt'] = 0
    sheet_collections_wide['unposted_lag'] = 0
    sheet_collections_wide['unposted_workload'] = 0
    sheet_collections_wide['allocated_pmt'] = 0
    sheet_collections_wide['posted_pmt'] = sheet_collections_wide['amount']
    sheet_collections_wide['unposted_pmt'] = 0
    
    cols_sp = ['sheet_date', 
               'amount', 
               'dist_payment',
               'posted_nodb_pmt', 
               'unposted_lag',
               'unposted_workload',
               'allocated_pmt',
               'posted_pmt',
               'unposted_pmt']
    
    sheet_collections_wide = sheet_collections_wide[cols_sp ]
    
    facility_collections['sheet_name'] = sheet_name
    sheet_collections_wide['sheet_name'] = sheet_name
    
    return [facility_collections,sheet_collections_wide]