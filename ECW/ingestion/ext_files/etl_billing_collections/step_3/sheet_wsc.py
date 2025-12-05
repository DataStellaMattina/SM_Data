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

def build_collections_wsc():
    sheet_name = 'WSC - INCOMINGS'
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
    print("Enviaroment: " + enviaroment + " WSC")
    gp = p.GLOBLAL_PARAMS(enviaroment)

    # -------- Conexiones a BD --------
    conn_raw = None
    conn_dm = None

    try:
        print("🔌 Abriendo conexiones a PostgreSQL...")
        conn_raw = f.connect_to_postgres(secret, gp.DB_NAME)
        conn_dm = f.connect_to_postgres(secret, gp.DB_NAME_DM)

        print("📥 Leyendo parámetros y catálogos desde BD...")
        wsc_sheet = f.get_collections(sheet_name,conn_raw)
        wsc_sheet_w_pmt = f.get_collections_det(sheet_name,conn_raw)

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
    #posted_idx = (~wsc_sheet_w_pmt['posted_payment'].isnull()) | (wsc_sheet_w_pmt['payment_id'].isin([3,4]))
    wsc_sheet_w_pmt['posted_db'] = np.where(wsc_sheet_w_pmt['posted_payment'].isnull(),0,1)
    wsc_sheet_w_pmt['posted_nodb'] = np.where(wsc_sheet_w_pmt['payment_id'].isin([3,4]),1,0)

    #wsc_sheet_w_pmt['posted_db'] = np.where(posted_idx,0,1)
    #wsc_sheet_w_pmt['posted_nodb'] = np.where(wsc_sheet_w_pmt['payment_id'].isin([3,4]),1,0)

    ####### DISTRIBUTED, UNPOSTED, POSTED NODB
    wsc_distributed = wsc_sheet_w_pmt[wsc_sheet_w_pmt['posted_db']==1].reset_index().drop(columns=['index'])
    wsc_unposted = wsc_sheet_w_pmt[(wsc_sheet_w_pmt['posted_db']==0) & (wsc_sheet_w_pmt['posted_nodb']==0)].reset_index().drop(columns=['index'])
    wsc_posted_nodb = wsc_sheet_w_pmt[(wsc_sheet_w_pmt['posted_db']==0) & (wsc_sheet_w_pmt['posted_nodb']==1)].reset_index().drop(columns=['index'])

    ####### DISTRIBUTED
    wsc_distributed['dist_payment'] = wsc_distributed['posted_payment']

    wsc_distributed_hos_idx = wsc_distributed['posted_facility_group_id']==7
    wsc_distributed_hos = wsc_distributed[wsc_distributed_hos_idx].reset_index().drop(columns=['index'])
    wsc_distributed_no_hos = wsc_distributed[~wsc_distributed_hos_idx].reset_index().drop(columns=['index'])

    wsc_distributed_hos_dt = wsc_distributed_hos.groupby(by=['sheet_date'])[['dist_payment']].sum().reset_index()
    wsc_distributed_hos_dt_bishop = wsc_distributed_hos_dt.copy()
    wsc_distributed_hos_dt_bishop['dist_payment'] = wsc_distributed_hos_dt_bishop['dist_payment']*0.35
    wsc_distributed_hos_dt_bishop['dist_facility_group_id'] = 1

    wsc_distributed_hos_dt_samuell = wsc_distributed_hos_dt.copy()
    wsc_distributed_hos_dt_samuell['dist_payment'] = wsc_distributed_hos_dt_samuell['dist_payment']*0.65
    wsc_distributed_hos_dt_samuell['dist_facility_group_id'] = 2

    wsc_distributed_hos_dt_fac = pd.concat([wsc_distributed_hos_dt_bishop,
                                            wsc_distributed_hos_dt_samuell]).reset_index().drop(columns=['index'])

    wsc_distributed_no_hos_dt_fac = wsc_distributed_no_hos.groupby(by=['sheet_date','posted_facility_group_id'])[['posted_payment']].sum().reset_index()
    wsc_distributed_no_hos_dt_fac.rename(columns={'posted_payment':'dist_payment',
                                                  'posted_facility_group_id':'dist_facility_group_id'},inplace=True)

    wsc_distributed_dt_fac = pd.concat([wsc_distributed_hos_dt_fac,
                                        wsc_distributed_no_hos_dt_fac])

    wsc_distributed_dt_fac  = wsc_distributed_dt_fac.groupby(by=['sheet_date','dist_facility_group_id'])[['dist_payment']].sum().reset_index()
    wsc_distributed_dt = wsc_distributed_dt_fac.groupby(by=['sheet_date'])[['dist_payment']].sum().reset_index()

    wsc_amount_dt = wsc_sheet.groupby(by=['sheet_date'])[['amount']].sum().reset_index()

    ############### ALLOCATED
    wsc_allocated_dt = pd.merge(wsc_amount_dt, wsc_distributed_dt,on=['sheet_date'],how='outer')
    wsc_allocated_dt = wsc_allocated_dt.fillna(0)
    wsc_allocated_dt['allocated_pmt'] = wsc_allocated_dt['amount'] - wsc_allocated_dt['dist_payment']

    wsc_allocated_dt_bishop = wsc_allocated_dt.copy()
    wsc_allocated_dt_bishop['allocated_pmt'] = wsc_allocated_dt_bishop['allocated_pmt']*0.8
    wsc_allocated_dt_bishop['dist_facility_group_id']=1

    wsc_allocated_dt_samuell =  wsc_allocated_dt.copy()
    wsc_allocated_dt_samuell['allocated_pmt'] = wsc_allocated_dt_samuell['allocated_pmt']*0.2
    wsc_allocated_dt_samuell['dist_facility_group_id']=2

    wsc_allocated_dt_fac = pd.concat([wsc_allocated_dt_bishop,wsc_allocated_dt_samuell]).reset_index().drop(columns=['index'])

    ############ FACILITY COLLECTIONS
    facility_collections = pd.merge(wsc_allocated_dt_fac[['sheet_date','allocated_pmt','dist_facility_group_id']],wsc_distributed_dt_fac,how='outer',on=['sheet_date','dist_facility_group_id'])
    facility_collections = facility_collections.fillna(0)
    facility_collections['amount'] = facility_collections['allocated_pmt'] + facility_collections ['dist_payment']

    #sheet_amount_dt = wsc_amount_dt.copy()
    #sheet_dist_dt = wsc_distributed.groupby(by=['sheet_date'])[['dist_payment']].sum().reset_index()
    #sheet_unposted = wsc_unposted.groupby(by=['sheet_date'])[['amount']].sum().reset_index().rename(columns={'amount':'unposted_pmt'})
    #sheet_posted_nodb = wsc_posted_nodb.groupby(by=['sheet_date'])[['amount']].sum().reset_index().rename(columns={'amount':'posted_nodb_pmt'})

    ############# SHEET COLLECTIONS
    sheet_amount_dt = wsc_amount_dt.copy()
    sheet_amount_dt['type'] = 'amount'
    sheet_dist_dt = wsc_distributed.groupby(by=['sheet_date'])[['dist_payment']].sum().reset_index().rename(columns={'dist_payment':'amount'})
    sheet_dist_dt['type'] = 'dist_payment'
    
    if sheet_dist_dt.shape[0]==0:
        sheet_dist_dt =  sheet_amount_dt.copy()
        sheet_dist_dt['amount'] = 0
        sheet_dist_dt['type'] = 'dist_payment'

    wsc_unposted['type'] = np.where(wsc_unposted['payment_id']==1,'unposted_workload','unposted_lag')
    sheet_unposted = wsc_unposted.groupby(by=['sheet_date','type'])[['amount']].sum().reset_index()
    sheet_unposted_workload = sheet_unposted [sheet_unposted ['type']=='unposted_workload']
    sheet_unposted_unposted_lag = sheet_unposted [sheet_unposted ['type']=='unposted_lag']
    
    if sheet_unposted_workload.shape[0]==0:
        sheet_unposted_workload =  sheet_amount_dt.copy()
        sheet_unposted_workload['amount'] = 0
        sheet_unposted_workload['type'] = 'unposted_workload'
    
    if sheet_unposted_unposted_lag.shape[0]==0:
        sheet_unposted_unposted_lag =  sheet_amount_dt.copy()
        sheet_unposted_unposted_lag['amount'] = 0
        sheet_unposted_unposted_lag['type'] = 'unposted_lag'
        

    sheet_posted_nodb = wsc_posted_nodb.groupby(by=['sheet_date'])[['amount']].sum().reset_index()
    sheet_posted_nodb['type'] = 'posted_nodb_pmt'
    
    if sheet_posted_nodb.shape[0]==0:
        sheet_posted_nodb =  sheet_amount_dt.copy()
        sheet_posted_nodb['amount'] = 0
        sheet_posted_nodb['type'] = 'posted_nodb_pmt'
        

    sheet_collections_long = pd.concat([sheet_amount_dt,
                                   sheet_dist_dt,
                                   sheet_unposted_workload ,
                                   sheet_unposted_unposted_lag,
                                   sheet_posted_nodb]).reset_index().drop(columns=['index'])

    sheet_collections_wide = sheet_collections_long.pivot(index="sheet_date", columns="type", values="amount").reset_index()
    sheet_collections_wide = sheet_collections_wide.fillna(0)
    
    columns=['sheet_date', 
             'amount', 
             'dist_payment',
             'posted_nodb_pmt',
             'unposted_lag', 
             'unposted_workload']
    
    sheet_collections_wide = sheet_collections_wide[columns]
    sheet_collections_wide['allocated_pmt'] = sheet_collections_wide['amount'] - sheet_collections_wide['dist_payment']
    sheet_collections_wide['posted_pmt'] = sheet_collections_wide['dist_payment'] + sheet_collections_wide['posted_nodb_pmt']
    sheet_collections_wide['unposted_pmt'] = sheet_collections_wide['unposted_workload'] + sheet_collections_wide['unposted_lag']
    
    facility_collections['sheet_name'] = sheet_name
    sheet_collections_wide['sheet_name'] = sheet_name
    
    return [facility_collections,sheet_collections_wide]
