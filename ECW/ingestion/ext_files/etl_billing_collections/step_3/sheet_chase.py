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

def build_collections_chase():
    sheet_name = 'CHASE - INCOMINGS'
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
        chase_sheet = f.get_collections(sheet_name,conn_raw)
        chase_sheet_w_pmt = f.get_collections_det(sheet_name,conn_raw)

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
    posted_idx = (~chase_sheet_w_pmt['posted_payment'].isnull()) |  (chase_sheet_w_pmt['merch_facility_group_id']>0)   
    chase_sheet_w_pmt['posted_db'] = np.where(posted_idx,1,0)
    chase_sheet_w_pmt['posted_nodb'] = np.where((chase_sheet_w_pmt['payment_id'].isin([3,4])) & (chase_sheet_w_pmt['posted_db']==0) ,1,0)
    
    ####### DISTRIBUTED, UNPOSTED, POSTED NODB
    chase_distributed = chase_sheet_w_pmt[chase_sheet_w_pmt['posted_db']==1].reset_index().drop(columns=['index'])
    chase_unposted = chase_sheet_w_pmt[(chase_sheet_w_pmt['posted_db']==0) & (chase_sheet_w_pmt['posted_nodb']==0)].reset_index().drop(columns=['index'])
    chase_posted_nodb = chase_sheet_w_pmt[(chase_sheet_w_pmt['posted_db']==0) & (chase_sheet_w_pmt['posted_nodb']==1)].reset_index().drop(columns=['index'])
    
    ####### DISTRIBUTE
    chase_distributed['merch_flag'] = np.where(chase_distributed['posted_payment'].isnull(),1,0)
    
    def add_dist_payment(row):
        if row['merch_flag']==0:
            return row['posted_payment']
        elif row['merch_flag']==1:
            return row['amount']
        
        pass
    
    def add_dist_fac(row):
        if row['merch_flag']==0:
            return row['posted_facility_group_id']
        elif row['merch_flag']==1:
            return row['merch_facility_group_id']
        
        pass
    
    chase_distributed['dist_payment'] = chase_distributed.apply(func=add_dist_payment,axis=1)
    chase_distributed['dist_facility_group_id'] = chase_distributed.apply(func=add_dist_fac,axis=1)
    
    chase_distributed_hos_idx = chase_distributed['posted_facility_group_id']==7
    chase_distributed_hos = chase_distributed[chase_distributed_hos_idx].reset_index().drop(columns=['index'])
    chase_distributed_no_hos = chase_distributed[~chase_distributed_hos_idx].reset_index().drop(columns=['index'])
    
    chase_distributed_hos_dt = chase_distributed_hos.groupby(by=['sheet_date'])[['dist_payment']].sum().reset_index()
    chase_distributed_hos_dt_bishop = chase_distributed_hos_dt.copy()
    chase_distributed_hos_dt_bishop['dist_payment'] = chase_distributed_hos_dt_bishop['dist_payment']*0.60
    chase_distributed_hos_dt_bishop['dist_facility_group_id'] = 1
    
    chase_distributed_hos_dt_samuell = chase_distributed_hos_dt.copy()
    chase_distributed_hos_dt_samuell['dist_payment'] = chase_distributed_hos_dt_samuell['dist_payment']*0.40
    chase_distributed_hos_dt_samuell['dist_facility_group_id'] = 2
    
    chase_distributed_hos_dt_fac = pd.concat([chase_distributed_hos_dt_bishop,
                                            chase_distributed_hos_dt_samuell]).reset_index().drop(columns=['index'])
    
    chase_distributed_no_hos_dt_fac = chase_distributed_no_hos.groupby(by=['sheet_date','dist_facility_group_id'])[['dist_payment']].sum().reset_index()
    chase_distributed_no_hos_dt_fac.rename(columns={'posted_payment':'dist_payment',
                                                  'posted_facility_group_id':'dist_facility_group_id'},inplace=True)
    
    chase_distributed_dt_fac = pd.concat([chase_distributed_hos_dt_fac,
                                        chase_distributed_no_hos_dt_fac])
    
    chase_distributed_dt_fac  = chase_distributed_dt_fac.groupby(by=['sheet_date','dist_facility_group_id'])[['dist_payment']].sum().reset_index()
    chase_distributed_dt = chase_distributed_dt_fac.groupby(by=['sheet_date'])[['dist_payment']].sum().reset_index()
    
    
    ############# POSTED NODB FACILITY
    chase_posted_nodb_dt = chase_posted_nodb.groupby(by=['sheet_date'])[['amount']].sum().reset_index()
    
    chase_posted_nodb_dt_bishop = chase_posted_nodb_dt.copy()
    chase_posted_nodb_dt_bishop['amount'] = chase_posted_nodb_dt_bishop['amount']*0.31
    chase_posted_nodb_dt_bishop['dist_facility_group_id'] = 1
    
    chase_posted_nodb_dt_samuell = chase_posted_nodb_dt.copy()
    chase_posted_nodb_dt_samuell['amount'] = chase_posted_nodb_dt_samuell['amount']*0.15
    chase_posted_nodb_dt_samuell['dist_facility_group_id'] = 2
    
    chase_posted_nodb_dt_merit = chase_posted_nodb_dt.copy()
    chase_posted_nodb_dt_merit['amount'] = chase_posted_nodb_dt_merit['amount']*0.30
    chase_posted_nodb_dt_merit['dist_facility_group_id'] = 4
    
    chase_posted_nodb_dt_galloway = chase_posted_nodb_dt.copy()
    chase_posted_nodb_dt_galloway['amount'] = chase_posted_nodb_dt_galloway['amount']*0.17
    chase_posted_nodb_dt_galloway['dist_facility_group_id'] = 5
    
    chase_posted_nodb_dt_arlington = chase_posted_nodb_dt.copy()
    chase_posted_nodb_dt_arlington['amount'] = chase_posted_nodb_dt_arlington['amount']*0.03
    chase_posted_nodb_dt_arlington['dist_facility_group_id'] = 9

    chase_posted_nodb_dt_safely  = chase_posted_nodb_dt.copy()
    chase_posted_nodb_dt_safely['amount'] = chase_posted_nodb_dt_safely['amount']*0.04
    chase_posted_nodb_dt_safely['dist_facility_group_id'] = 13
    
    chase_posted_nodb_dt_fac = pd.concat([chase_posted_nodb_dt_bishop,
                                          chase_posted_nodb_dt_samuell,
                                          chase_posted_nodb_dt_merit,
                                          chase_posted_nodb_dt_galloway,
                                          chase_posted_nodb_dt_arlington,
                                          chase_posted_nodb_dt_safely])
    
    chase_posted_nodb_dt_fac = chase_posted_nodb_dt_fac.groupby(by=['sheet_date','dist_facility_group_id'])[['amount']].sum().reset_index()
    chase_posted_nodb_dt_fac.rename(columns={'amount':'posted_nodb_pmt'},inplace=True)
    
    ############### ALLOCATED
    chase_posted_nodb_dt_allo = chase_posted_nodb_dt.copy()
    chase_posted_nodb_dt_allo.rename(columns={'amount':'tot_posted_nodb_pmt'},inplace=True)
    
    chase_amount_dt = chase_sheet.groupby(by=['sheet_date'])[['amount']].sum().reset_index()
    
    chase_allocated_dt = pd.merge(chase_amount_dt, chase_distributed_dt,on=['sheet_date'],how='outer')
    chase_allocated_dt = chase_allocated_dt.fillna(0)
    chase_allocated_dt['allocated_pmt'] = chase_allocated_dt['amount'] - chase_allocated_dt['dist_payment']
    
    chase_allocated_dt_allo = chase_allocated_dt.copy()
    chase_allocated_dt_allo.rename(columns={'allocated_pmt':'tot_allocated_pmt'},inplace=True)
    
    chase_posted_nodb_dt_fac = pd.merge(chase_posted_nodb_dt_fac,chase_posted_nodb_dt_allo,on=['sheet_date'],how='left')
    chase_posted_nodb_dt_fac = pd.merge(chase_posted_nodb_dt_fac,chase_allocated_dt_allo,on=['sheet_date'],how='outer')
    #chase_posted_nodb_dt_fac = pd.merge(chase_posted_nodb_dt_fac,chase_allocated_dt_allo,on=['sheet_date'],how='left')
    null_dates = chase_posted_nodb_dt_fac[chase_posted_nodb_dt_fac['dist_facility_group_id'].isnull()]['sheet_date'].values

    ##########################    dates without posted nodb
    fac_dist = pd.DataFrame({'dist_facility_group_id':[1,2,4,5,9,13],
                            'per':[0.43,0.21,0.08,0.19,0.09,0]})
    
    if null_dates.shape[0]>0:
        null_fac_list =[]
        for date in null_dates:
            null_fac_df = fac_dist[['dist_facility_group_id']].copy()
            null_fac_df['sheet_date'] = date
            null_fac_list.append(null_fac_df)
            
        null_fac_dfs = pd.concat(null_fac_list).reset_index().drop(columns=['index'])
        null_fac_dfs.rename(columns={'dist_facility_group_id':'null_fac'},inplace=True)
    
        chase_posted_nodb_dt_fac = pd.merge(chase_posted_nodb_dt_fac,null_fac_dfs,on=['sheet_date'],how='left')
        chase_posted_nodb_dt_fac['dist_facility_group_id'] = np.where(chase_posted_nodb_dt_fac['null_fac'].isnull(),chase_posted_nodb_dt_fac['dist_facility_group_id'],chase_posted_nodb_dt_fac['null_fac'])
        chase_posted_nodb_dt_fac['dist_facility_group_id'] = chase_posted_nodb_dt_fac['dist_facility_group_id'].astype(int)
        chase_posted_nodb_dt_fac = chase_posted_nodb_dt_fac.fillna(0)

    ###############################
    
    
    chase_posted_nodb_dt_fac = pd.merge(chase_posted_nodb_dt_fac,fac_dist,on=['dist_facility_group_id'],how='left')
    chase_posted_nodb_dt_fac['allocated_pmt'] = ((chase_posted_nodb_dt_fac['tot_allocated_pmt'] - chase_posted_nodb_dt_fac['tot_posted_nodb_pmt'])*chase_posted_nodb_dt_fac['per']) + chase_posted_nodb_dt_fac['posted_nodb_pmt']       
    
    chase_allocated_dt_fac = chase_posted_nodb_dt_fac[['sheet_date','allocated_pmt','dist_facility_group_id']]
    
    ############ FACILITY COLLECTIONS
    facility_collections = pd.merge(chase_allocated_dt_fac[['sheet_date','allocated_pmt','dist_facility_group_id']],chase_distributed_dt_fac,how='outer',on=['sheet_date','dist_facility_group_id'])
    facility_collections = facility_collections.fillna(0)
    facility_collections['amount'] = facility_collections['allocated_pmt'] + facility_collections ['dist_payment']
    
    #sheet_amount_dt = wsc_amount_dt.copy()
    #sheet_dist_dt = wsc_distributed.groupby(by=['sheet_date'])[['dist_payment']].sum().reset_index()
    #sheet_unposted = wsc_unposted.groupby(by=['sheet_date'])[['amount']].sum().reset_index().rename(columns={'amount':'unposted_pmt'})
    #sheet_posted_nodb = wsc_posted_nodb.groupby(by=['sheet_date'])[['amount']].sum().reset_index().rename(columns={'amount':'posted_nodb_pmt'})
    
    ############# SHEET COLLECTIONS
    sheet_amount_dt = chase_amount_dt.copy()
    sheet_amount_dt['type'] = 'amount'
    sheet_dist_dt = chase_distributed.groupby(by=['sheet_date'])[['dist_payment']].sum().reset_index().rename(columns={'dist_payment':'amount'})
    sheet_dist_dt['type'] = 'dist_payment'
    
    chase_unposted['type'] = np.where(chase_unposted['payment_id']==1,'unposted_workload','unposted_lag')
    sheet_unposted = chase_unposted.groupby(by=['sheet_date','type'])[['amount']].sum().reset_index()
    
    sheet_posted_nodb = chase_posted_nodb.groupby(by=['sheet_date'])[['amount']].sum().reset_index()
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