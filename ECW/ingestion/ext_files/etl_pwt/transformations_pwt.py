# transformations_pwt.py
from pyspark.sql import functions as F, types as T
from functions_glue import (
    compute_minutes_in_status, build_visit_ids, attach_visit_keys,
    compute_patient_time_and_pwt
)

def normalize_and_cast(df, rename_map: dict):
    # renombra
    for src, dst in rename_map.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)

    # tipos básicos
    # appointment_date → date
    if "appointment_date" in df.columns:
        df = df.withColumn("appointment_date", F.to_date(F.col("appointment_date")))
    # log_time → timestamp
    if "log_time" in df.columns:
        df = df.withColumn("log_time", F.to_timestamp(F.col("log_time")))
    # patient_id → bigint
    if "patient_id" in df.columns:
        df = df.withColumn("patient_id", F.col("patient_id").cast(T.LongType()))

    # minutes_in_status
    if "time_in_status" in df.columns:
        df = df.withColumn("minutes_in_status", compute_minutes_in_status("time_in_status"))

    # visit_type null → "notype"
    df = df.withColumn("visit_type",
                       F.when(F.col("visit_type").isNull(), F.lit("notype")).otherwise(F.col("visit_type")))

    return df

def make_raw_output(df):
    # Selección similar al Pandas para la RAW
    keep = [
        "facility_name","appointment_provider_name","resource_provider_name",
        "appointment_date","appointment_start_time","patient_id","visit_type",
        "log_time","status","time_in_status","minutes_in_status","source_file"
    ]
    return df.select([c for c in keep if c in df.columns])

def build_pwt_pat(df, facility_info_df, calendar_df):
    # join con facility_info
    j1 = df.join(facility_info_df, ["facility_name"], "inner")
    # id de visit_type y llaves
    j1 = build_visit_ids(j1)
    j1 = attach_visit_keys(j1)
    # calcular patient_time y pwt
    p = compute_patient_time_and_pwt(j1)
    # join calendar week
    p = p.join(calendar_df, ["appointment_date"], "left")

    # output final
    out = p.select(
        "patient_id",
        "start_time",
        "appointment_date",
        "facility_group_id",
        "facility_group_name",
        "visit_id",
        "visit_type",
        "end_time",
        "patient_time",
        "appointment_date_calendar_week_id",
        F.round(F.col("pwt").cast("double"), 4).alias("pwt")
    )
    return out

def build_pwt_summary(df, facility_info_df, calendar_df):
    grp = (df.groupBy(
        "facility_name",
        "resource_provider_name",
        "patient_id",
        "visit_type",
        "appointment_date",
        "status"
    ).agg(F.sum("minutes_in_status").alias("minutes_in_status")))

    j = grp.join(facility_info_df, ["facility_name"], "inner").join(calendar_df, ["appointment_date"], "left")
    return j
