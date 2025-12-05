# params_glue.py
from typing import Dict, Any

# ====== S3 (prod o dev vía args) ======
# Se pasan por argumentos del Job:
#   --s3_bucket  --s3_input_prefix  --s3_history_prefix

# ====== JDBC (RAW / DM) ======
# Se pasan por args o vía Secrets Manager (recomendado):
#   --jdbc_host_raw  --jdbc_port  --jdbc_db_raw  --jdbc_user  --jdbc_password
#   --jdbc_host_dm   --jdbc_db_dm
JDBC_OPTIONS_BASE = {
    "driver": "org.postgresql.Driver",
    "batchsize": "10000",
    "truncate": "true",   # importante para mode=overwrite
}

# Tablas de destino
TABLE_RAW = "raw_ebo_report.pwt"
TABLE_PWT_PAT = "public.report_pwt_pat"
TABLE_PWT_SUMMARY = "public.report_pwt_summary"

# Tablas auxiliares JDBC (lectura)
CALENDAR_DETAIL_SQL = """
SELECT date AS appointment_date,
       calendar_week_id AS appointment_date_calendar_week_id
FROM aux.calendar_detail;
"""

FACILITY_INFO_SQL = """
SELECT facility_group_id,
       facility_group_name,
       facility_name
FROM aux.facility_info
WHERE location_type='Clinic';
"""

# Columnas a leer del Excel (igual que tu Pandas)
XLSX_COLS = [
    "Appointment Facility Name",
    "Appointment Provider Name",
    "Resource Provider Name",
    "Appointment Date",
    "Appointment Start Time",
    "Patient Acct No",
    "Visit Type",
    "Log Time",
    "Status",
    "Time in Status",
]

# Renombres (1:1 con tu script)
RENAME_MAP = {
    "Appointment Facility Name": "facility_name",
    "Appointment Provider Name": "appointment_provider_name",
    "Resource Provider Name": "resource_provider_name",
    "Appointment Date": "appointment_date",
    "Appointment Start Time": "appointment_start_time",
    "Patient Acct No": "patient_id",
    "Visit Type": "visit_type",
    "Log Time": "log_time",
    "Status": "status",
    "Time in Status": "time_in_status",
}
