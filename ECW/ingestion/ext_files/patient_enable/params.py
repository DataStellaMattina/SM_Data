# params.py
from pyspark.sql import types as T


# --- Lectura del insumo (TSV UTF-16) ---
READ_OPTIONS = {
    "header": True,
    "delimiter": "\t",
    "encoding": "UTF-16",
}

DATE_FORMAT = "yyyy-MM-dd"  # ajusta si tu CSV trae otro formato

# --- Mapeo de renombrado (origen -> destino) ---
RENAME_MAP = {
    "Patient ID": "patient_id",
    "Patient Name": "patient_name",
    "Patient First Name": "patient_first_name",
    "Patient Last Name": "patient_last_name",
    "Patient Cell Phone": "patient_cell_phone",
    "Patient Home Phone": "patient_home_phone",
    "Patient Work Phone": "patient_work_phone",
    "Patient DOB": "patient_dob",
    "Patient Age": "patient_age",
    "Patient E-mail": "patient_e_mail",
    "Patient Registration Date": "patient_registration_date",
    "Patient Registry Enabled": "patient_registry_enabled",
    "Patient Communication Notes": "patient_communication_notes",
    "Patient Enable Letters": "patient_enable_letters",
    "Patient Imm Registry Notification": "patient_imm_registry_notification",
    "Patient opts out of all practice communication flag": "patient_opts_out_of_all_practice_communication_flag",
    "Patient Text Contact Type": "patient_text_contact_type",
    "Patient Text Enabled": "patient_text_enabled",
    "Patient Text Enabled Language": "patient_text_enabled_language",
    "Patient Voice Enabled": "patient_voice_enabled",
    "Patient Voice Enabled Contact Type": "patient_voice_enabled_contact_type",
    "Patient Voice Enabled Language": "patient_voice_enabled_language",
    # Tus headers peculiares:
    "Patient Voice Enabled Time22": "patient_voice_enabled_time",
    "Patient Voice Enabled Time23": "patient_voice_enabled_time_1",
    "Patient Web Enabled": "patient_web_enabled",
}

# --- Esquema destino (tipos Spark que mapean a Postgres) ---
SCHEMA_MAP = {
    "patient_id": T.LongType(),                 # BIGINT
    "patient_name": T.StringType(),             # TEXT
    "patient_first_name": T.StringType(),
    "patient_last_name": T.StringType(),
    "patient_cell_phone": T.StringType(),
    "patient_home_phone": T.StringType(),
    "patient_work_phone": T.StringType(),
    "patient_dob": T.DateType(),                # DATE
    "patient_age": T.IntegerType(),             # INT
    "patient_e_mail": T.StringType(),
    "patient_registration_date": T.DateType(),  # DATE
    "patient_registry_enabled": T.StringType(),
    "patient_communication_notes": T.StringType(),
    "patient_enable_letters": T.StringType(),
    "patient_imm_registry_notification": T.DecimalType(18, 2),  # NUMERIC
    "patient_opts_out_of_all_practice_communication_flag": T.StringType(),
    "patient_text_contact_type": T.StringType(),
    "patient_text_enabled": T.StringType(),
    "patient_text_enabled_language": T.StringType(),
    "patient_voice_enabled": T.StringType(),
    "patient_voice_enabled_contact_type": T.StringType(),
    "patient_voice_enabled_language": T.StringType(),
    "patient_voice_enabled_time": T.StringType(),
    "patient_voice_enabled_time_1": T.StringType(),
    "patient_web_enabled": T.StringType(),
}

# Columnas de texto a limpiar (BOM/zero-width/etc.)
TEXT_COLS = [
    "patient_name","patient_first_name","patient_last_name",
    "patient_cell_phone","patient_home_phone","patient_work_phone",
    "patient_e_mail","patient_registry_enabled","patient_communication_notes",
    "patient_enable_letters","patient_text_contact_type","patient_text_enabled",
    "patient_text_enabled_language","patient_voice_enabled",
    "patient_voice_enabled_contact_type","patient_voice_enabled_language",
    "patient_voice_enabled_time","patient_voice_enabled_time_1","patient_web_enabled",
]

# Columnas Yes/No a normalizar (opcional)
YN_COLS = ["patient_web_enabled","patient_text_enabled","patient_voice_enabled"]
