import sys, os

class GLOBLAL_PARAMS:
    def __init__(self, env: str):

        #self.env = os.getenv("ENV", "dev")  # dev o prod
        self.env = env.strip().lower()

        if self.env == "dev":
            self.s3_bucket = '02-sm-data-dev'
            self.s3_input_prefix_staging = 'staging/ecw/ext_file/etl_documents/input_files'
            self.s3_archive_prefix__staging = 'staging/ecw/ext_file/etl_documents/history_files'
            self.s3_input_prefix_raw = 'raw/ext/etl_documents/input_files/'
            self.s3_archive_prefix_raw = 'raw/ext/etl_documents/history_files/'            
            self.DB_NAME = 'ecw_stellamattina_db_raw_dev'
            self.DB_NAME_DM = 'stellamattina_data_mart_dev'
            # --- Lectura del insumo (TSV UTF-16) ---
            self.READ_OPTIONS = {
                "header": True,
                "delimiter": "\t",
                "encoding": "UTF-16",
            }

        else:
            self.s3_bucket = "01-sm-data-pro"
            self.s3_input_prefix_staging = 'staging/ecw/ext_files/etl_documents/input_files'
            self.s3_archive_prefix__staging = 'staging/ecw/ext_files/etl_documents/history_files'
            self.s3_input_prefix_raw = 'raw/ext/etl_documents/input_files/'
            self.s3_archive_prefix_raw = 'raw/ext/etl_documents/history_files/' 
            self.DB_NAME = 'ecw_stellamattina_db_raw'
            self.DB_NAME_DM = 'stellamattina_data_mart_prod'
             # --- Lectura del insumo (TSV UTF-16) ---
            self.READ_OPTIONS = {
                "header": True,
                "delimiter": "\t",
                "encoding": "UTF-16",
            }

