import sys, os

class GLOBLAL_PARAMS:
    def __init__(self, env: str):

        #self.env = os.getenv("ENV", "dev")  # dev o prod
        self.env = env.strip().lower()

        if self.env == "dev":
            self.s3_bucket = '02-sm-data-dev' 
            self.s3_input_prefix = 'staging/ecw/ext_file/etl_cpt_fees/input_files'
            self.s3_archive_prefix = 'staging/ecw/ext_file/etl_cpt_fees/history_files'
            self.DB_NAME = 'ecw_stellamattina_db_raw_dev'
            self.DB_NAME_DM = 'stellamattina_data_mart_dev'

        else:
            self.s3_bucket = '01-sm-data-pro' #pro
            self.s3_input_prefix = 'staging/ecw/ext_files/etl_cpt_fees/input_files'
            self.s3_archive_prefix = 'staging/ecw/ext_files/etl_cpt_fees/history_files'
            self.DB_NAME = 'ecw_stellamattina_db_raw'
            self.DB_NAME_DM = 'stellamattina_data_mart_prod'

        #----> arguments
        #--extra-py-files
        #--AWS_REGION   us-east-2
        #--DB_SECRET_NAME   (consultar en secret manager)
