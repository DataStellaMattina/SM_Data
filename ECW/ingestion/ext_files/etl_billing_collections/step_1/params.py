
class GLOBLAL_PARAMS:
    def __init__(self):

        #PRO
        self.s3_bucket = '01-sm-data-pro' #pro
        self.s3_input_prefix = 'staging/ecw/ext_files/etl_billling_collections/input_files'
        self.s3_archive_prefix = 'staging/ecw/ext_files/etl_billling_collections/history_files'
        
        #DEV
        """
        self.s3_bucket = '02-sm-data-dev' 
        self.s3_input_prefix = 'staging/ecw/ext_file/etl_billling_collections/input_files'
        self.s3_archive_prefix = 'staging/ecw/ext_file/etl_billling_collections/history_files'
        """
        #--- DB names
        #Raw
        self.DB_NAME_DEV = 'ecw_stellamattina_db_raw_dev'
        self.DB_NAME = 'ecw_stellamattina_db_raw'
        #DM
        self.DB_NAME_DM_PROD = 'stellamattina_data_mart_prod'

        #----> arguments
        #--extra-py-files   s3://03-sm-cod-prod/ext_files/etl_bc/functions.py,s3://03-sm-cod-prod/ext_files/etl_bc/params.py
        #--AWS_REGION   us-east-2
        #--DB_SECRET_NAME   (consultar en secret manager)



