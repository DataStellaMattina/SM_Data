--job name 
ing_step1_documents_dev / ing_step1_documents_prod

#----> arguments

--Python library path
s3://04-sm-cod-dev/ext_files/etl_documents/functions.py,s3://04-sm-cod-dev/ext_files/etl_documents/params.py

#--AWS_REGION   us-east-2
#--DB_SECRET_NAME   (consultar en secret manager)