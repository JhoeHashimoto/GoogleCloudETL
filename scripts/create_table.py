# BIBLIOTECAS
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import secretmanager
import logging
import numpy as np


tipo_carga                     = 'full'
projeto_localizacao            = 'us-east4'
projeto_regiao                 = 'us-east4'
dataflow_driver                = 'sqljdbc4-3.0.jar'
dataflow_driver_class          = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
airflow_max_task               = 5
network                        = 'default'
subnetwork                     = 'default'
dataflow_num_workers           = 1
dataflow_max_workers           = 2
dataflow_type_machine          = 'n1-standard-1'
manutencao_query               = 'N'
periodicidade_minutos          = 1440
manutencao                     = 'N'
bigquery_svc                   = 'service-account-estagiarios@roxpartner-estagiarios-infra.iam.gserviceaccount.com'
project_id                     = "roxpartner-estagiarios-infra"
projeto                        = "roxpartner-estagiarios-infra"
project_num                    = '122083571465'


def connectionGoogleBigQuery():

   
    key_path = r"C:\Users\gustavo.raposo\Desktop\rox projetos\entregavel- engenharia de dados\scripts\roxpartner-estagiarios-infra-b4a1bebdd8d5.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client_bq = bigquery.Client(credentials=credentials, project=credentials.project_id)

    return client_bq


def secret_manager(secret_resource_id):

    key_path = r"C:\Users\gustavo.raposo\Desktop\rox projetos\entregavel- engenharia de dados\scripts\roxpartner-estagiarios-infra-b4a1bebdd8d5.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    try:
        client = secretmanager.SecretManagerServiceClient(credentials=credentials)
        response = client.access_secret_version(name=secret_resource_id)
        secret_value = response.payload.data.decode("UTF-8")
        return secret_value
    except Exception as e:
        logging.error(f"Failed to access secret: {secret_resource_id}, due to: {e}")
        raise e


def connectionMySQL():

    MYSQL_DATABASE_USER = secret_manager(f"projects/{project_num}/secrets/controle_username/versions/latest")
    MYSQL_DATABASE_PASSWORD = secret_manager(f"projects/{project_num}/secrets/controle_password/versions/latest")
    MYSQL_DATABASE_HOST = secret_manager(f"projects/{project_num}/secrets/controle_serverid/versions/latest")
    MYSQL_DATABASE_NAME = secret_manager(f"projects/{project_num}/secrets/controle_db/versions/latest")

    engine_mysql = create_engine(
        f'mysql+pymysql://{MYSQL_DATABASE_USER}:{MYSQL_DATABASE_PASSWORD}@{MYSQL_DATABASE_HOST}/{MYSQL_DATABASE_NAME}'
    )

    Session_mysql = sessionmaker(bind=engine_mysql)
    session_mysql = Session_mysql()

    return session_mysql


conexaoMySQL          = connectionMySQL()
conexaoGoogleBigQuery = connectionGoogleBigQuery()

tabela_controle_mysql = conexaoMySQL.execute(text("SELECT codigo, tabela_origem, sistema_origem, tabela_origem_pk FROM controle WHERE status = 'ativo' and bigquery_create_table = 'S' "))
rows_tabela_controle_mysql = tabela_controle_mysql.fetchall()

for tabela_origem in rows_tabela_controle_mysql:
    tabela_origem_pks = ""
    array_tabela_controle      = np.array(tabela_origem)
    codigo_tabela              = array_tabela_controle[0]
    tabela_origem              = array_tabela_controle[1]
    sistema_origem             = array_tabela_controle[2]
    lista_tabela_origem_pks    = array_tabela_controle[3]
    lista_tabela_origem_pks = lista_tabela_origem_pks.split(',')
    codigo_tabela = int(codigo_tabela)
    extensao = tabela_origem.split('.')[-1]

    tabela_colunas_origem = ""
    sql_query = text(" select column_name, ordinal_position, data_type, coalesce(character_maximum_length,0) as character_maximum_length, coalesce(numeric_precision,0) as numeric_precision, coalesce(numeric_scale,0) as numeric_scale, coalesce(datetime_precision,0) as datetime_precision from table_mapping where codigo_tabela = :codigo_tabela;")
    information_schema_column_mapping_mssql = conexaoMySQL.execute(sql_query, {"codigo_tabela": codigo_tabela}  )
    rows_information_schema_column_mapping_mssql = information_schema_column_mapping_mssql.fetchall()
    for array_mapping in rows_information_schema_column_mapping_mssql:
            array_mapping = np.array(array_mapping)
            column_name              = array_mapping[0]
            ordinal_position         = array_mapping[1]
            data_type                = array_mapping[2]
            character_maximum_length = array_mapping[3]
            numeric_precision        = array_mapping[4]
            numeric_scale            = array_mapping[5]
            datetime_precision       = array_mapping[6]

            tabela_colunas_origem = tabela_colunas_origem + column_name + "," 


    tabela_colunas_origem = tabela_colunas_origem[:-1]
    tabela_colunas_origem = tabela_colunas_origem.lower()
    bigquery_dataset_tabela = sistema_origem.lower() + "_" + tabela_origem.lower().replace('.', '_')
    bigquery_dataset_land = "LAND_" + sistema_origem.upper()
    bigquery_dataset_raw = "RAW_" + sistema_origem.upper()
    bigquery_dataset_trusted = "TRUSTED_" + sistema_origem.upper()

    #Caso a tabela tenha mais de uma PK
    for pk in lista_tabela_origem_pks:
        tabela_origem_pks = tabela_origem_pks + pk + ","
    tabela_origem_pks = tabela_origem_pks[:-1]

    create_table_land = "CREATE TABLE " + project_id + '.' + bigquery_dataset_land + "." + bigquery_dataset_tabela + "( sys_change_version bigint, sys_change_operation string, sys_change_creation_version  bigint, commit_time timestamp,"
    create_table_raw = "CREATE TABLE " + project_id + '.' + bigquery_dataset_raw + "." + bigquery_dataset_tabela + "( sys_change_version bigint, sys_change_operation string, sys_change_creation_version  bigint, commit_time timestamp,"
    create_table_trusted = "CREATE TABLE " + project_id + '.' + bigquery_dataset_trusted + "." + bigquery_dataset_tabela + "("

    sql_query = text(" SELECT tm.column_name_datalake, tm.data_type, td.datatype_destino FROM controle_etl.table_mapping tm, transform_datatype td  WHERE tm.codigo_tabela= :codigo_tabela AND lower(tm.data_type) = lower(td.datatype_origem) order by tm.ordinal_position " )
    mapping_mysql = conexaoMySQL.execute(sql_query, {"codigo_tabela": codigo_tabela})
    rows_mapping_mysql = mapping_mysql.fetchall()

    for array_mapping_bq in rows_mapping_mysql:
        array_columns_bq = np.array(array_mapping_bq)
        column_name_datalake       = array_columns_bq[0].lower()
        data_type_origem  = array_columns_bq[1].lower()
        data_type_destino = array_columns_bq[2].lower()
        create_table_land    = create_table_land + ' ' + column_name_datalake + ' string,'
        create_table_raw     = create_table_raw + ' ' + column_name_datalake + ' string,'
        create_table_trusted = create_table_trusted + ' ' + column_name_datalake + ' ' + data_type_destino + ','

    create_table_land     = create_table_land + ' partition_time TIMESTAMP) PARTITION BY DATE_TRUNC(partition_time, MONTH) CLUSTER BY ' + tabela_origem_pks.lower() + ";"
    create_table_raw      = create_table_raw + ' partition_time TIMESTAMP) PARTITION BY DATE_TRUNC(partition_time, MONTH) CLUSTER BY ' + tabela_origem_pks.lower() + ";"
    create_table_trusted  = create_table_trusted + ' partition_time TIMESTAMP) PARTITION BY DATE_TRUNC(partition_time, MONTH) CLUSTER BY ' + tabela_origem_pks.lower() + ";"


    print(tabela_origem_pks)
    try:
        logging.info(f'Creating table {bigquery_dataset_land}')
        query_job = conexaoGoogleBigQuery.query(create_table_land)
        query_job.result()
        logging.info(f'Sucessfully create table {bigquery_dataset_land}')
    except Exception as create_error:
         logging.error(f'An error has ocurred during CREATE TABLE process in table {bigquery_dataset_land}')
         raise create_error

    try:
        logging.info(f'Creating table {bigquery_dataset_raw}')
        query_job = conexaoGoogleBigQuery.query(create_table_raw)
        query_job.result()
        logging.info(f'Sucessfully create table {bigquery_dataset_raw}')
    except Exception as create_error:
         logging.error(f'An error has ocurred during CREATE TABLE process in table {bigquery_dataset_raw}')
         raise create_error
    
    try:
        logging.info(f'Creating table {bigquery_dataset_trusted}')
        query_job = conexaoGoogleBigQuery.query(create_table_trusted)
        query_job.result()
        logging.info(f'Sucessfully create table {bigquery_dataset_land}')
    except Exception as create_error:
         logging.error(f'An error has ocurred during CREATE TABLE process in table {bigquery_dataset_trusted}')
         raise create_error    
  
    bucket_land  = "roxpartner-estagiarios-infra/land/" + sistema_origem.lower() + "/" + bigquery_dataset_tabela
    bucket_raw = "roxpartner-estagiarios-infra/raw/" + sistema_origem.lower() + "/" + bigquery_dataset_tabela

    try:
        logging.info("Updating table controle_etl.controle")
        update_query = text("UPDATE controle_etl.controle SET dt_carga_ini=now(), dt_carga_fim=now(), bigquery_dataset_tabela=:bigquery_dataset_tabela, bucket_land=:bucket_land, bucket_raw=:bucket_raw, bigquery_dataset_land=:bigquery_dataset_land, bigquery_dataset_raw=:bigquery_dataset_raw, bigquery_dataset_trusted=:bigquery_dataset_trusted , bigquery_create_table='N', tabela_colunas_origem=:tabela_colunas_origem, tabela_origem_pk=:tabela_origem_pk, bigquery_clustered=:tabela_origem_pk, tipo_carga=:tipo_carga, projeto_localizacao=:projeto_localizacao, projeto_regiao=:projeto_regiao, dataflow_driver=:dataflow_driver, dataflow_driver_class=:dataflow_driver_class, airflow_max_task=:airflow_max_task, network=:network, subnetwork=:subnetwork, dataflow_num_workers=:dataflow_num_workers, dataflow_max_workers=:dataflow_max_workers, dataflow_type_machine=:dataflow_type_machine, manutencao_query=:manutencao_query, periodicidade_minutos=:periodicidade_minutos, manutencao=:manutencao, bigquery_svc=:bigquery_svc, extensao=:extensao, projeto=:projeto WHERE codigo=:codigo_tabela ")
        update_data = {'codigo_tabela': codigo_tabela, 'bigquery_dataset_tabela': bigquery_dataset_tabela, 'bucket_land': bucket_land, 'bucket_raw': bucket_raw, 'bigquery_dataset_land': bigquery_dataset_land, 'bigquery_dataset_raw': bigquery_dataset_raw, 'bigquery_dataset_trusted': bigquery_dataset_trusted, 'tabela_colunas_origem': tabela_colunas_origem ,'tabela_origem_pk': tabela_origem_pks, 'tipo_carga': tipo_carga, 'projeto_localizacao':projeto_localizacao, 'projeto_regiao':projeto_regiao, 'dataflow_driver':dataflow_driver, 'dataflow_driver_class':dataflow_driver_class,'airflow_max_task':airflow_max_task, 'network':network, 'subnetwork':subnetwork, 'dataflow_num_workers':dataflow_num_workers, 'dataflow_max_workers':dataflow_max_workers, 'dataflow_type_machine':dataflow_type_machine, 'manutencao_query':manutencao_query, 'periodicidade_minutos':periodicidade_minutos, 'manutencao':manutencao, 'bigquery_svc':bigquery_svc,'extensao':extensao, 'projeto':projeto }
        conexaoMySQL.execute(update_query, update_data)
        conexaoMySQL.commit()
        logging.info('Sucessfully updated table controle_etl.controle')
    except Exception as update_error:
        logging.error(f"An error has occured during UPDATE process in {bigquery_dataset_land}.{bigquery_dataset_tabela}. Error: {update_error}")
        raise update_error


conexaoGoogleBigQuery.close()
conexaoMySQL.close()