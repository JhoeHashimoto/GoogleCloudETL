# Importação das bibliotecas
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
from google.auth.transport import requests as g_requests
from google.cloud import secretmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import logging
import pandas as pd
import time
import numpy as np
import json
import io
import os
import pendulum


from datetime import datetime, timedelta


project_num = '122083571465'
project_id = 'roxpartner-estagiarios-infra'
land_path_file = r"C:\Users\gustavo.raposo\Desktop\flatfiles\land"

#caminho do diretório local onde os arquivos em formato parquet serão baixados
raw_path_file = "/home/airflow/raw/flatfile/"


today = datetime.now()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")


source_bucket_name            = "roxpartner-estagiarios-infra"
source_folder_path            = "land/flatfile/"
dest_bucket_name              = "roxpartner-estagiarios-infra"
destination_path_processed    = f"raw/flatfile/processed/{year}/{month}/{day}/"
destination_path_original     = f"raw/flatfile/original/{year}/{month}/{day}/"

key_path = "/home/airflow/roxpartner-estagiarios-infra-b4a1bebdd8d5.json"


gcs_bucket_name = "roxpartner-estagiarios-infra"
gcs_client = storage.Client.from_service_account_json(json_credentials_path=key_path)


credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

def connectionGoogleBigQuery():

    client_bq = bigquery.Client(credentials=credentials, project=credentials.project_id)

    return client_bq


def secret_manager(secret_resource_id):
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

conexao = connectionMySQL()
print(conexao)


def insertMonitoring(operacao, tabela, codigo_tabela, status):
    conexaoMySQL = connectionMySQL()

    today = datetime.now()
    event = operacao + ' NA TABLE ' + tabela

    try:
        monitoring_query = text(" INSERT INTO controle_etl.monitoring_events (codigo_tabela, event, status, event_datetime) VALUES (:codigo_tabela, :event, :status, :today) ")
        monitoring_data = {'codigo_tabela': codigo_tabela, 'event': event, 'status': status, 'today': today}
        conexaoMySQL.execute(monitoring_query, monitoring_data)
        conexaoMySQL.commit()
    except Exception as monitoring_error:
        logging.error(f"Error logging monitoring event: {monitoring_error}")
        raise monitoring_error


def copyFileGCPBucket(bucket_name, source_blob_name, destination_blob_name):
    """Copy a blob to the bucket."""

    key_path = "/home/airflow/roxpartner-estagiarios-infra-b4a1bebdd8d5.json"

    storage_client = storage.Client.from_service_account_json(json_credentials_path=key_path)

    bucket = storage_client.bucket(bucket_name)

    source_blob = bucket.blob(source_blob_name)

    bucket.copy_blob(source_blob, bucket, destination_blob_name)
    return "OK"

def uploadFileGCPBucket(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    key_path = "/home/airflow/roxpartner-estagiarios-infra-b4a1bebdd8d5.json"

    storage_client = storage.Client.from_service_account_json(json_credentials_path=key_path)

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    return "OK"


def deleteFileGCPBucket(bucket_name, blob_name):
    key_path = "/home/airflow/roxpartner-estagiarios-infra-b4a1bebdd8d5.json"
    storage_client = storage.Client.from_service_account_json(json_credentials_path=key_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    return "ok"

def delete_files(path):

    try:
        if not os.path.isdir(path):
            print(f"O diretório {path} não existe")

        for arquivo in os.listdir(path):
            caminho_arquivo = os.path.join(path, arquivo)

            if os.path.isfile(caminho_arquivo):
                os.remove(caminho_arquivo)
                print(f'Arquivo {arquivo} apagado.')
    except Exception as delete_file_error:
        logging.error(f"An error occurred: {delete_file_error}")
        raise delete_file_error


def downloadFileGCPBucket(bucket_name, blob_name):
    key_path = "/home/airflow/roxpartner-estagiarios-infra-b4a1bebdd8d5.json"

    try:
        logging.info(f"Initializing storage client with key path: {key_path}")
        storage_client = storage.Client.from_service_account_json(json_credentials_path=key_path)

        logging.info(f"Getting bucket: {bucket_name}")
        bucket = storage_client.bucket(bucket_name)

        logging.info(f"Getting blob: {blob_name}")
        blob = bucket.blob(blob_name)

        if not blob.exists():
            return False

        csv_content = blob.download_as_bytes()

        return csv_content
    except Exception as download_error:
        logging.error(f"An error occurred: {download_error}")
        raise download_error

def getFileBucket(file_name):
    key_path = "/home/airflow/roxpartner-estagiarios-infra-b4a1bebdd8d5.json"
    storage_client = storage.Client.from_service_account_json(json_credentials_path=key_path)
    bucket = storage_client.bucket('roxpartner-estagiarios-infra')
    blob = bucket.blob(file_name)
    return blob
                

def csv_to_parquet(csv_file, parquet_file, header_row, start_row, rename_columns, dtypes, codigo_tabela):
    parquet_file = raw_path_file + parquet_file

    df = pd.read_csv(io.BytesIO(csv_file),  delimiter=';')

    df.head()

    #new_column_names = {
    #    'id colaborador': 'id_colaborador',
    #    'nome colaboador': 'nome_colaborador',
    #    # Add more mappings as needed
    #}
    new_column_names = rename_columns

    df = df.rename(columns=new_column_names)

    # Set data types
    #dtypes = {
    #    'id_colaborador': 'string',
    #    'nome_colaborador': 'string'
    #}
    df = df.astype(dtypes)

    df.to_parquet(parquet_file, engine='pyarrow')

    logging.info(f'Csv file {csv_file} successfully converted to Parquet file {parquet_file}.')

    loadBigQuery(codigo_tabela)


def loadBigQuery(codigo_tabela):

    conexaoMySQL = connectionMySQL()

    query = " SELECT bigquery_dataset_land, bigquery_dataset_raw, bigquery_dataset_trusted, bigquery_dataset_tabela, tabela_origem, projeto, extensao, tipo_carga, tabela_origem_pk, tabela_colunas_origem, tabela_origem FROM controle WHERE codigo=:codigo_tabela "
    tabela_controle_mysql = conexaoMySQL.execute(text(query), {"codigo_tabela": codigo_tabela})
    rows_tabela_controle_mysql = tabela_controle_mysql.fetchall()
    for dados_tabela_origem in rows_tabela_controle_mysql:
        array_tabela_controle              = np.array(dados_tabela_origem)
        bigquery_dataset_land              = array_tabela_controle[0]
        bigquery_dataset_raw               = array_tabela_controle[1]
        bigquery_dataset_trusted           = array_tabela_controle[2]
        bigquery_dataset_tabela            = array_tabela_controle[3]
        arquivo_origem                     = array_tabela_controle[4]
        projeto                            = array_tabela_controle[5]
        extensao                           = array_tabela_controle[6]
        tipo_carga                         = array_tabela_controle[7]
        tabela_origem_pk                   = array_tabela_controle[8]
        tabela_colunas_origem              = array_tabela_controle[9]
        tabela_origem                      = array_tabela_controle[10]
        parquet_file                       = arquivo_origem.replace(extensao, 'parquet')

    sql_colunas_raw = ""
    sql_colunas_trusted = ""
    sql_colunas_schema = ""
    column_schema_bq_list = ""
    list_colunas_schema = []

    query_mapping = text(" SELECT DISTINCT tm.data_type FROM controle_etl.table_mapping tm WHERE tm.codigo_tabela=:codigo_tabela ")
    tabela_types = conexaoMySQL.execute(query_mapping, {"codigo_tabela": codigo_tabela})
    rows_tabela_types_mysql = tabela_types.fetchall()

    array_tabela_types = []

    for dados_types in rows_tabela_types_mysql:
        array_tabela_types.append(np.array(dados_types))

    rows_tabela_schemas_mysql = []
    column_name_dtype = ""
    i = 0

    for data_type in array_tabela_types:
        if data_type == 'date':
            query = " SELECT CONCAT('CASE WHEN ', lower(tm.column_name_datalake), ' = '' '' OR ', lower(tm.column_name_datalake), ' = ''-'' THEN NULL ELSE CAST(SUBSTR(',lower(tm.column_name_datalake),', 0, 10) AS ' ,td.datatype_destino, ') END as ',lower(tm.column_name_datalake),',') as coluna_concat_cast,tm.column_name_datalake as  column_name_datalake, REPLACE(CONCAT('bigquery.SchemaField(#',tm.column_name_datalake,'#,#STRING#),'),'#','''') as column_schema_bq, tm.ordinal_position as ordinal_position  FROM controle_etl.table_mapping tm, transform_datatype td  WHERE tm.codigo_tabela=:codigo_tabela AND lower(tm.data_type) = 'date' AND lower(tm.data_type) = lower(td.datatype_origem) order by tm.ordinal_position "
            tabela_schemas = conexaoMySQL.execute(text(query), {"codigo_tabela": codigo_tabela})
            rows_tabela_schemas_date = tabela_schemas.fetchall()

            for dados_schemas in rows_tabela_schemas_date:
                array_tabela_dates  = np.array(dados_schemas)
                rows_tabela_schemas_mysql.append(array_tabela_dates)
                column_name_dtype = column_name_dtype + array_tabela_dates[1] + ','
            logging.info(column_name_dtype)
            logging.info(rows_tabela_schemas_mysql)
        elif data_type == 'int':
            query = " SELECT CONCAT('CASE WHEN ', lower(tm.column_name_datalake), ' = '' '' OR ', lower(tm.column_name_datalake), ' = ''-'' THEN NULL ELSE CAST(SPLIT(',lower(tm.column_name_datalake),', ''.'')[SAFE_OFFSET(0)] AS ' ,td.datatype_destino, ') END as ',lower(tm.column_name_datalake),',') as coluna_concat_cast,tm.column_name_datalake as  column_name_datalake, REPLACE(CONCAT('bigquery.SchemaField(#',tm.column_name_datalake,'#,#STRING#),'),'#','''') as column_schema_bq, tm.ordinal_position as ordinal_position  FROM controle_etl.table_mapping tm, transform_datatype td  WHERE tm.codigo_tabela=:codigo_tabela AND lower(tm.data_type) = 'int' AND lower(tm.data_type) = lower(td.datatype_origem) order by tm.ordinal_position "
            tabela_schemas = conexaoMySQL.execute(text(query), {"codigo_tabela": codigo_tabela})
            rows_tabela_schemas_int = tabela_schemas.fetchall()

            for dados_schemas in rows_tabela_schemas_int:
                array_tabela_ints  = np.array(dados_schemas)
                rows_tabela_schemas_mysql.append(array_tabela_ints)
                column_name_dtype = column_name_dtype + array_tabela_ints[1] + ','
            logging.info('Entrou no int')
        elif data_type == 'float':
            #query = " SELECT CONCAT('CASE WHEN ', lower(tm.column_name_datalake), ' = '' '' OR ', lower(tm.column_name_datalake), ' = ''-'' THEN NULL ELSE SAFE_CAST(REPLACE(',lower(tm.column_name_datalake),',',','.') AS ' ,td.datatype_destino, ') END as ',lower(tm.column_name_datalake),',') as coluna_concat_cast,tm.column_name_datalake as  column_name_datalake, REPLACE(CONCAT('bigquery.SchemaField(#',tm.column_name_datalake,'#,#STRING#),'),'#','''') as column_schema_bq, tm.ordinal_position as ordinal_position  FROM controle_etl.table_mapping tm, transform_datatype td  WHERE tm.codigo_tabela=:codigo_tabela AND lower(tm.data_type) = 'float' AND lower(tm.data_type) = lower(td.datatype_origem) order by tm.ordinal_position "
            query = """SELECT CONCAT('CASE WHEN ', lower(tm.column_name_datalake), ' = '' '' OR ', lower(tm.column_name_datalake), ' = ''-'' THEN NULL ELSE SAFE_CAST(REPLACE(',lower(tm.column_name_datalake),',",",".") AS ' ,td.datatype_destino, ') END as ',lower(tm.column_name_datalake),',') as coluna_concat_cast,tm.column_name_datalake as  column_name_datalake, REPLACE(CONCAT('bigquery.SchemaField(#',tm.column_name_datalake,'#,#STRING#),'),'#','''') as column_schema_bq, tm.ordinal_position as ordinal_position  FROM controle_etl.table_mapping tm, transform_datatype td  WHERE tm.codigo_tabela=:codigo_tabela AND lower(tm.data_type) = 'float' AND lower(tm.data_type) = lower(td.datatype_origem) order by tm.ordinal_position"""
            tabela_schemas = conexaoMySQL.execute(text(query), {"codigo_tabela": codigo_tabela})
            rows_tabela_schemas_float = tabela_schemas.fetchall()

            for dados_schemas in rows_tabela_schemas_float:
                array_tabela_floats  = np.array(dados_schemas)
                rows_tabela_schemas_mysql.append(array_tabela_floats)
                column_name_dtype = column_name_dtype + array_tabela_floats[1] + ','
        else:
            if i <= 0:
                query = " SELECT CONCAT('CAST(',lower(tm.column_name_datalake),' AS ' ,td.datatype_destino, ') as ',lower(tm.column_name_datalake),',') as coluna_concat_cast,tm.column_name_datalake as  column_name_datalake, REPLACE(CONCAT('bigquery.SchemaField(#',tm.column_name_datalake,'#,#STRING#),'),'#','''') as column_schema_bq, tm.ordinal_position as ordinal_position  FROM controle_etl.table_mapping tm, transform_datatype td  WHERE tm.codigo_tabela=:codigo_tabela AND lower(tm.data_type) <> 'date' AND lower(tm.data_type) <> 'int' AND lower(tm.data_type) <> 'float' AND lower(tm.data_type) = lower(td.datatype_origem) order by tm.ordinal_position "
                tabela_schemas = conexaoMySQL.execute(text(query), {"codigo_tabela": codigo_tabela})
                rows_tabela_schemas_all = tabela_schemas.fetchall()

                for dados_schemas in rows_tabela_schemas_all:
                    array_tabela_all  = np.array(dados_schemas)
                    rows_tabela_schemas_mysql.append(array_tabela_all)
                    column_name_dtype = column_name_dtype + array_tabela_all[1] + ','
                i += 1
            else:
                pass

    rows_tabela_schemas_mysql = pd.DataFrame(rows_tabela_schemas_mysql, columns=['coluna_concat_cast', 'column_name_datalake', 'column_schema_bq', 'ordinal_position'])
    rows_tabela_schemas_mysql['ordinal_position'] = rows_tabela_schemas_mysql['ordinal_position'].astype('int')
    rows_tabela_schemas_mysql = rows_tabela_schemas_mysql.sort_values(by='ordinal_position')

    for index, dados_schemas in rows_tabela_schemas_mysql.iterrows():
        coluna_concat_cast              = dados_schemas['coluna_concat_cast']
        column_name_datalake            = dados_schemas['column_name_datalake']
        column_schema_bq                = dados_schemas['column_schema_bq']
        teste = column_schema_bq[:-1]
        list_colunas_schema.append(f'{teste}')
        sql_colunas_trusted = sql_colunas_trusted + coluna_concat_cast
        sql_colunas_raw     = sql_colunas_raw + column_name_datalake + ','
        column_schema_bq = column_schema_bq.replace("'",'"')
        sql_colunas_schema = sql_colunas_schema + column_schema_bq

    logging.info(sql_colunas_trusted)


    client_bq = connectionGoogleBigQuery()

    table_ref = client_bq.dataset(bigquery_dataset_land).table(bigquery_dataset_tabela)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    path_raw_parquet = "/home/airflow/raw/flatfile/" + parquet_file

    with open(path_raw_parquet, "rb") as source_file:
        job = client_bq.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )

    job.result()

    try:
        logging.info(f"Updating table {bigquery_dataset_land}.{bigquery_dataset_tabela}.")
        query_update = ("UPDATE " + projeto + "." + bigquery_dataset_land + "." + bigquery_dataset_tabela + " SET sys_change_version = 0 , sys_change_operation = 'I', sys_change_creation_version = 0, commit_time = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR), partition_time = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR) WHERE partition_time is null ;")
        client_bq.query(query_update)
        time.sleep(15)
        logging.info(f"Successfully updated table {bigquery_dataset_land}.{bigquery_dataset_tabela}.")

        insertMonitoring('UPDATE', f'{bigquery_dataset_land}.{bigquery_dataset_tabela}', codigo_tabela, 'OK')
    except Exception as update_error:
        logging.error(f"An error has occured during UPDATE process in {bigquery_dataset_land}.{bigquery_dataset_tabela}. Error: {update_error}")
        insertMonitoring('UPDATE', f'{bigquery_dataset_land}.{bigquery_dataset_tabela}', codigo_tabela, 'ERROR')
        raise update_error

    bq_query_quantidade_land = (" SELECT count(*) as quantidade_linhas FROM " + projeto + "." + bigquery_dataset_land + "." + bigquery_dataset_tabela + ";")
    query_job = client_bq.query(bq_query_quantidade_land)
    query_job.result()
    for quantidade_linhas_resultset in query_job:
        quantidade_linhas = quantidade_linhas_resultset['quantidade_linhas']

    bq_query_partition_time_land = (" SELECT max(partition_time) as partition_time FROM " + projeto + "." + bigquery_dataset_land + "." + bigquery_dataset_tabela + ";")
    query_job = client_bq.query(bq_query_partition_time_land)
    query_job.result()
    for partition_time_resultset in query_job:
        partition_time = str(partition_time_resultset['partition_time'])

    try:
        logging.info(f"Updating table controle for table {bigquery_dataset_tabela}.")
        update_query = " UPDATE controle_etl.controle SET partition_time = :partition_time WHERE codigo=:codigo_tabela "
        update_data = { 'partition_time': partition_time, 'codigo_tabela': codigo_tabela }
        conexaoMySQL = connectionMySQL()
        conexaoMySQL.execute(text(update_query), update_data)
        conexaoMySQL.commit()
        logging.info(f"Successfully updated table controle for table {bigquery_dataset_tabela}.")

        insertMonitoring('UPDATE', 'controle', codigo_tabela, 'OK')
    except Exception as controle_error:
        logging.error(f"An error has occured during UPDATE process in {bigquery_dataset_land}.{bigquery_dataset_tabela}. Error: {controle_error}")
        insertMonitoring('UPDATE', 'controle', codigo_tabela, 'ERROR')
        raise controle_error

    try:
        logging.info(f"Inserting data into table {bigquery_dataset_raw}.{bigquery_dataset_tabela}.")
        sql_query_raw = ("INSERT INTO " + projeto + "." + bigquery_dataset_raw + "." + bigquery_dataset_tabela + " SELECT sys_change_version, sys_change_operation, sys_change_creation_version, commit_time, " + sql_colunas_raw + "  PARTITION_TIME FROM "  + projeto + "." + bigquery_dataset_land + "." + bigquery_dataset_tabela + ";")
        client_bq.query(sql_query_raw)
        query_job.result()
        time.sleep(5)
        logging.info(f"Successfully inserted data into table {bigquery_dataset_raw}.{bigquery_dataset_tabela}.")

        insertMonitoring('INSERT', f'{bigquery_dataset_raw}.{bigquery_dataset_tabela}', codigo_tabela, 'OK')
    except Exception as insert_error:
        logging.error(f"An error has occured during INSERT process in {bigquery_dataset_raw}.{bigquery_dataset_tabela}. Error: {insert_error}")
        insertMonitoring('INSERT', f'{bigquery_dataset_raw}.{bigquery_dataset_tabela}', codigo_tabela, 'ERROR')
        raise insert_error
    
    if tipo_carga == 'full': 
        try:
            logging.info(f"Truncating table {bigquery_dataset_trusted}.{bigquery_dataset_tabela}.")
            bq_query_truncate_trusted = ("TRUNCATE TABLE " + projeto + "." + bigquery_dataset_trusted + "." + bigquery_dataset_tabela + ";")
            query_job = client_bq.query(bq_query_truncate_trusted)
            query_job.result()
            time.sleep(15)
            logging.info(f"Successfully truncated table {bigquery_dataset_trusted}.{bigquery_dataset_tabela}.")

            insertMonitoring('TRUNCATE', f'{bigquery_dataset_trusted}.{bigquery_dataset_tabela}', codigo_tabela, 'OK')
        except Exception as truncate_error:
            logging.error(f"An error has occured during TRUNCATE process in {bigquery_dataset_trusted}.{bigquery_dataset_tabela}. Error: {truncate_error}")
            insertMonitoring('TRUNCATE', f'{bigquery_dataset_trusted}.{bigquery_dataset_tabela}', codigo_tabela, 'ERROR')
            raise truncate_error

        try:
            logging.info(f"Inserting data into table {bigquery_dataset_trusted}.{bigquery_dataset_tabela}.")
            sql_query_trusted = ("INSERT INTO " + projeto + "." + bigquery_dataset_trusted + "." + bigquery_dataset_tabela + "  SELECT " + sql_colunas_trusted + "  PARTITION_TIME FROM "  + projeto + "." + bigquery_dataset_raw + "." + bigquery_dataset_tabela + " WHERE PARTITION_TIME ='" + partition_time + "' ;")
            #print(sql_query_trusted)
            query_job = client_bq.query(sql_query_trusted)
            query_job.result()
            logging.info(f"Successfully inserted data into table {bigquery_dataset_trusted}.{bigquery_dataset_tabela}.")

            insertMonitoring('INSERT', f'{bigquery_dataset_trusted}.{bigquery_dataset_tabela}', codigo_tabela, 'OK')
        except Exception as trusted_error:
            logging.error(f"An error has occured during INSERT process in {bigquery_dataset_trusted}.{bigquery_dataset_tabela}. Error: {trusted_error}")
            insertMonitoring('INSERT', f'{bigquery_dataset_trusted}bigquery_dataset_tabela', codigo_tabela, 'ERROR')
            raise trusted_error
            
    elif tipo_carga == 'incremental':

        update_columns = ""
        insert_columns = ""
        tabela_colunas_origemdatalake = "" 
        sql_query = text(" SELECT CONCAT('T.',lower(tm.column_name_datalake),' = S.' ,lower(tm.column_name_datalake),',') as update_column FROM table_mapping tm, controle c WHERE tm.codigo_tabela=:codigo_tabela AND c.codigo = tm.codigo_tabela AND lower(tm.column_name) <> lower(c.tabela_origem_pk) order by tm.ordinal_position " )
        mapping_mysql = conexaoMySQL.execute(sql_query, {"codigo_tabela": codigo_tabela})
        rows_mapping_mysql = mapping_mysql.fetchall()
        # Print the results
        for array_mapping_bq in rows_mapping_mysql:
            array_columns_bq     = np.array(array_mapping_bq)
            update_column_name   = array_columns_bq[0]
            update_columns  = update_columns + update_column_name

        sql_query = text(" SELECT CONCAT('S.',lower(tm.column_name_datalake),',') as insert_column FROM table_mapping tm  WHERE tm.codigo_tabela=:codigo_tabela order by tm.ordinal_position " )
        mapping_mysql = conexaoMySQL.execute(sql_query, {"codigo_tabela": codigo_tabela})
        rows_mapping_mysql = mapping_mysql.fetchall()
        # Print the results
        for array_mapping_bq in rows_mapping_mysql:
            array_columns_bq     = np.array(array_mapping_bq)
            insert_column_name   = array_columns_bq[0]
            insert_columns  = insert_columns + insert_column_name
        
        sql_query = text(" SELECT CONCAT(lower(tm.column_name_datalake),',') as insert_column FROM table_mapping tm  WHERE tm.codigo_tabela=:codigo_tabela order by tm.ordinal_position " )
        mapping_mysql = conexaoMySQL.execute(sql_query, {"codigo_tabela": codigo_tabela})
        rows_mapping_mysql = mapping_mysql.fetchall()
        # Print the results
        for array_mapping_bq in rows_mapping_mysql:
            array_columns_bq     = np.array(array_mapping_bq)
            insert_coluna_nome   = array_columns_bq[0]
            tabela_colunas_origemdatalake  = tabela_colunas_origemdatalake + insert_coluna_nome

        try:
            logging.info(f"Merging data into table {bigquery_dataset_trusted}.{bigquery_dataset_tabela}.")
            sql_query_trusted = ("MERGE " + projeto + "." + bigquery_dataset_trusted + "." + bigquery_dataset_tabela + " T USING (WITH TABLE_MERGE AS ( SELECT ROW_NUMBER() OVER (PARTITION BY " + tabela_origem_pk + " ORDER BY PARTITION_TIME DESC) AS dense_rank, * FROM " + projeto + "." + bigquery_dataset_raw + "." + bigquery_dataset_tabela + " WHERE partition_time >= '" + partition_time + "'), TABLE_FILTER AS ( SELECT " + sql_colunas_trusted + " partition_time FROM TABLE_MERGE WHERE dense_rank = 1) SELECT DISTINCT * FROM TABLE_FILTER) S ON T." + tabela_origem_pk.lower() + "=S." + tabela_origem_pk.lower() + " WHEN MATCHED THEN UPDATE SET " + update_columns + " T.partition_time = S.partition_time WHEN NOT MATCHED BY SOURCE THEN DELETE WHEN NOT MATCHED BY TARGET THEN INSERT (" + tabela_colunas_origemdatalake + " partition_time) VALUES ( " + insert_columns + " S.partition_time );")
            print(sql_query_trusted)
            query_job = client_bq.query(sql_query_trusted)
            query_job.result()
            logging.info(f"Successfully merged data into table {bigquery_dataset_trusted}.{bigquery_dataset_tabela}.")

            insertMonitoring('MERGE', f'{bigquery_dataset_trusted}.{bigquery_dataset_tabela}', codigo_tabela, 'OK')
        except Exception as trusted_error:
            logging.error(f"An error has occured during MERGE process in {bigquery_dataset_trusted}.{bigquery_dataset_tabela}. Error: {trusted_error}")
            insertMonitoring('INSERT', f'{bigquery_dataset_trusted}bigquery_dataset_tabela', codigo_tabela, 'ERROR')
            raise trusted_error

    else:
        print('Sem carga')


    try:
        logging.info(f"Truncating table {bigquery_dataset_land}.{bigquery_dataset_tabela}.")
        bq_query_truncate_land = ("TRUNCATE TABLE " + projeto + "." + bigquery_dataset_land + "." + bigquery_dataset_tabela + ";")
        query_job = client_bq.query(bq_query_truncate_land)
        query_job.result()
        time.sleep(15)
        logging.info(f"Successfully truncated table {bigquery_dataset_land}.{bigquery_dataset_tabela}.")

        insertMonitoring('TRUNCATE', f'{bigquery_dataset_land}.{bigquery_dataset_tabela}', codigo_tabela, 'OK')
    except Exception as truncate_land:
        logging.error(f"An error has occured during TRUNCATE process in {bigquery_dataset_land}.{bigquery_dataset_tabela}. Error: {truncate_land}")
        insertMonitoring('TRUNCATE', f'{bigquery_dataset_land}.{bigquery_dataset_tabela}', codigo_tabela, 'ERROR')
        raise truncate_land

    bucket_name = "roxpartner-estagiarios-infra"
    path_land_local = 'land/flatfile/' + arquivo_origem
    destination_blob_name = destination_path_original + arquivo_origem
    # copia o arquivo original da land para a raw do bucket
    copyFileGCPBucket(bucket_name, path_land_local, destination_blob_name)

    path_raw_local = "/home/airflow/raw/flatfile/" + parquet_file
    destination_blob_name = destination_path_processed + parquet_file
    # faz o upload do arquivo em formato parquet no diretório local para a bucket
    uploadFileGCPBucket(bucket_name, path_raw_local, destination_blob_name)

    # apaga os arquivo da land do bucket
    blob_name = source_folder_path + arquivo_origem
    deleteFileGCPBucket(bucket_name, blob_name)

    delete_files(raw_path_file)
    return "ok"



conexaoGoogleBigQuery = connectionGoogleBigQuery()
conexaoMySQL          = connectionMySQL()

query = " SELECT codigo, tabela_origem, bigquery_dataset_land, bigquery_dataset_raw, bigquery_dataset_trusted, bigquery_dataset_tabela, projeto, dt_carga_ini, dt_carga_fim, tipo_carga, bigquery_svc, tabela_colunas_origem, tipo_banco_origem, sistema_origem, tabela_origem_pk, partition_time, extensao, flatfile_cabecalho_linha, flatfile_data_linha, flatfile_range_coluna FROM controle WHERE tipo='flatfile' and status = 'ativo' and bigquery_create_table = 'N' and DATE_ADD(dt_carga_fim, INTERVAL 1 MINUTE) < now() "
tabela_controle_mysql = conexaoMySQL.execute(text(query))
rows_tabela_controle_mysql = tabela_controle_mysql.fetchall()
def run(codigo_tabela, tabela_origem, bigquery_dataset_land, bigquery_dataset_raw, bigquery_dataset_trusted, bigquery_dataset_tabela, projeto, dt_carga_ini, dt_carga_fim, tipo_carga, bigquery_svc, tabela_colunas_origem, tipo_banco_origem, sistema_origem, tabela_origem_pk, partition_time, extensao, flatfile_cabecalho_linha, flatfile_data_linha, flatfile_range_coluna, **kwargs):
    array_tabela_controle      = np.array([codigo_tabela, tabela_origem, bigquery_dataset_land, bigquery_dataset_raw, bigquery_dataset_trusted, bigquery_dataset_tabela, projeto, dt_carga_ini, dt_carga_fim, tipo_carga, bigquery_svc, tabela_colunas_origem, tipo_banco_origem, sistema_origem, tabela_origem_pk, partition_time, extensao, flatfile_cabecalho_linha, flatfile_data_linha, flatfile_range_coluna])
    codigo_tabela              = array_tabela_controle[0]
    tabela_origem              = array_tabela_controle[1]
    bigquery_dataset_land      = array_tabela_controle[2]
    bigquery_dataset_raw       = array_tabela_controle[3]
    bigquery_dataset_trusted   = array_tabela_controle[4]
    bigquery_dataset_tabela    = array_tabela_controle[5]
    projeto                    = array_tabela_controle[6]
    dt_carga_ini               = array_tabela_controle[7]
    dt_carga_fim               = array_tabela_controle[8]
    tipo_carga                 = array_tabela_controle[9]
    bigquery_svc               = array_tabela_controle[10]
    tabela_colunas_origem      = array_tabela_controle[11]
    tipo_banco_origem          = array_tabela_controle[12]
    sistema_origem             = array_tabela_controle[13]
    tabela_origem_pk           = array_tabela_controle[14]
    partition_time             = array_tabela_controle[15]
    extensao                   = array_tabela_controle[16]
    flatfile_cabecalho_linha   = array_tabela_controle[17]
    flatfile_data_linha        = array_tabela_controle[18]
    flatfile_range_coluna      = array_tabela_controle[19]
    parquet_file               = tabela_origem.replace(extensao, 'parquet')


    sql_cast_columns = ""
    sql_columns_datalake = ""

    # variáveis que contém os jsons de referencia para os nomes das colunas e para o tipo dos dados
    # abre as chave do json
    sql_df_new_column_names = "{"
    sql_dtypes = "{"

    sql_query = text(" SELECT CONCAT('CAST(',lower(tm.column_name_datalake),' AS ' ,td.datatype_destino, ') as ',lower(tm.column_name_datalake),',') as coluna_concat_cast, tm.column_name_datalake as  column_name_datalake, REPLACE(CONCAT('#',tm.column_name,'#: #',tm.column_name_datalake,'#,'),'#','''') as df_new_column_names, REPLACE(CONCAT('#',tm.column_name_datalake,'#: #string#,'),'#','''') as coluna_dtype   FROM controle_etl.table_mapping tm, transform_datatype td  WHERE tm.codigo_tabela=:codigo_tabela AND lower(tm.data_type) = lower(td.datatype_origem) order by tm.ordinal_position " )
    mapping_mysql = conexaoMySQL.execute(sql_query, {"codigo_tabela": codigo_tabela})
    rows_mapping_mysql = mapping_mysql.fetchall()
    # Print the results
    for array_mapping_bq in rows_mapping_mysql:
        array_columns_bq         = np.array(array_mapping_bq)
        coluna_concat_cast       = array_columns_bq[0].lower()
        column_name_datalake     = array_columns_bq[1].lower()
        df_new_column_names      = array_columns_bq[2]
        coluna_dtype             = array_columns_bq[3].lower()


        sql_cast_columns         = sql_cast_columns + coluna_concat_cast
        sql_columns_datalake     = sql_columns_datalake + column_name_datalake + ','
        # recebe os respectivos dados
        sql_df_new_column_names  = sql_df_new_column_names + df_new_column_names
        sql_dtypes               = sql_dtypes + coluna_dtype

    # tira a última vírgula e fecha as chaves do json
    sql_df_new_column_names = sql_df_new_column_names[:-1]
    sql_df_new_column_names = sql_df_new_column_names + "}"
    sql_dtypes              = sql_dtypes[:-1]
    sql_dtypes              = sql_dtypes + "}"

    # substitue as aspas simples por aspas duplas
    sql_df_new_column_names = sql_df_new_column_names.replace("'",'"')
    sql_dtypes = sql_dtypes.replace("'",'"')

     # método .loads que converte a string em um json
    new_column_names = json.loads(sql_df_new_column_names)
    dtypes = json.loads(sql_dtypes)
    separator = ';'

    bucket_name = 'roxpartner-estagiarios-infra'
    blob_name = 'land/flatfile/' + tabela_origem
    destination_file_name = r"C:\Users\gustavo.raposo\Desktop\flatfiles\land"

    csv_arquivo = downloadFileGCPBucket(bucket_name, blob_name)
    
    if csv_arquivo:
        csv_to_parquet(csv_arquivo, parquet_file, flatfile_cabecalho_linha, flatfile_data_linha, new_column_names, dtypes, codigo_tabela)
    


with DAG(
        dag_id="flatfile_load_transform_data",
        schedule_interval='@daily',
        start_date=pendulum.datetime(2024, 9, 2, tz="America/Sao_Paulo"),
        catchup=False,
        is_paused_upon_creation=True,
        tags=["Extração e Transformação", "flatfile"]
) as dag:
    tasks = []

    for row in rows_tabela_controle_mysql:
        codigo_tabela, tabela_origem, bigquery_dataset_land, bigquery_dataset_raw, bigquery_dataset_trusted, bigquery_dataset_tabela, projeto, dt_carga_ini, dt_carga_fim, tipo_carga, bigquery_svc, tabela_colunas_origem, tipo_banco_origem, sistema_origem, tabela_origem_pk, partition_time, extensao, flatfile_cabecalho_linha, flatfile_data_linha, flatfile_range_coluna = row

        task = PythonOperator(
            task_id=bigquery_dataset_tabela,
            python_callable=run,
            provide_context=True,
            retries=0,
            op_kwargs={
                'codigo_tabela': codigo_tabela,
                'tabela_origem': tabela_origem,
                'bigquery_dataset_land': bigquery_dataset_land,
                'bigquery_dataset_raw': bigquery_dataset_raw,
                'bigquery_dataset_trusted': bigquery_dataset_trusted,
                'bigquery_dataset_tabela': bigquery_dataset_tabela,
                'projeto': projeto,
                'dt_carga_ini': dt_carga_ini,
                'dt_carga_fim': dt_carga_fim,
                'tipo_carga': tipo_carga,
                'bigquery_svc': bigquery_svc,
                'tabela_colunas_origem': tabela_colunas_origem,
                # 'schema_origem': schema_origem,
                'tipo_banco_origem': tipo_banco_origem,
                'sistema_origem': sistema_origem,
                'tabela_origem_pk': tabela_origem_pk,
                'partition_time': partition_time,
                'extensao': extensao,
                'flatfile_cabecalho_linha': flatfile_cabecalho_linha,
                'flatfile_data_linha': flatfile_data_linha,
                'flatfile_range_coluna': flatfile_range_coluna
            },
        )
        tasks.append(task)

    end_task = DummyOperator(
        task_id='end_task'
    )

    for task in tasks:
        task >> end_task
    
    trigger_task = TriggerDagRunOperator(
        task_id='trigger_dag',
        trigger_dag_id='refined_datamarts',
    )

    end_task >> trigger_task