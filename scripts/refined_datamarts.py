from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
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
from datetime import datetime, timedelta
import pendulum



key_path = "/home/airflow/roxpartner-estagiarios-infra-b4a1bebdd8d5.json"
gcs_client = storage.Client.from_service_account_json(json_credentials_path=key_path)

project_num = '122083571465'
gcs_bucket_name = "roxpartner-estagiarios-infra"
gcs_client = storage.Client.from_service_account_json(json_credentials_path=key_path)


credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

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

def connectionGoogleBigQuery():

   
    key_path = "/home/airflow/roxpartner-estagiarios-infra-b4a1bebdd8d5.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client_bq = bigquery.Client(credentials=credentials, project=credentials.project_id)

    return client_bq

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
    


def create_refined_datamarts():

    conexaoMySQL          = connectionMySQL()
    conexaoGoogleBigQuery = connectionGoogleBigQuery()

    create_pedidos = "CREATE OR REPLACE TABLE `roxpartner-estagiarios-infra.REFINED_FLATFILE.pedidos` AS SELECT sod.sales_order_id,sod.sales_order_detail_id, sod.carrier_tracking_number, sod.order_qty,sod.special_offer_id,sod.unit_price,sod.unit_price_discount,sod.line_total,soh.revision_number,soh.status,soh.online_order_flag,soh.sales_order_number,soh.purchase_order_number, soh.sales_person_id, soh.bill_to_address_id, soh.ship_method_id, soh.credit_card_id, soh.credit_card_approval_code, soh.currency_rate_id, soh.subtotal, soh.tax_amt, soh.freight, soh.total_due, soh.comment, sod.product_id, soh.customer_id, soh.order_date, soh.due_date, soh.ship_date FROM `roxpartner-estagiarios-infra.TRUSTED_FLATFILE.flatfile_sales_salesorderdetail_csv` sod INNER JOIN `roxpartner-estagiarios-infra.TRUSTED_FLATFILE.flatfile_sales_salesorderheader_csv` soh ON sod.sales_order_id = soh.sales_order_id"
    try:
        logging.info(f'Creating fact table pedidos')
        query_job = conexaoGoogleBigQuery.query(create_pedidos)
        query_job.result()
        logging.info(f'Sucessfully create fact table pedidos')
    except Exception as create_error:
         logging.error(f'An error has ocurred during CREATE TABLE process in fact table pedidos')
         raise create_error
    

    create_clientes = "CREATE OR REPLACE TABLE `roxpartner-estagiarios-infra.REFINED_FLATFILE.cliente` AS select customer_id, person_id, store_id, territory_id, account_number from `roxpartner-estagiarios-infra.TRUSTED_FLATFILE.flatfile_sales_customer_csv`"
    try:
        logging.info(f'Creating dim table clientes')
        query_job = conexaoGoogleBigQuery.query(create_clientes)
        query_job.result()
        logging.info(f'Sucessfully create dim table clientes')
    except Exception as create_error:
         logging.error(f'An error has ocurred during CREATE TABLE process in dim table clientes')
         raise create_error
    
    create_produtos = "CREATE OR REPLACE TABLE `roxpartner-estagiarios-infra.REFINED_FLATFILE.produto` AS SELECT product_id, name, product_number, make_flag, finished_goods_flag, color, safety_stock_level, reorder_point, standard_cost, list_price, size, size_unit_measure_code, weight, days_to_manufacture, product_line, class, style, product_subcategory_id, product_model_id from `roxpartner-estagiarios-infra.TRUSTED_FLATFILE.flatfile_production_product_csv`"
    try:
        logging.info(f'Creating dim table produtos')
        query_job = conexaoGoogleBigQuery.query(create_produtos)
        query_job.result()
        logging.info(f'Sucessfully create dim table produtos')
    except Exception as create_error:
         logging.error(f'An error has ocurred during CREATE TABLE process in dim table produtos')
         raise create_error
    

    create_tempo = "CREATE OR REPLACE TABLE `roxpartner-estagiarios-infra.REFINED_FLATFILE.tempo` AS SELECT pro.sell_start_date, EXTRACT(YEAR FROM pro.sell_start_date) AS sell_start_year, EXTRACT(MONTH FROM pro.sell_start_date) AS sell_start_month, EXTRACT(DAY FROM pro.sell_start_date) AS sell_start_day, pro.sell_end_date, EXTRACT(YEAR FROM  pro.sell_end_date) AS sell_end_year, EXTRACT(MONTH FROM pro.sell_end_date) AS sell_end_month, EXTRACT(DAY FROM pro.sell_end_date) AS sell_end_day, soh.order_date, EXTRACT(YEAR FROM  soh.order_date) AS order_year, EXTRACT(MONTH FROM soh.order_date) AS order_month, EXTRACT(DAY FROM soh.order_date) AS order_day, soh.due_date, EXTRACT(YEAR FROM  soh.due_date) AS due_year, EXTRACT(MONTH FROM soh.due_date) AS due_month, EXTRACT(DAY FROM soh.due_date) AS due_day, soh.ship_date, EXTRACT(YEAR FROM  soh.ship_date) AS ship_year, EXTRACT(MONTH FROM soh.ship_date) AS ship_month, EXTRACT(DAY FROM soh.ship_date) AS ship_day, FROM `roxpartner-estagiarios-infra.TRUSTED_FLATFILE.flatfile_production_product_csv` pro INNER JOIN `roxpartner-estagiarios-infra.TRUSTED_FLATFILE.flatfile_sales_salesorderdetail_csv` sod ON pro.product_id = sod.product_id INNER JOIN `roxpartner-estagiarios-infra.TRUSTED_FLATFILE.flatfile_sales_salesorderheader_csv` soh ON sod.sales_order_id = soh.sales_order_id"
    try:
        logging.info(f'Creating dim table tempo')
        query_job = conexaoGoogleBigQuery.query(create_tempo)
        query_job.result()
        logging.info(f'Sucessfully create dim table tempo')
    except Exception as create_error:
         logging.error(f'An error has ocurred during CREATE TABLE process in dim table tempo')
         raise create_error
    
    create_vendedor = "CREATE OR REPLACE TABLE `roxpartner-estagiarios-infra.REFINED_FLATFILE.vendedor` as select distinct sales_person_id, name_style, title, first_name, middle_name, last_name, suffix, email_promotion, additional_contact_info, demographics from `roxpartner-estagiarios-infra.TRUSTED_FLATFILE.flatfile_sales_salesorderheader_csv` soh inner join `roxpartner-estagiarios-infra.TRUSTED_FLATFILE.flatfile_person_person_csv` per on soh.sales_person_id = per.business_entity_id"
    try:
        logging.info(f'Creating dim table vendedor')
        query_job = conexaoGoogleBigQuery.query(create_vendedor)
        query_job.result()
        logging.info(f'Sucessfully create dim table vendedor')
    except Exception as create_error:
         logging.error(f'An error has ocurred during CREATE TABLE process in dim table vendedor')
         raise create_error

    print('ok')
    


def run():
    create_refined_datamarts()


with DAG(
        dag_id="refined_datamarts",
        schedule_interval=None,
        start_date=pendulum.datetime(2024, 9, 4, tz="America/Sao_Paulo"),
        catchup=False,
        is_paused_upon_creation=True,
        tags=["refined", "flatfile"]
) as dag:
    PythonOperator(
        task_id='refined-datamart',
        python_callable=run,
        provide_context=True,
        retries=0,
    )
    pass