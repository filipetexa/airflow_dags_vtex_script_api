from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from concurrent.futures import ThreadPoolExecutor


# Importando suas funções customizadas
from my_company.common_package.fetch_catalog_and_pricing import fetch_catalog_and_pricing



from airflow.hooks.base_hook import BaseHook

# Função para criar os dicionários de configuração
def get_vtex_config():
    connection = BaseHook.get_connection('vtex_api')
    return {
        'api_key': connection.login,
        'api_token': connection.password,
        'account_name': connection.extra_dejson.get('account_name')
    }

def get_mongodb_config():
    connection = BaseHook.get_connection('mongodb_radan')
    return {
        'host': connection.host,
        'port': connection.port,
        'username': connection.login,
        'password': connection.password,
        'database': connection.extra_dejson.get('database')
    }

def main():
    # Criar os dicionários de configuração
    vtex_config = get_vtex_config()
    mongodb_config = get_mongodb_config()

    logging.info(f"VTEX Config: {vtex_config}")
    logging.info(f"MongoDB Config: {mongodb_config}")

    # chama função para realaizar o fetch das ordens pasando o numero de workers
    fetch_catalog_and_pricing(workers=20, config=vtex_config, mongo_config=mongodb_config)
    
    

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

# Definindo a DAG
dag = DAG(
    dag_id='dag_update_orders_24h',
    default_args=default_args,
    description='A single task DAG that encapsulates the entire process',
    schedule_interval='0 0 * * *',  # Executa à meia-noite todos os dias
    catchup=False  # Impede a execução de 'catch-up' para dias passados quando a DAG não foi executada
)

# Definindo a Task
execute_process_task = PythonOperator(
    task_id='execute_main_function',
    python_callable=main,
    dag=dag,
)

