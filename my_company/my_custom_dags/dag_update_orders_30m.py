from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from concurrent.futures import ThreadPoolExecutor


# Importando suas funções customizadas
from my_company.common_package.fetch_orders import fetch_orders_for_day

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

def safe_function(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        return f"Falha: {args}, Erro: {str(e)}"

def main():
    # Criar os dicionários de configuração
    vtex_config = get_vtex_config()
    mongodb_config = get_mongodb_config()

    logging.info(f"VTEX Config: {vtex_config}")
    logging.info(f"MongoDB Config: {mongodb_config}")

    # start_date = datetime.strptime('2024-06-01', '%Y-%m-%d')
    # end_date = datetime.strptime('2024-06-02', '%Y-%m-%d')
    
    # Definindo as datas dinamicamente
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    # Usando ThreadPoolExecutor para paralelizar a execução
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = executor.map(lambda date: safe_function(fetch_orders_for_day, date, vtex_config, mongodb_config),[date.strftime('%Y-%m-%d') for date in dates])
    
    # Fechamento dos executores após o término das tarefas
    executor.shutdown(wait=True)

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    
}

# Definindo a DAG
dag = DAG(
    dag_id='dag_update_orders_30m',  # Certifique-se de usar um ID único para a DAG
    default_args=default_args,
    description='A single task DAG that encapsulates the entire process',
    schedule_interval='*/30 * * * *',  # Executa a cada 30 minutos
    catchup=False  # Impede a execução de 'catch-up'
)


# Definindo a Task
execute_process_task = PythonOperator(
    task_id='execute_main_function',
    python_callable=main,
    dag=dag,
)

