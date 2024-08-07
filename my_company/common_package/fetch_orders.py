import requests
import logging
from datetime import datetime, timedelta
from my_company.common_package.mongodb_functions import insert_json_to_collection, delete_document


# Configuração inicial do logging para capturar informações durante a execução.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_order_details(order_id):
    # Carrega configurações da API da Vtex a partir do arquivo de configuração.
    # config = load_config('vtex')
    config = {
    'api_key': 'vtexappkey-lojasradan-PZLPSM',
    'api_token': 'CSUMUBDCOEGJOZNCOJXXQTBKJDTZTCYWSLOUSWEMCXDVMWJXIWTNTKABWFJAUYXVJWKZGOAXWGGDJAEIFVUHCOPHBEXERXNYHYEKQOGCSPHTEDUWRCTHCAHUSRRWPPKY',
    'account_name': 'lojasradan'
    }

    # Constrói o endpoint para detalhes da ordem utilizando o ID da ordem.
    details_endpoint = f"https://{config['account_name']}.vtexcommercestable.com.br/api/oms/pvt/orders/{order_id}"
    headers = {
        "X-VTEX-API-AppKey": config['api_key'],
        "X-VTEX-API-AppToken": config['api_token']
    }
    try:
        # Faz a requisição para obter detalhes da ordem.
        response = requests.get(details_endpoint, headers=headers)
        response.raise_for_status()  # Levanta um erro se a requisição falhar.
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Erro ao buscar detalhes da ordem {order_id}: {e}")
        return None

def date_range(start_date, end_date):
    # Gera todas as datas dentro do intervalo especificado.
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def fetch_orders_for_day(date, config, mongo_config):

    
    list_endpoint = f"https://{config['account_name']}.vtexcommercestable.com.br/api/oms/pvt/orders"
    headers = {
        "X-VTEX-API-AppKey": config['api_key'],
        "X-VTEX-API-AppToken": config['api_token']
    }
    page = 1
    more_pages = True
    

    while more_pages:
        params = {
            'f_creationDate': f"creationDate:[{date}T00:00:00.000Z TO {date}T23:59:59.999Z]",
            'per_page': 100,
            'page': page
        }
        try:
            # Faz a requisição para listar as ordens de um dia específico.
            response = requests.get(list_endpoint, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Imprime informações sobre a página atual e o número total de páginas.
            print(f"Processando {date}: Página {page} de {data['paging']['pages']}")

            for order in data['list']:
                # tenta deletar o documento da ordem no banco e deleta se ele existir
                delete_document('orders', {'orderId': order['orderId']}, mongo_config)

                order_details = fetch_order_details(order['orderId'])
                if order_details:
                    inserted_id = insert_json_to_collection(order_details, 'orders', mongo_config)
                    print(f"mongoDb collection id: {inserted_id}")

            # Verifica se há mais páginas para processar.
            paging = data['paging']
            if paging['currentPage'] < paging['pages']:
                page += 1
            else:
                more_pages = False
        except requests.RequestException as e:
            logging.error(f"Erro ao listar ordens para {date}: {e}")
            break

def fetch_orders(start_date, end_date):
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    for single_date in date_range(start_dt, end_dt):
        print(f"Iniciando processamento para {single_date.strftime('%Y-%m-%d')}")
        fetch_orders_for_day(single_date.strftime('%Y-%m-%d'))

if __name__ == '__main__':
    # Inicia o processo de buscar ordens para o intervalo especificado.
    fetch_orders('2023-01-01', '2023-01-05')
