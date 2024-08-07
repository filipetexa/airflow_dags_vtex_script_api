import requests
from mongodb_functions import insert_json_to_collection, clear_collections, validate_mongo_connection

from concurrent.futures import ThreadPoolExecutor

# Fetch detailed product information based on product ID
def fetch_product_data(product_id, config, mongo_config):

    product_details_endpoint = f"https://{config['account_name']}.vtexcommercestable.com.br/api/catalog/pvt/product/{product_id}"
    headers = {
        "X-VTEX-API-AppKey": config['api_key'],
        "X-VTEX-API-AppToken": config['api_token']
    }
    response = requests.get(product_details_endpoint, headers=headers)
    if response.status_code == 200:
        product_details = response.json()
        insert_json_to_collection(product_details, 'products', mongo_config)
        print(f"Product {product_id} details fetched and stored.")
    else:
        print(f"Error fetching product details for {product_id}: {response.status_code}")

# Fetch pricing information based on SKU ID
def fetch_pricing(sku_id, config, mongo_config):

    pricing_endpoint = f"https://api.vtex.com/{config['account_name']}/pricing/prices/{sku_id}"
    headers = {
        "X-VTEX-API-AppKey": config['api_key'],
        "X-VTEX-API-AppToken": config['api_token']
    }
    response = requests.get(pricing_endpoint, headers=headers)
    if response.status_code == 200:
        price = response.json()  
        insert_json_to_collection(price, 'pricings', mongo_config)
        print(f"Pricing for SKU {sku_id} fetched and stored.")
    else:
        print(f"Error fetching price for SKU {sku_id}: {response.status_code}")

# Fetch inventory information based on SKU ID
def fetch_inventory(sku_id, config, mongo_config):

    inventory_endpoint = f"https://{config['account_name']}.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/{sku_id}"
    headers = {
        "X-VTEX-API-AppKey": config['api_key'],
        "X-VTEX-API-AppToken": config['api_token']
    }
    response = requests.get(inventory_endpoint, headers=headers)
    if response.status_code == 200:
        inventory = response.json()  
        insert_json_to_collection(inventory, 'inventory', mongo_config)
        print(f"Inventory for SKU {sku_id} fetched and stored.")
    else:
        print(f"Error fetching price for SKU {sku_id}: {response.status_code}")

# Fetch and store SKU details for a given product ID
def fetch_skus(product_id, config, mongo_config):
    
    skus_endpoint = f"https://{config['account_name']}.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitByProductId/{product_id}"
    headers = {
        "X-VTEX-API-AppKey": config['api_key'],
        "X-VTEX-API-AppToken": config['api_token']
    }
    response = requests.get(skus_endpoint, headers=headers)
    if response.status_code == 200:
        skus = response.json()  # This is expected to be a list of SKUs
        for sku in skus:
            insert_json_to_collection(sku, 'SKUs', mongo_config)
            sku_id = sku.get('Id', 0)
            fetch_pricing(sku_id)
            fetch_inventory(sku_id)
    else:
        print(f"Error fetching SKUs for product {product_id}: {response.status_code}")

# Função para buscar e processar dados de produto e seus SKUs
def process_product(product_id, config, mongo_config):
    fetch_product_data(product_id, config, mongo_config)
    fetch_skus(product_id, config, mongo_config)
    
    
# Main function to fetch all products and their SKUs
def fetch_catalog_and_pricing(workers, config, mongo_config):
    
    catalog_endpoint = f"https://{config['account_name']}.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds"
    headers = {
        "X-VTEX-API-AppKey": config['api_key'],
        "X-VTEX-API-AppToken": config['api_token']
    }
    index_from = 1
    total_products = float('inf')
    processed = 0
    
    # Valida conexão com o banco Mongo
    connection_status = validate_mongo_connection(mongo_config)
    
    if connection_status:
        # clear collection on mongoDB
        clear_collections(['products', 'SKUs', 'pricings', 'products'])
        while processed < total_products:
            params = {
                '_from': index_from,
                '_to': index_from + 249
            }
            response = requests.get(catalog_endpoint, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                product_ids = data.get('data', [])
                total_products = data.get('range', {}).get('total', 0)

                # Utiliza ThreadPoolExecutor para processar cada produto em uma thread separada
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    executor.map(process_product, product_ids)
                
                executor.shutdown(wait=True)

                processed += len(product_ids)
                index_from += 250
            else:
                print(f"Error fetching catalog: {response.status_code}")



if __name__ == "__main__":
    fetch_catalog_and_pricing()
