from pymongo import MongoClient


def get_mongo_client(config):
    database = config['database']  # Certifique-se de que o nome do banco está sendo lido corretamente.
    
    # Verifica se o 'username' e 'password' existem e têm um valor não vazio.
    if config.get('username') and config.get('password'):
        # Usuário e senha fornecidos.
        #uri = f"mongodb+srv://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{database}"
        uri = f"mongodb+srv://{config['username']}:{config['password']}@{config['host']}/{database}"

    else:
        # Sem usuário e senha.
        uri = f"mongodb://{config['host']}:{config['port']}/{database}"

        
    client = MongoClient(uri)
    return client

def validate_mongo_connection(config):
    try:
        client = get_mongo_client(config)
        # Tenta listar os bancos de dados disponíveis para verificar a conexão
        databases = client.list_database_names()
        print("Conexão com o MongoDB validada com sucesso. Bancos de dados disponíveis:", databases)
        return True
    except Exception as e:
        print("Falha ao conectar com o MongoDB:", e)
        return False

def find_document(collection_name, query, config):
    client = get_mongo_client(config)
    db = client[config['database']]  
    
    collection = db[collection_name]
    documents = list(collection.find(query))
    
    if documents:
        print("Documentos encontrados:", documents)
    else:
        print("Nenhum documento encontrado com o filtro fornecido.")
    
    return documents


def insert_json_to_collection(json_data, collection_name, config):
    client = get_mongo_client(config)
    db = client[config['database']]
    collection = db[collection_name]
    result = collection.insert_one(json_data)
    
    # Retorna o ID do documento inserido
    return result.inserted_id


def delete_document(collection_name, query, config):
    client = get_mongo_client(config)
    db = client[config['database']]  
    
    collection = db[collection_name]
    result = collection.delete_many(query)  # Use delete_one(query) se desejar deletar apenas o primeiro documento encontrado
    
    if result.deleted_count > 0:
        print(f"{result.deleted_count} documento(s) deletado(s).")
    else:
        print("Nenhum documento foi deletado.")


def clear_collections(collections, config):
    client = get_mongo_client(config)
    db = client[config['database']]
    
    for collection in collections:
        try:
            db[collection].delete_many({})  # Deleta todos os documentos na coleção
            print(f"Todos os documentos foram deletados da coleção '{collection}'.")
        except Exception as e:
            print(f"Erro ao limpar a coleção '{collection}': {e}")
            
            
            