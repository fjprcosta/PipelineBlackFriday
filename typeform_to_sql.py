import os
from dotenv import load_dotenv
import requests
import pandas as pd 
from sqlalchemy import create_engine

# Adicione esta linha no início do script para carregar as variáveis de ambiente
load_dotenv()

api_token = os.getenv('TYPEFORM_API_TOKEN')
form_id = os.getenv('TYPEFORM_FORM_ID')

url = f"https://api.typeform.com/forms/{form_id}/responses"
headers = {"Authorization": f"Bearer {api_token}"}
params = {'page_size': 100}

# Adicione verificações para as variáveis de ambiente
if not api_token or not form_id:
    print("Erro: TYPEFORM_API_TOKEN ou TYPEFORM_FORM_ID não estão definidos no arquivo .env")
    exit(1)

# Adicione um tratamento de exceção para a requisição
try:
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Isso levantará uma exceção para códigos de status HTTP ruins
except requests.exceptions.RequestException as e:
    print(f"Erro ao fazer a requisição: {e}")
    exit(1)

# Verifica se a requisição foi bem-sucedida
if response.status_code == 200:
    # Obtém os dados da resposta
    data = response.json()
    
    # Extrai as respostas
    responses = data['items']
    
    # Cria uma lista para armazenar os dados formatados
    formatted_responses = []
    
    for resp in responses:
        formatted_resp = {
            'submitted_at': resp['submitted_at'],
            'response_id': resp['response_id']
        }
        
        # Adiciona as respostas de cada pergunta
        for answer in resp['answers']:
            question_id = answer['field']['id']
            question_type = answer['type']
            
            if question_type in ['text', 'number', 'date']:
                formatted_resp[question_id] = answer[question_type]
            elif question_type == 'choice':
                formatted_resp[question_id] = answer['choice']['label']
            elif question_type == 'choices':
                formatted_resp[question_id] = ', '.join([choice['label'] for choice in answer['choices']['labels']])
    
        formatted_responses.append(formatted_resp)
    
    # Cria um DataFrame com as respostas formatadas
    df = pd.DataFrame(formatted_responses)
    
    # Configuração da conexão com o banco de dados PostgreSQL
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    
    # Salva o DataFrame na tabela SQL
    table_name = 'typeform_responses'
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    
    print(f"As respostas foram salvas na tabela '{table_name}' do banco de dados.")
else:
    print(f"Erro ao obter as respostas: {response.status_code}")
    exit(1)