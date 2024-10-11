import os
from dotenv import load_dotenv
import requests
import pandas as pd 
from pprint import pprint
api_token = os.getenv('TYPEFORM_API_TOKEN')
form_id = os.getenv('TYPEFORM_FORM_ID')

url = f"https://api.typeform.com/forms/{form_id}/responses"
headers = {"Authorization": f"Bearer {api_token}"}
params = {'page_size': 100}

response = requests.get(url, headers=headers, params=params)

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
    
    # Salva o DataFrame em um arquivo CSV
    csv_filename = 'typeform_responses.csv'
    df.to_csv(csv_filename, index=False)
    
    print(f"As respostas foram salvas em '{csv_filename}'")
else:
    print(f"Erro ao obter as respostas: {response.status_code}")