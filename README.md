# PipelineBlackFriday
Projeto de ETL: Typeform e Google Analytics
Este projeto tem como objetivo realizar uma ETL (Extração, Transformação e Carga) completa, extraindo dados do Typeform e do Google Analytics.

Visão Geral
O projeto consiste em dois scripts principais:

typeform.py: Extrai dados de respostas do Typeform.
google_analytics.py: Extrai dados do Google Analytics 4.
Ambos os scripts extraem dados de suas respectivas fontes e os carregam em um formato estruturado para análise posterior.

Funcionalidades
Extração de Dados do Typeform
Conecta-se à API do Typeform usando credenciais armazenadas em variáveis de ambiente.
Recupera respostas de um formulário específico.
Formata os dados extraídos em uma estrutura consistente.
Salva os dados em um arquivo CSV.
Extração de Dados do Google Analytics
Utiliza a API do Google Analytics 4 para extrair métricas e dimensões específicas.
Coleta dados desde uma data de início definida até o dia atual.
Estrutura os dados em um DataFrame do pandas.
Carrega os dados em uma tabela de banco de dados PostgreSQL.
Configuração
Para executar este projeto, você precisará:

Configurar as variáveis de ambiente necessárias em um arquivo .env:

Para o Typeform: TYPEFORM_API_TOKEN e TYPEFORM_FORM_ID
Para o Google Analytics: GA_PROPERTY_ID, GA_START_DATE
Para o banco de dados: DATABASE_URL
Instalar as dependências necessárias (listadas em requirements.txt).

Configurar as credenciais do Google Analytics conforme a documentação da API.

Uso
Execute typeform.py para extrair dados do Typeform:

python typeform.py
Execute google_analytics.py para extrair dados do Google Analytics e carregá-los no banco de dados:

python google_analytics.py
Próximos Passos
Implementar a etapa de transformação dos dados.
Criar um pipeline de dados automatizado.
Desenvolver visualizações e dashboards com os dados extraídos.