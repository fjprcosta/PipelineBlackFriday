import os
import pandas as pd
from sqlalchemy import create_engine
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from dotenv import load_dotenv
from google.oauth2 import service_account

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

def baixar_dados_analytics():
    """Baixa dados do Google Analytics 4 e salva em um banco de dados SQL."""
    try:
        # Caminho para o arquivo de credenciais JSON
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_path:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS não encontrado nas variáveis de ambiente")
        print(f"Usando arquivo de credenciais: {credentials_path}")

        # Criar credenciais a partir do arquivo JSON
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )

        # Criar cliente com as credenciais explícitas
        client = BetaAnalyticsDataClient(credentials=credentials)

        # Obter o property_id do ambiente
        property_id = os.getenv("GA_PROPERTY_ID")
        if not property_id:
            raise ValueError("GA_PROPERTY_ID não encontrado nas variáveis de ambiente")

        # Usar uma data de início dinâmica (por exemplo, 30 dias atrás)
        from datetime import datetime, timedelta
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="city"),
                Dimension(name="country"),
                Dimension(name="deviceCategory"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
            ],
            metrics=[
                Metric(name="totalUsers"),
                Metric(name="newUsers"),
                Metric(name="activeUsers"),
                Metric(name="sessions"),
                Metric(name="engagedSessions"),
                Metric(name="averageSessionDuration"),
                Metric(name="screenPageViews"),
                Metric(name="conversions"),
                Metric(name="totalRevenue"),
            ],
            date_ranges=[DateRange(start_date=start_date, end_date="today")],
        )
        response = client.run_report(request)

        if not response.rows:
            print("Nenhum dado encontrado no Google Analytics para o período especificado.")
            return

        # Criar listas para armazenar os dados
        data = []
        header = [dim.name for dim in request.dimensions] + [metric.name for metric in request.metrics]

        # Preencher a lista de dados
        for row in response.rows:
            row_data = [dim_value.value for dim_value in row.dimension_values] + [metric_value.value for metric_value in row.metric_values]
            data.append(row_data)

        # Criar um DataFrame com os dados
        df = pd.DataFrame(data, columns=header)

        # Configuração da conexão com o banco de dados PostgreSQL
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL não encontrado nas variáveis de ambiente")

        engine = create_engine(db_url)

        # Salvar o DataFrame na tabela 'google_analytics_data' do banco de dados
        df.to_sql(name='google_analytics_data', con=engine, if_exists='replace', index=False)

        print("Dados do Google Analytics salvos no banco de dados PostgreSQL.")
    except Exception as e:
        print(f"Erro ao baixar e salvar dados do Google Analytics: {str(e)}")

if __name__ == "__main__":
    baixar_dados_analytics()