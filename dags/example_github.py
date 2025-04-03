from airflow import DAG
from datetime import datetime, timedelta
import requests
from airflow.operators.python import PythonOperator

# Substituir onde necessário
start_date = datetime(2023, 10, 1)
retry_delay = timedelta(minutes=5)  # timedelta já é acessível sem precisar do módulo completo


# Função para testar a conexão no GitHub
def test_github_connection():
    url = "https://api.github.com/repos/gmontinny/airflow_provedores"
    token = "*************************************"

    try:
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json',
        }

        response = requests.get(url, headers=headers)

        # Testa se o status code indica sucesso
        if response.status_code == 200:
            print("Conexão bem-sucedida! Repositório acessado com sucesso.")
        else:
            print(f"Falha na conexão. Código de status: {response.status_code}")
    except Exception as e:
        print(f"Erro ao testar a conexão: {e}")


# Definição da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': retry_delay,
}

with DAG(
        'test_github_connection_dag',
        default_args=default_args,
        description='DAG para testar conexão com o GitHub',
        schedule_interval=None,
        start_date=start_date,
        catchup=False,
) as dag:
    test_connection = PythonOperator(
        task_id='test_github_connection',
        python_callable=test_github_connection
    )

    test_connection