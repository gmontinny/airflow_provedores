from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.github.sensors.github import GithubSensor
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from typing import Any

# Variáveis sensíveis devem ser configuradas como variáveis de ambiente ou conexões no Airflow
GITHUB_TOKEN = "************************************"  # Substitua por uma conexão do Airflow
GITHUB_USER = "gmontinny"
GITHUB_REPO = "gmontinny/airflow"
YOUR_COMMIT_MESSAGE = "Where is the ISS right now?"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def commit_message_checker(repo: Any, trigger_message: str) -> bool | None:
    """Check the last 10 commits to a repository for a specific message.
    Args:
        repo (Any): The GitHub repository object.
        trigger_message (str): The commit message to look for.
    """

    result = None
    try:
        if repo is not None and trigger_message is not None:
            commits = repo.get_commits().get_page(0)[:10]
            for commit in commits:
                if trigger_message in commit.commit.message:
                    result = True
                    break
            else:
                result = False

    except Exception as e:
        raise AirflowException(f"GitHub operator error: {e}")
    return result

with DAG(
        "github_sensor_dag",
        default_args=default_args,
        description="DAG para monitorar repositório no Github usando GithubSensor",
        schedule_interval=timedelta(minutes=10),
        start_date=datetime(2023, 1, 1),  # Ajuste a data inicial
        catchup=False,
) as dag:
    start = DummyOperator(task_id="start")

    check_github_commit = GithubSensor(
        task_id="check_github_commit",
        github_conn_id="github_default",  # Configurar a conexão no Airflow
        timeout=60 * 10,  # Timeout total de 10 minutos
        poke_interval=60,  # Verifica o repositório a cada 60 segundos
        method_name="get_repo",
        method_params={"full_name_or_id": GITHUB_REPO},
        result_processor=lambda repo: commit_message_checker(repo, YOUR_COMMIT_MESSAGE)
    )


    end = DummyOperator(task_id="end")

    # Definindo a sequência das tarefas
    start >> check_github_commit >> end