import json
import logging
import boto3
import pytest
import requests
from datetime import datetime
from moto import mock_aws
from unittest.mock import MagicMock
from unittest.mock import patch

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Fixture para carregar variaveis Airflow
@pytest.fixture(scope="session")
def variaveis_teste():
    variaveis_path = "/opt/airflow/tests/files/variables.json"
    with open(variaveis_path, "r") as f:
        return json.load(f)

# Fixture para dicionario de erros
@pytest.fixture(autouse=True)
def mock_mensagens_de_erro(variaveis_teste):
    with patch("script_utils.mensagens_de_erro") as mock_mensagem:
        mock_mensagem.return_value = {k.lower(): v for k, v in variaveis_teste["ERRORS_DICT"].items()}
        yield


# Fixture para variaveis do Airflow
@pytest.fixture(autouse=True)
def mock_airflow_variables(variaveis_teste):
    with patch("airflow.models.Variable.get") as mock_variable_get:
        mock_variable_get.side_effect = lambda key, default=None, deserialize_json=False: (
            variaveis_teste.get(key, default) if not deserialize_json
            else variaveis_teste.get(key, default)
        )
        yield

# Fixture para datetime
@pytest.fixture
def mock_datetime_fixo():
    with patch("script_worker_bronze.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime(2025, 7, 7, 10, 41, 9)
        mock_datetime.strftime = datetime.strftime
        yield mock_datetime


# Fixture para carregar arquivo da API
@pytest.fixture(scope="session")
def arquivo_teste():
    arquivo_path = "/opt/airflow/tests/files/raw_list_20250707_104109.json"
    with open(arquivo_path, "r") as f:
        return json.load(f)

# Fixture para airflow
@pytest.fixture
def mock_airflow(variaveis_teste):
    ti = MagicMock()
    xcom_store = {}

    def xcom_push(key, value):
        xcom_store[key] = value
        logger.debug(f"XCom push: key={key}, value={value}")

    # Retorna o valor armazenado em xcom_store para a chave fornecida, ou None se não existir
    def xcom_pull(task_ids=None, key=None):        
        return xcom_store.get(key, None)

    ti.xcom_push.side_effect = xcom_push
    ti.xcom_pull.side_effect = xcom_pull
    ti.task_id = "test_task"

    # Patch do Variable.get
    with patch("airflow.models.Variable.get") as mock_variable_get:
        mock_variable_get.side_effect = lambda key, default=None, deserialize_json=False: (
            variaveis_teste.get(key, default)
        )
        yield ti

# Fixture para S3 (bucket)
@pytest.fixture
def s3_mock(variaveis_teste):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=variaveis_teste["MAIN_BUCKET_NAME"])
        for folder in variaveis_teste["PROJECT_FOLDER_LIST"]:
            folder_path = f"{variaveis_teste["PROJECT_FOLDER_NAME"]}/{folder}/"
            s3.put_object(Bucket=variaveis_teste["MAIN_BUCKET_NAME"], Key=folder_path, Body="")
        yield s3


# Fixture resposta API sucesso
@pytest.fixture
def mock_api_response_successo(arquivo_teste):
    mock_response = MagicMock()
    mock_response.json.return_value = arquivo_teste
    mock_response.raise_for_status.return_value = None
    return mock_response

# Fixture resposta API invalida
@pytest.fixture
def mock_api_response_invalida():
    mock_response = MagicMock()
    mock_response.json.return_value = {"error": "Invalid response"}
    mock_response.raise_for_status.return_value = None
    return mock_response

# Fixture resposta API timeout
@pytest.fixture
def mock_api_timeout():
    # Retorna exceção que requests.get() deve levantar
    return requests.exceptions.Timeout("A requisicao excedeu o tempo limite.")


# Fixture para erro inesperado no json.dumps
@pytest.fixture
def mock_exception_json_dumps_error():
    with patch("json.dumps") as mock_json_dumps:
        mock_json_dumps.side_effect = Exception("Erro na serializacao JSON")
        yield mock_json_dumps
        
# Fixture para erro inesperado na conexão
@pytest.fixture
def mock_exception_s3_connection_error():
    with patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_conn") as mock_get_conn:
        mock_get_conn.side_effect = Exception("Erro de conexao.")
        yield mock_get_conn