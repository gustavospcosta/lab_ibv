import pytest
import json
from script_utils import pega_info_arquivo
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError
from unittest.mock import MagicMock
import logging

logger = logging.getLogger(__name__)

def test_pega_info_arquivo_sucesso(s3_mock, mock_airflow, mock_mensagens_de_erro, arquivo_teste, variaveis_teste):
    """
    Testa o sucesso da função pega_info_arquivo com um arquivo JSON válido no S3.
    """

    # Configura o caminho do arquivo manual
    manual_file_path = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{variaveis_teste['RAWDATA_FOLDER_NAME']}/raw_list_20250707_104109.json"
    
    with pytest.MonkeyPatch.context() as mp:
        # Simula a variável Airflow MANUAL_FILE_PATH
        mp.setattr("airflow.models.Variable.get", lambda key, default=None: manual_file_path if key == "MANUAL_FILE_PATH" else default)

        # Faz upload do conteúdo da fixture arquivo_teste para o S3 simulado
        s3_mock.put_object(
            Bucket=variaveis_teste["MAIN_BUCKET_NAME"],
            Key=manual_file_path,
            Body=json.dumps(arquivo_teste, ensure_ascii=False)
        )

        # Executa a função
        result = pega_info_arquivo(
            aws_conn_id=variaveis_teste["AWS_CONN_ID"],
            bucket_name=variaveis_teste["MAIN_BUCKET_NAME"],
            ti=mock_airflow
        )

        # Verifica o resultado
        assert result is True
        assert mock_airflow.xcom_push.called
        # O primeiro xcom_push deve conter o bronze_filepath_manual correto
        assert mock_airflow.xcom_push.call_args_list[0].kwargs == {
            "key": "bronze_filepath_manual",
            "value": manual_file_path
        }
        # O segundo xcom_push deve conter o timestamp_nome_arquivo correto
        assert mock_airflow.xcom_push.call_args_list[1].kwargs == {
            "key": "timestamp_nome_arquivo",
            "value": "20250707_104109"
        }

def test_pega_info_arquivo_variavel_nao_encontrada(mock_airflow, variaveis_teste, monkeypatch):
    """
    Testa falha quando a variável MANUAL_FILE_PATH não existe.
    """
    # Simula KeyError para Variable.get
    monkeypatch.setattr("airflow.models.Variable.get", lambda key, default=None: (_ for _ in ()).throw(KeyError("MANUAL_FILE_PATH")))

    with pytest.raises(ValueError) as exc_info:
        pega_info_arquivo(
            aws_conn_id="aws_default",
            bucket_name="test-bucket",
            ti=mock_airflow
        )
    
    assert str(exc_info.value) == variaveis_teste["ERRORS_DICT"]["ERROR_VARIAVEL_NOTFOUND"]


def test_pega_info_arquivo_path_invalido(s3_mock, mock_airflow, mock_mensagens_de_erro, variaveis_teste):
    """
    Testa falha quando o caminho do arquivo não segue o padrão esperado.
    """
    invalid_path = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{variaveis_teste['RAWDATA_FOLDER_NAME']}/invalid_file.json"
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("airflow.models.Variable.get", lambda key, default=None: invalid_path if key == "MANUAL_FILE_PATH" else default)

        with pytest.raises(ValueError) as exc_info:
            pega_info_arquivo(
                aws_conn_id=variaveis_teste["AWS_CONN_ID"],
                bucket_name=variaveis_teste["MAIN_BUCKET_NAME"],
                ti=mock_airflow
            )
    
        assert str(exc_info.value) == variaveis_teste["ERRORS_DICT"]["ERROR_VALIDATION_FAIL"]


def test_pega_info_arquivo_arquivo_nao_encontrado(s3_mock, mock_airflow, mock_mensagens_de_erro, variaveis_teste):
    """
    Testa falha quando o arquivo não existe no S3.
    """
    manual_file_path = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{variaveis_teste['RAWDATA_FOLDER_NAME']}/raw_list_20250707_104109.json"
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("airflow.models.Variable.get", lambda key, default=None: manual_file_path if key == "MANUAL_FILE_PATH" else default)

        with pytest.raises(ValueError) as exc_info:
            pega_info_arquivo(
                aws_conn_id=variaveis_teste["AWS_CONN_ID"],
                bucket_name=variaveis_teste["MAIN_BUCKET_NAME"],
                ti=mock_airflow
            )
    
    assert str(exc_info.value) == variaveis_teste["ERRORS_DICT"]["ERROR_FILE_NOTFOUND"]
    

def test_pega_info_arquivo_arquivo_vazio(s3_mock, mock_airflow, mock_mensagens_de_erro, variaveis_teste):
    """
    Testa falha quando o arquivo está vazio (0 bytes).
    """
    manual_file_path = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{variaveis_teste['RAWDATA_FOLDER_NAME']}/raw_list_20250707_104109.json"
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("airflow.models.Variable.get", lambda key, default=None: manual_file_path if key == "MANUAL_FILE_PATH" else default)

        # Faz upload de um arquivo vazio
        s3_mock.put_object(
            Bucket=variaveis_teste["MAIN_BUCKET_NAME"],
            Key=manual_file_path,
            Body=""
        )

        with pytest.raises(ValueError) as exc_info:
            pega_info_arquivo(
                aws_conn_id=variaveis_teste["AWS_CONN_ID"],
                bucket_name=variaveis_teste["MAIN_BUCKET_NAME"],
                ti=mock_airflow
            )
    
    assert str(exc_info.value) == variaveis_teste["ERRORS_DICT"]["ERROR_EMPTY_FILE"]


def test_pega_info_arquivo_conteudo_invalido(s3_mock, mock_airflow, mock_mensagens_de_erro, variaveis_teste):
    """
    Testa falha quando o conteúdo do arquivo não é uma lista JSON válida.
    """
    manual_file_path = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{variaveis_teste['RAWDATA_FOLDER_NAME']}/raw_list_20250707_104109.json"
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("airflow.models.Variable.get", lambda key, default=None: manual_file_path if key == "MANUAL_FILE_PATH" else default)

        # Faz upload de um arquivo com conteúdo JSON inválido (não uma lista)
        s3_mock.put_object(
            Bucket=variaveis_teste["MAIN_BUCKET_NAME"],
            Key=manual_file_path,
            Body=json.dumps({"error": "not a list"})
        )

        with pytest.raises(ValueError) as exc_info:
            pega_info_arquivo(
                aws_conn_id=variaveis_teste["AWS_CONN_ID"],
                bucket_name=variaveis_teste["MAIN_BUCKET_NAME"],
                ti=mock_airflow
            )
    
    assert str(exc_info.value) == variaveis_teste["ERRORS_DICT"]["ERROR_CHECK_FILE_CONTENT"]


def test_pega_info_arquivo_erro_arquivo_nao_encontrado(mock_airflow, mock_mensagens_de_erro, variaveis_teste, monkeypatch):
    """
    Testa falha quando o arquivo não é encontrado no S3 (check_for_key retorna False).
    """

    manual_file_path = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{variaveis_teste['RAWDATA_FOLDER_NAME']}/raw_list_20250707_104109.json"

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("airflow.models.Variable.get", lambda key, default=None: manual_file_path if key == "MANUAL_FILE_PATH" else default)

        # Simula arquivo não encontrado: check_for_key retorna False
        monkeypatch.setattr(S3Hook, "check_for_key", lambda *args, **kwargs: False)

        with pytest.raises(ValueError) as exc_info:
            pega_info_arquivo(
                aws_conn_id=variaveis_teste["AWS_CONN_ID"],
                bucket_name=variaveis_teste["MAIN_BUCKET_NAME"],
                ti=mock_airflow
            )

    assert str(exc_info.value) == variaveis_teste["ERRORS_DICT"]["ERROR_FILE_NOTFOUND"]

