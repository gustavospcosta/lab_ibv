import pytest
import io
from unittest.mock import patch
from script_utils import deleta_arquivo_processado
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def test_deleta_arquivo_processado_sucesso_manual(s3_mock, variaveis_teste, mock_mensagens_de_erro, mock_airflow):
    """
    Testa sucesso ao deletar arquivo na execução manual.
    """
    
    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    raw_folder = "raw.data"
    filename = "test.json"
    source_key = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{raw_folder}/{filename}"
    file_content = b'{"id": 1, "name": "Test"}'

    # Cria objeto original no bucket
    s3_mock.put_object(Bucket=bucket, Key=source_key, Body=file_content)

    # Mock xcom_pull para simular execução manual e silver_filepath
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        ("check_manual_execution", "work_type"): "manual",
        ("processa_arquivos_task", "silver_filepath"): source_key,
    }.get((task_ids, key), None)

    # Mock métodos do S3Hook
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: key == source_key)
        mp.setattr(S3Hook, "delete_objects", lambda self, bucket, keys: None)

        # Act
        result = deleta_arquivo_processado(
            aws_conn_id="aws_default",
            bucket_name=bucket,
            ti=mock_airflow
        )

    # Assert
    assert result is True
    response = s3_mock.list_objects_v2(Bucket=bucket)
    keys = [item["Key"] for item in response.get("Contents", [])]
    assert source_key not in keys


def test_deleta_arquivo_processado_sucesso_agendada(s3_mock, variaveis_teste, mock_mensagens_de_erro, mock_airflow):
    """
    Testa sucesso ao deletar arquivo na execução agendada.
    """
    
    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    raw_folder = "raw.data"
    filename = "test.json"
    source_key = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{raw_folder}/{filename}"
    file_content = b'{"id": 1, "name": "Test"}'

    # Cria objeto original no bucket
    s3_mock.put_object(Bucket=bucket, Key=source_key, Body=file_content)

    # Mock xcom_pull para simular execução agendada e silver_filepath
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        ("check_manual_execution", "work_type"): "agendada",
        ("processa_arquivos_task", "silver_filepath"): source_key,
    }.get((task_ids, key), None)

    # Mock métodos do S3Hook
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: key == source_key)

        # Act
        result = deleta_arquivo_processado(
            aws_conn_id="aws_default",
            bucket_name=bucket,
            ti=mock_airflow
        )

    # Assert
    assert result is True
    response = s3_mock.list_objects_v2(Bucket=bucket)
    keys = [item["Key"] for item in response.get("Contents", [])]
    assert source_key not in keys


def test_deleta_arquivo_processado_arquivo_nao_encontrado(s3_mock, variaveis_teste, mock_mensagens_de_erro, mock_airflow):
    """
    Testa falha quando o arquivo não existe.
    """
    
    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    raw_folder = "raw.data"
    filename = "test.json"
    source_key = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{raw_folder}/{filename}"

    # Mock xcom_pull para simular execução manual e silver_filepath
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        ("check_manual_execution", "work_type"): "manual",
        ("processa_arquivos_task", "silver_filepath"): source_key,
    }.get((task_ids, key), None)

    # Mock S3Hook com arquivo ausente
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: False)

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            deleta_arquivo_processado(
                aws_conn_id="aws_default",
                bucket_name=bucket,
                ti=mock_airflow
            )
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_FILE_NOTFOUND"]


def test_deleta_arquivo_processado_xcom_invalido(s3_mock, variaveis_teste, mock_mensagens_de_erro, mock_airflow):
    """
    Testa falha quando o XCom não fornece o caminho do arquivo.
    """
        
    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]

    # Mock xcom_pull para retornar None
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        ("check_manual_execution", "work_type"): "manual",
        ("processa_arquivos_task", "silver_filepath"): None,
    }.get((task_ids, key), None)

    # Mock S3Hook
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            deleta_arquivo_processado(
                aws_conn_id="aws_default",
                bucket_name=bucket,
                ti=mock_airflow
            )
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_XCOM_INFO"]


def test_deleta_arquivo_processado_erro_inesperado(s3_mock, variaveis_teste, mock_mensagens_de_erro, mock_airflow):
    """
    Testa erro inesperado ao deletar arquivo.
    """
    
    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    raw_folder = "raw.data"
    filename = "test.json"
    source_key = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{raw_folder}/{filename}"
    file_content = b'{"id": 1, "name": "Test"}'

    # Cria objeto original no bucket
    s3_mock.put_object(Bucket=bucket, Key=source_key, Body=file_content)

    # Mock xcom_pull para simular execução manual e silver_filepath
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        ("check_manual_execution", "work_type"): "manual",
        ("processa_arquivos_task", "silver_filepath"): source_key,
    }.get((task_ids, key), None)

    # Mock S3Hook e erro no delete_object
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: key == source_key)

        # Mock delete_object do cliente boto3 para lançar exceção
        def raise_exception(*args, **kwargs):
            raise Exception("Erro de conexao.")

        s3_mock.delete_object = raise_exception

        # Act & Assert
        with pytest.raises(Exception) as exc:
            deleta_arquivo_processado(
                aws_conn_id="aws_default",
                bucket_name=bucket,
                ti=mock_airflow
            )

        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_CONEXAO"]

