import pytest
from unittest.mock import patch
import io
from script_utils import move_arquivo_processado
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def test_move_arquivo_processado_sucesso(s3_mock, variaveis_teste, mock_mensagens_de_erro, mock_airflow):
    """
    Testa sucesso ao mover arquivo.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    raw_folder = "raw.data"
    processed_folder = variaveis_teste["PROCESSED_FOLDER_NAME"]
    filename = "test.json"
    source_key = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{raw_folder}/{filename}"
    destination_key = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{processed_folder}/{filename}"
    file_content = b'{"id": 1, "name": "Test"}'

    # Cria objeto original no bucket
    s3_mock.put_object(Bucket=bucket, Key=source_key, Body=file_content)

    # Mock xcom_pull para simular execução manual
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        ("check_manual_execution", "work_type"): "manual",
        ("pega_info_arquivo_task", "bronze_filepath_manual"): source_key,
    }.get((task_ids, key), None)

    # Mock métodos do S3Hook
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)
        mp.setattr(S3Hook, "copy_object", lambda self, **kwargs: None)
        mp.setattr(S3Hook, "get_key", lambda self, key, bucket_name: type("obj", (), {
            "get": lambda self: {"Body": io.BytesIO(file_content)}
        })())
        mp.setattr(S3Hook, "delete_objects", lambda self, bucket, keys: None)

        # Act
        result = move_arquivo_processado(
            aws_conn_id="aws_default",
            bucket_name=bucket,
            processed_folder_name=processed_folder,
            ti=mock_airflow
        )

    # Assert
    assert result is True


def test_move_arquivo_processado_arquivo_nao_encontrado(s3_mock, variaveis_teste, mock_mensagens_de_erro, mock_airflow):
    """
    Testa falha quando o arquivo de origem não existe.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    raw_folder = "raw.data"
    processed_folder = variaveis_teste["PROCESSED_FOLDER_NAME"]
    filename = "test.json"
    source_key = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{raw_folder}/{filename}"

    # Mock xcom_pull para simular execução manual
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        ("check_manual_execution", "work_type"): "manual",
        ("pega_info_arquivo_task", "bronze_filepath_manual"): source_key,
    }.get((task_ids, key), None)

    # Mock S3Hook com arquivo de origem ausente
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: False)

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            move_arquivo_processado(
                aws_conn_id="aws_default",
                bucket_name=bucket,
                processed_folder_name=processed_folder,
                ti=mock_airflow
            )
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_FILE_NOTFOUND"]


def test_move_arquivo_processado_xcom_invalido(s3_mock, variaveis_teste, mock_mensagens_de_erro, mock_airflow):
    """
    Testa falha quando o XCom não fornece o caminho do arquivo.
    """   
    
    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    processed_folder = variaveis_teste["PROCESSED_FOLDER_NAME"]

    # Mock xcom_pull para retornar None
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        ("check_manual_execution", "work_type"): "manual",
        ("pega_info_arquivo_task", "bronze_filepath_manual"): None,
    }.get((task_ids, key), None)

    # Mock S3Hook
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            move_arquivo_processado(
                aws_conn_id="aws_default",
                bucket_name=bucket,
                processed_folder_name=processed_folder,
                ti=mock_airflow
            )
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_XCOM_INFO"]


def test_move_arquivo_processado_erro_inesperado(s3_mock, variaveis_teste, mock_mensagens_de_erro, mock_airflow):
    """
    Testa erro inesperado ao mover arquivo.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    raw_folder = "raw.data"
    processed_folder = variaveis_teste["PROCESSED_FOLDER_NAME"]
    filename = "test.json"
    source_key = f"{variaveis_teste['PROJECT_FOLDER_NAME']}/{raw_folder}/{filename}"
    file_content = b'{"id": 1, "name": "Test"}'

    # Cria objeto original no bucket
    s3_mock.put_object(Bucket=bucket, Key=source_key, Body=file_content)

    # Mock xcom_pull para simular execução manual
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        ("check_manual_execution", "work_type"): "manual",
        ("pega_info_arquivo_task", "bronze_filepath_manual"): source_key,
    }.get((task_ids, key), None)

    # Mock S3Hook com exceção ao copiar
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)
        mp.setattr(S3Hook, "copy_object", lambda self, **kwargs: (_ for _ in ()).throw(Exception("Erro de conexão S3")))
        mp.setattr(S3Hook, "get_key", lambda self, key, bucket_name: type("obj", (), {
            "get": lambda self: {"Body": io.BytesIO(file_content)}
        })())
        mp.setattr(S3Hook, "delete_objects", lambda self, bucket, keys: None)

        # Act & Assert
        with pytest.raises(Exception) as exc:
            move_arquivo_processado(
                aws_conn_id="aws_default",
                bucket_name=bucket,
                processed_folder_name=processed_folder,
                ti=mock_airflow
            )
        
