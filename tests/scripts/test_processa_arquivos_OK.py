import pytest
import pandas as pd
import json
from unittest.mock import patch
from script_worker_prata import processa_arquivos
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def test_processa_arquivos_sucesso_manual(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa sucesso na execução manual.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    work_folder = variaveis_teste["CLEANDATA_FOLDER_NAME"]
    json_list = variaveis_teste["JSON_FIELDS_LIST"]
    df_datatype = variaveis_teste["DF_DATATYPE_SCHEMA"]
    timestamp = "20250707_104109"
    bronze_filepath = f"{project_folder}/raw.data/raw_list_{timestamp}.json"
    parquet_path = f"{project_folder}/{work_folder}/df_validado_{timestamp}.parquet"

    # Dados de teste
    test_data = [
        {"id": "123", "name": "Brewery A", "country": "Brazil", "state": "SP"},
        {"id": "124", "name": "Brewery B", "country": "Brazil", "state": "RJ"}
    ]
    s3_mock.put_object(Bucket=bucket, Key=bronze_filepath, Body=json.dumps(test_data))

    # Configura XCom
    mock_airflow.xcom_pull.return_value = "manual"  # work_type
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        "check_manual_execution": {"work_type": "manual"},
        "pega_info_arquivo_task": {
            "bronze_filepath_manual": bronze_filepath,
            "timestamp_nome_arquivo": timestamp
        }
    }.get(task_ids, {}).get(key)

    # Mock S3Hook e salva_parquet
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: key == bronze_filepath)
        mp.setattr(S3Hook, "read_key", lambda self, key, bucket_name: json.dumps(test_data))
        mp.setattr("script_worker_prata.salva_parquet", lambda *args, **kwargs: True)

        # Act
        result = processa_arquivos(
            aws_conn_id="aws_default",
            source_bucket_name=bucket,
            project_folder_name=project_folder,
            work_folder=work_folder,
            json_list=json_list,
            df_datatype=df_datatype,
            ti=mock_airflow
        )

    # Assert
    assert result is True
    mock_airflow.xcom_push.assert_called_with(key="silver_filepath", value=parquet_path)


def test_processa_arquivos_sucesso_agendada(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa sucesso na execução agendada.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    work_folder = variaveis_teste["CLEANDATA_FOLDER_NAME"]
    json_list = variaveis_teste["JSON_FIELDS_LIST"]
    df_datatype = variaveis_teste["DF_DATATYPE_SCHEMA"]
    timestamp = "20250707_104109"
    bronze_filepath = f"{project_folder}/raw.data/raw_list_{timestamp}.json"
    parquet_path = f"{project_folder}/{work_folder}/df_validado_{timestamp}.parquet"

    # Dados de teste
    test_data = [
        {"id": "123", "name": "Brewery A", "country": "Brazil", "state": "SP"},
        {"id": "124", "name": "Brewery B", "country": "Brazil", "state": "RJ"}
    ]
    s3_mock.put_object(Bucket=bucket, Key=bronze_filepath, Body=json.dumps(test_data))

    # Configura XCom
    mock_airflow.xcom_pull.return_value = "agendada"  # work_type
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        "check_manual_execution": {"work_type": "agendada"},
        "salva_dados_api_task": {
            "bronze_filepath_api": bronze_filepath,
            "timestamp_api": timestamp
        }
    }.get(task_ids, {}).get(key)

    # Mock S3Hook e salva_parquet
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: key == bronze_filepath)
        mp.setattr(S3Hook, "read_key", lambda self, key, bucket_name: json.dumps(test_data))
        mp.setattr("script_worker_prata.salva_parquet", lambda *args, **kwargs: True)

        # Act
        result = processa_arquivos(
            aws_conn_id="aws_default",
            source_bucket_name=bucket,
            project_folder_name=project_folder,
            work_folder=work_folder,
            json_list=json_list,
            df_datatype=df_datatype,
            ti=mock_airflow
        )

    # Assert
    assert result is True
    mock_airflow.xcom_push.assert_called_with(key="silver_filepath", value=parquet_path)


def test_processa_arquivos_xcom_invalido(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa falha por XCom inválido.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    work_folder = variaveis_teste["CLEANDATA_FOLDER_NAME"]
    json_list = variaveis_teste["JSON_FIELDS_LIST"]
    df_datatype = variaveis_teste["DF_DATATYPE_SCHEMA"]

    # Configura XCom
    mock_airflow.xcom_pull.return_value = None

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            processa_arquivos(
                aws_conn_id="aws_default",
                source_bucket_name=bucket,
                project_folder_name=project_folder,
                work_folder=work_folder,
                json_list=json_list,
                df_datatype=df_datatype,
                ti=mock_airflow
            )
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_XCOM_INFO"]


def test_processa_arquivos_arquivo_nao_encontrado(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa falha por arquivo não encontrado.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    work_folder = variaveis_teste["CLEANDATA_FOLDER_NAME"]
    json_list = variaveis_teste["JSON_FIELDS_LIST"]
    df_datatype = variaveis_teste["DF_DATATYPE_SCHEMA"]
    timestamp = "20250707_104109"
    bronze_filepath = f"{project_folder}/raw.data/raw_list_{timestamp}.json"

    # Configura XCom
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        "check_manual_execution": {"work_type": "manual"},
        "pega_info_arquivo_task": {
            "bronze_filepath_manual": bronze_filepath,
            "timestamp_nome_arquivo": timestamp
        }
    }.get(task_ids, {}).get(key)

    # Mock S3Hook para simular arquivo ausente
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: False)

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            processa_arquivos(
                aws_conn_id="aws_default",
                source_bucket_name=bucket,
                project_folder_name=project_folder,
                work_folder=work_folder,
                json_list=json_list,
                df_datatype=df_datatype,
                ti=mock_airflow
            )
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_FILE_NOTFOUND"]


def test_processa_arquivos_arquivo_vazio(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa falha por arquivo vazio.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    work_folder = variaveis_teste["CLEANDATA_FOLDER_NAME"]
    json_list = variaveis_teste["JSON_FIELDS_LIST"]
    df_datatype = variaveis_teste["DF_DATATYPE_SCHEMA"]
    timestamp = "20250707_104109"
    bronze_filepath = f"{project_folder}/raw.data/raw_list_{timestamp}.json"

    # Configura XCom
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        "check_manual_execution": {"work_type": "manual"},
        "pega_info_arquivo_task": {
            "bronze_filepath_manual": bronze_filepath,
            "timestamp_nome_arquivo": timestamp
        }
    }.get(task_ids, {}).get(key)

    # Mock S3Hook com arquivo vazio
    s3_mock.put_object(Bucket=bucket, Key=bronze_filepath, Body="")
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)
        mp.setattr(S3Hook, "read_key", lambda self, key, bucket_name: "")

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            processa_arquivos(
                aws_conn_id="aws_default",
                source_bucket_name=bucket,
                project_folder_name=project_folder,
                work_folder=work_folder,
                json_list=json_list,
                df_datatype=df_datatype,
                ti=mock_airflow
            )
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_EMPTY_FILE"]


def test_processa_arquivos_conteudo_invalido(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa falha por conteúdo inválido.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    work_folder = variaveis_teste["CLEANDATA_FOLDER_NAME"]
    json_list = variaveis_teste["JSON_FIELDS_LIST"]
    df_datatype = variaveis_teste["DF_DATATYPE_SCHEMA"]
    timestamp = "20250707_104109"
    bronze_filepath = f"{project_folder}/raw.data/raw_list_{timestamp}.json"

    # Configura XCom
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        "check_manual_execution": {"work_type": "manual"},
        "pega_info_arquivo_task": {
            "bronze_filepath_manual": bronze_filepath,
            "timestamp_nome_arquivo": timestamp
        }
    }.get(task_ids, {}).get(key)

    # Mock S3Hook com conteúdo inválido
    s3_mock.put_object(Bucket=bucket, Key=bronze_filepath, Body=json.dumps({"error": "invalid"}))
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)
        mp.setattr(S3Hook, "read_key", lambda self, key, bucket_name: json.dumps({"error": "invalid"}))

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            processa_arquivos(
                aws_conn_id="aws_default",
                source_bucket_name=bucket,
                project_folder_name=project_folder,
                work_folder=work_folder,
                json_list=json_list,
                df_datatype=df_datatype,
                ti=mock_airflow
            )
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_CHECK_FILE_CONTENT"]


def test_processa_arquivos_falha_salvar_parquet(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa falha ao salvar Parquet.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    work_folder = variaveis_teste["CLEANDATA_FOLDER_NAME"]
    json_list = variaveis_teste["JSON_FIELDS_LIST"]
    df_datatype = variaveis_teste["DF_DATATYPE_SCHEMA"]
    timestamp = "20250707_104109"
    bronze_filepath = f"{project_folder}/raw.data/raw_list_{timestamp}.json"

    # Dados de teste
    test_data = [
        {"id": "123", "name": "Brewery A", "country": "Brazil", "state": "SP"}
    ]
    s3_mock.put_object(Bucket=bucket, Key=bronze_filepath, Body=json.dumps(test_data))

    # Configura XCom
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        "check_manual_execution": {"work_type": "manual"},
        "pega_info_arquivo_task": {
            "bronze_filepath_manual": bronze_filepath,
            "timestamp_nome_arquivo": timestamp
        }
    }.get(task_ids, {}).get(key)

    # Mock S3Hook e simula falha em salva_parquet
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)
        mp.setattr(S3Hook, "read_key", lambda self, key, bucket_name: json.dumps(test_data))
        mp.setattr("script_worker_prata.salva_parquet", lambda *args, **kwargs: False)

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            processa_arquivos(
                aws_conn_id="aws_default",
                source_bucket_name=bucket,
                project_folder_name=project_folder,
                work_folder=work_folder,
                json_list=json_list,
                df_datatype=df_datatype,
                ti=mock_airflow
            )
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_SAVING_FILE"]


def test_processa_arquivos_erro_inesperado(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa erro inesperado.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    work_folder = variaveis_teste["CLEANDATA_FOLDER_NAME"]
    json_list = variaveis_teste["JSON_FIELDS_LIST"]
    df_datatype = variaveis_teste["DF_DATATYPE_SCHEMA"]
    timestamp = "20250707_104109"
    bronze_filepath = f"{project_folder}/raw.data/raw_list_{timestamp}.json"

    # Configura XCom
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        "check_manual_execution": {"work_type": "manual"},
        "pega_info_arquivo_task": {
            "bronze_filepath_manual": bronze_filepath,
            "timestamp_nome_arquivo": timestamp
        }
    }.get(task_ids, {}).get(key)

    # Mock S3Hook com conteúdo inválido para json.loads
    s3_mock.put_object(Bucket=bucket, Key=bronze_filepath, Body="invalid_json")
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)
        mp.setattr(S3Hook, "read_key", lambda self, key, bucket_name: "invalid_json")

        # Act & Assert
        with pytest.raises(Exception):
            processa_arquivos(
                aws_conn_id="aws_default",
                source_bucket_name=bucket,
                project_folder_name=project_folder,
                work_folder=work_folder,
                json_list=json_list,
                df_datatype=df_datatype,
                ti=mock_airflow
            )
