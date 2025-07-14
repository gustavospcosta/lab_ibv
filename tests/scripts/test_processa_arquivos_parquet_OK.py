import pytest
import pandas as pd
import io
from unittest.mock import patch
from script_worker_gold import processa_arquivos_parquet
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def test_processa_arquivos_parquet_sucesso_manual(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa sucesso na execução manual.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    aggregated_folder = variaveis_teste["AGGREGATED_FOLDER_NAME"]
    timestamp = "20250707_104109"
    silver_filepath = f"{project_folder}/cleandata/df_validado_{timestamp}.parquet"
    parquet_key_01 = f"{project_folder}/{aggregated_folder}/df_aggregated.01_{timestamp}.parquet"
    parquet_key_02 = f"{project_folder}/{aggregated_folder}/df_aggregated.02_{timestamp}.parquet"

    # Dados de teste
    test_data = pd.DataFrame({
        "id": ["123", "124"],
        "name": ["Brewery A", "Brewery B"],
        "country": ["Brazil", "Brazil"],
        "state": ["SP", "RJ"],
        "brewery_type": ["micro", "macro"]
    })
    buffer = io.BytesIO()
    test_data.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_mock.put_object(Bucket=bucket, Key=silver_filepath, Body=buffer.getvalue())

    # Configura XCom
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        "check_manual_execution": {"work_type": "manual"},
        "pega_info_arquivo_task": {"timestamp_nome_arquivo": timestamp},
        "processa_arquivos_task": {"silver_filepath": silver_filepath}
    }.get(task_ids, {}).get(key)

    # Mock S3Hook e salva_parquet
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: key == silver_filepath)
        mp.setattr(S3Hook, "get_key", lambda self, key, bucket_name: type("obj", (), {
            "download_fileobj": lambda self, fileobj: fileobj.write(buffer.getvalue())  # Adiciona 'self'
        })())
        mp.setattr("script_worker_gold.salva_parquet", lambda *args, **kwargs: True)

        # Act
        result = processa_arquivos_parquet(
            aws_conn_id="aws_default",
            source_bucket_name=bucket,
            project_folder_name=project_folder,
            aggregated_folder_name=aggregated_folder,
            ti=mock_airflow
        )

    # Assert
    assert result is True


def test_processa_arquivos_parquet_sucesso_agendada(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa sucesso na execução agendada.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    aggregated_folder = variaveis_teste["AGGREGATED_FOLDER_NAME"]
    timestamp = "20250707_104109"
    silver_filepath = f"{project_folder}/cleandata/df_validado_{timestamp}.parquet"
    parquet_key_01 = f"{project_folder}/{aggregated_folder}/df_aggregated.01_{timestamp}.parquet"
    parquet_key_02 = f"{project_folder}/{aggregated_folder}/df_aggregated.02_{timestamp}.parquet"

    # Dados de teste
    test_data = pd.DataFrame({
        "id": ["123", "124"],
        "name": ["Brewery A", "Brewery B"],
        "country": ["Brazil", "Brazil"],
        "state": ["SP", "RJ"],
        "brewery_type": ["micro", "macro"]
    })
    buffer = io.BytesIO()
    test_data.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_mock.put_object(Bucket=bucket, Key=silver_filepath, Body=buffer.getvalue())

    # Configura XCom
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        ("check_manual_execution", "work_type"): "agendada",
        ("salva_dados_api_task", "timestamp_api"): timestamp,
        ("processa_arquivos_task", "silver_filepath"): silver_filepath
    }.get((task_ids, key))

    # Mock S3Hook e salva_parquet
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: key == silver_filepath)
        mp.setattr(S3Hook, "get_key", lambda self, key, bucket_name: type("obj", (), {
            "download_fileobj": lambda self, fileobj: fileobj.write(buffer.getvalue())
        })())
        mp.setattr("script_worker_gold.salva_parquet", lambda *args, **kwargs: True)

        # Act
        result = processa_arquivos_parquet(
            aws_conn_id="aws_default",
            source_bucket_name=bucket,
            project_folder_name=project_folder,
            aggregated_folder_name=aggregated_folder,
            ti=mock_airflow
        )

    # Assert
    assert result is True


def test_processa_arquivos_parquet_xcom_invalido(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa falha por XCom inválido.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    aggregated_folder = variaveis_teste["AGGREGATED_FOLDER_NAME"]

    # Configura XCom para retornar None
    mock_airflow.xcom_pull.return_value = None

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            processa_arquivos_parquet(
                aws_conn_id="awstum",
                source_bucket_name=bucket,
                project_folder_name=project_folder,
                aggregated_folder_name=aggregated_folder,
                ti=mock_airflow
            )
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_XCOM_INFO"]


def test_processa_arquivos_parquet_arquivo_nao_encontrado(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa falha por arquivo não encontrado.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    aggregated_folder = variaveis_teste["AGGREGATED_FOLDER_NAME"]
    timestamp = "20250707_104109"
    silver_filepath = f"{project_folder}/cleandata/df_validado_{timestamp}.parquet"

    # Configura XCom
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        "check_manual_execution": {"work_type": "manual"},
        "pega_info_arquivo_task": {"timestamp_nome_arquivo": timestamp},
        "processa_arquivos_task": {"silver_filepath": silver_filepath}
    }.get(task_ids, {}).get(key)

    # Mock S3Hook para simular arquivo ausente
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: False)

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            processa_arquivos_parquet(
                aws_conn_id="aws_default",
                source_bucket_name=bucket,
                project_folder_name=project_folder,
                aggregated_folder_name=aggregated_folder,
                ti=mock_airflow
            )
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_FILE_NOTFOUND"]



def test_processa_arquivos_parquet_falha_salvar_parquet_01(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa falha ao salvar o primeiro Parquet.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    aggregated_folder = variaveis_teste["AGGREGATED_FOLDER_NAME"]
    timestamp = "20250707_104109"
    silver_filepath = f"{project_folder}/cleandata/df_validado_{timestamp}.parquet"

    # Dados de teste (DataFrame Silver)
    test_data = pd.DataFrame({
        "id": ["123"],
        "name": ["Brewery A"],
        "country": ["Brazil"],
        "state": ["SP"],
        "brewery_type": ["micro"]
    })

    # Salva no bucket simulado
    buffer = io.BytesIO()
    test_data.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_mock.put_object(Bucket=bucket, Key=silver_filepath, Body=buffer.getvalue())

    # Configura XCom conforme esperado pela DAG
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        ("check_manual_execution", "work_type"): "manual",
        ("pega_info_arquivo_task", "timestamp_nome_arquivo"): timestamp,
        ("processa_arquivos_task", "silver_filepath"): silver_filepath
    }.get((task_ids, key))

    # Mocks do S3 e salva_parquet
    with pytest.MonkeyPatch().context() as mp:
        # S3Hook
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)

        # Objeto simulado com método download_fileobj
        class MockS3Object:
            def download_fileobj(self, fileobj):
                fileobj.write(buffer.getvalue())

        mp.setattr(S3Hook, "get_key", lambda self, key, bucket_name: MockS3Object())

        # Simula falha ao salvar o primeiro arquivo Parquet
        mp.setattr("script_worker_gold.salva_parquet", lambda df, aws_conn_id, bucket_name, key, **kwargs: False if "df_aggregated.01" in key else True)

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            processa_arquivos_parquet(
                aws_conn_id="aws_default",
                source_bucket_name=bucket,
                project_folder_name=project_folder,
                aggregated_folder_name=aggregated_folder,
                ti=mock_airflow
            )

        # Verifica se o erro foi o esperado
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_SAVING_FILE"]


def test_processa_arquivos_parquet_falha_salvar_parquet_02(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa falha ao salvar o segundo Parquet.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    aggregated_folder = variaveis_teste["AGGREGATED_FOLDER_NAME"]
    timestamp = "20250707_104109"
    silver_filepath = f"{project_folder}/cleandata/df_validado_{timestamp}.parquet"

    # Dados de teste (DataFrame Silver)
    test_data = pd.DataFrame({
        "id": ["123", "124"],
        "name": ["Brewery A", "Brewery B"],
        "country": ["Brazil", "Brazil"],
        "state": ["SP", "RJ"],
        "brewery_type": ["micro", "macro"]
    })

    # Salva no bucket simulado
    buffer = io.BytesIO()
    test_data.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_mock.put_object(Bucket=bucket, Key=silver_filepath, Body=buffer.getvalue())

    # Configura XCom conforme esperado pela DAG
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        ("check_manual_execution", "work_type"): "manual",
        ("pega_info_arquivo_task", "timestamp_nome_arquivo"): timestamp,
        ("processa_arquivos_task", "silver_filepath"): silver_filepath
    }.get((task_ids, key))

    # Mocks do S3 e salva_parquet
    with pytest.MonkeyPatch().context() as mp:
        # S3Hook
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)

        # Objeto simulado com método download_fileobj
        class MockS3Object:
            def download_fileobj(self, fileobj):
                fileobj.write(buffer.getvalue())

        mp.setattr(S3Hook, "get_key", lambda self, key, bucket_name: MockS3Object())

        # Simula sucesso no primeiro salva_parquet e falha no segundo
        def mock_salva_parquet(df, aws_conn_id, bucket_name, key, **kwargs):
            if "df_aggregated.02" in key:
                return False  # Simula falha
            return True  # Sucesso no primeiro

        mp.setattr("script_worker_gold.salva_parquet", mock_salva_parquet)

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            processa_arquivos_parquet(
                aws_conn_id="aws_default",
                source_bucket_name=bucket,
                project_folder_name=project_folder,
                aggregated_folder_name=aggregated_folder,
                ti=mock_airflow
            )

        # Verifica se o erro foi o esperado
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_SAVING_FILE"]


def test_processa_arquivos_parquet_erro_inesperado(s3_mock, mock_airflow, variaveis_teste, mock_mensagens_de_erro):
    """
    Testa erro inesperado.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    project_folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    aggregated_folder = variaveis_teste["AGGREGATED_FOLDER_NAME"]
    timestamp = "20250707_104109"
    silver_filepath = f"{project_folder}/cleandata/df_validado_{timestamp}.parquet"

    # Dados de teste inválidos (causam erro no pd.read_parquet)
    s3_mock.put_object(Bucket=bucket, Key=silver_filepath, Body=b"invalid_parquet_data")

    # Configura XCom
    mock_airflow.xcom_pull.side_effect = lambda task_ids, key: {
        "check_manual_execution": {"work_type": "manual"},
        "pega_info_arquivo_task": {"timestamp_nome_arquivo": timestamp},
        "processa_arquivos_task": {"silver_filepath": silver_filepath}
    }.get(task_ids, {}).get(key)

    # Mock S3Hook
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)
        mp.setattr(S3Hook, "get_key", lambda self, key, bucket_name: type("obj", (), {
            "download_fileobj": lambda fileobj: fileobj.write(b"invalid_parquet_data")
        })())

        # Act & Assert
        with pytest.raises(Exception):
            processa_arquivos_parquet(
                aws_conn_id="aws_default",
                source_bucket_name=bucket,
                project_folder_name=project_folder,
                aggregated_folder_name=aggregated_folder,
                ti=mock_airflow
            )


