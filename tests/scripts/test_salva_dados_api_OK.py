import pytest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from script_worker_bronze import salva_dados_api


def test_salva_dados_api_sucesso(s3_mock, mock_airflow, variaveis_teste, mock_api_response_successo):
    """
    Testa sucesso na execução.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    raw = variaveis_teste["RAWDATA_FOLDER_NAME"]
    url = variaveis_teste["API_URL"]

    # Mock boto3 dentro do S3Hook para usar o s3_mock
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr("requests.get", lambda *args, **kwargs: mock_api_response_successo)

        # Act
        result = salva_dados_api(
            aws_conn_id="aws_default",
            bucket_name=bucket,
            folder_project_name=folder,
            rawdata_folder_name=raw,
            api_url=url,
            ti=mock_airflow
        )

    # Assert
    assert result is True
    # Verifica se XCOM foram criados corretamente
    assert "bronze_filepath_api" in mock_airflow.xcom_push.call_args_list[0].kwargs["key"]
    assert "timestamp_api" in mock_airflow.xcom_push.call_args_list[1].kwargs["key"]


def test_salva_dados_api_timeout(s3_mock, mock_airflow, variaveis_teste, mock_api_timeout):
    """
    Testa falha timeout da API.
    """

    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    raw = variaveis_teste["RAWDATA_FOLDER_NAME"]
    url = variaveis_teste["API_URL"]

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr("requests.get", lambda *args, **kwargs: (_ for _ in ()).throw(mock_api_timeout))

        with pytest.raises(ValueError) as exc:
            salva_dados_api(
                aws_conn_id="aws_default",
                bucket_name=bucket,
                folder_project_name=folder,
                rawdata_folder_name=raw,
                api_url=url,
                ti=mock_airflow
            )
        
        # Valida conteudo da mensagem de erro
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_API_TIMEOUT"]


def test_salva_dados_api_resposta_invalida(s3_mock, mock_airflow, variaveis_teste, mock_api_response_invalida):
    """
    Testa resposta invalida da API.
    """

    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    raw = variaveis_teste["RAWDATA_FOLDER_NAME"]
    url = variaveis_teste["API_URL"]

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr("requests.get", lambda *args, **kwargs: mock_api_response_invalida)

        with pytest.raises(ValueError) as exc:
            salva_dados_api(
                aws_conn_id="aws_default",
                bucket_name=bucket,
                folder_project_name=folder,
                rawdata_folder_name=raw,
                api_url=url,
                ti=mock_airflow
            )
        
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_INVALIDAPI_RESPONSE"]


def test_salva_dados_api_falha_check_key(s3_mock, mock_airflow, variaveis_teste, mock_api_response_successo):
    """
    Testa falha ao salvar arquivo.
    """

    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    raw = variaveis_teste["RAWDATA_FOLDER_NAME"]
    url = variaveis_teste["API_URL"]

    def fake_check_key(*args, **kwargs):
        # Simula falha ao verificar se arquivo foi salvo
        return False  

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: fake_check_key())
        mp.setattr("requests.get", lambda *args, **kwargs: mock_api_response_successo)

        with pytest.raises(ValueError) as exc:
            salva_dados_api(
                aws_conn_id="aws_default",
                bucket_name=bucket,
                folder_project_name=folder,
                rawdata_folder_name=raw,
                api_url=url,
                ti=mock_airflow
            )
        
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_SAVING_FILE"]


def test_salva_dados_api_erro_inesperado(s3_mock, mock_airflow, variaveis_teste, mock_api_response_successo, mock_exception_s3_connection_error):
    """
    Testa  erro inesperado.
    """

    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    folder = variaveis_teste["PROJECT_FOLDER_NAME"]
    raw = variaveis_teste["RAWDATA_FOLDER_NAME"]
    url = variaveis_teste["API_URL"]

    with pytest.MonkeyPatch().context() as mp:
        # Não é necessário mockar S3Hook.get_conn, pois a fixture mock_exception_s3_connection_error já faz isso
        mp.setattr("requests.get", lambda *args, **kwargs: mock_api_response_successo)

        # Verifica se a exceção é levantada
        with pytest.raises(Exception) as exc:
            salva_dados_api(
                aws_conn_id="aws_default",
                bucket_name=bucket,
                folder_project_name=folder,
                rawdata_folder_name=raw,
                api_url=url,
                ti=mock_airflow
            )
        
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_CONEXAO"]

