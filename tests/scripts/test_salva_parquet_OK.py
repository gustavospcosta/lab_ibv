import pytest
import pandas as pd
import io
from unittest.mock import patch
from script_utils import salva_parquet
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def test_salva_parquet_sucesso(s3_mock, variaveis_teste, mock_mensagens_de_erro, mock_airflow):
    """
    Testa sucesso ao salvar arquivo Parquet.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    parquet_key = "test_folder/cleandata/test_df.parquet"
    df = pd.DataFrame({
        "id": ["123", "124"],
        "name": ["Brewery A", "Brewery B"],
        "country": ["Brazil", "Brazil"],
        "state": ["SP", "RJ"]
    })

    # Mock S3Hook
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)
        mp.setattr(S3Hook, "load_file_obj", lambda self, file_obj, key, bucket_name, replace: s3_mock.put_object(
            Bucket=bucket_name, Key=key, Body=file_obj.getvalue()))

        # Act
        result = salva_parquet(
            df,
            aws_conn_id="aws_default",
            bucket_name=bucket,
            parquet_key=parquet_key,
            ti=mock_airflow
        )

    # Assert
    assert result is True



def test_salva_parquet_erro_inesperado(s3_mock, variaveis_teste, mock_mensagens_de_erro, mock_airflow):
    """
    Testa erro inesperado ao salvar arquivo Parquet.
    """

    # Arrange
    bucket = variaveis_teste["MAIN_BUCKET_NAME"]
    parquet_key = "test_folder/cleandata/test_df.parquet"
    df = pd.DataFrame({
        "id": ["123", "124"],
        "name": ["Brewery A", "Brewery B"],
        "country": ["Brazil", "Brazil"],
        "state": ["SP", "RJ"]
    })

    # Mock S3Hook com exceção ao salvar
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(S3Hook, "get_conn", lambda self: s3_mock)
        mp.setattr(S3Hook, "check_for_key", lambda self, key, bucket_name: True)
        mp.setattr(
            S3Hook,
            "load_file_obj",
            lambda *args, **kwargs: (_ for _ in ()).throw(Exception(variaveis_teste["ERRORS_DICT"]["ERROR_CONEXAO"]))
        )

        # Act & Assert
        with pytest.raises(Exception) as exc:
            salva_parquet(
                df,
                aws_conn_id="aws_default",
                bucket_name=bucket,
                parquet_key=parquet_key,
                ti=mock_airflow
            )

        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_CONEXAO"]
