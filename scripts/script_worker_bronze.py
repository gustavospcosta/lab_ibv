import logging
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
from datetime import datetime
from script_utils import mensagens_de_erro


def salva_dados_api(aws_conn_id: str, bucket_name: str, folder_project_name: str, rawdata_folder_name: str, api_url: str, **kwargs) -> bool:
    """
    Description:
        Extrai dados de uma API pública e salva como arquivo JSON na camada Bronze do S3.

    Args:
        aws_conn_id (str): ID da conexão AWS configurada no Airflow para autenticação no S3.
        bucket_name (str): Nome do bucket S3 onde o arquivo JSON será armazenado.
        folder_project_name (str): Nome da pasta principal do projeto no bucket S3.
        rawdata_folder_name (str): Nome da subpasta para armazenar os dados brutos.
        api_url (str): URL da API pública a ser consultada.
        **kwargs: Contexto do Airflow, incluindo 'ti' (TaskInstance) para operações XCom.

    Returns:
        bool: Retorna True se os dados forem salvos com sucesso no S3.

    Raises:
        ValueError: Se a chamada à API atingir o tempo limite, a resposta da API não for uma lista ou o arquivo não for salvo corretamente no S3.
        Exception: Se ocorrer um erro inesperado durante a consulta à API ou o salvamento no S3.
    """
    try:
        ti = kwargs["ti"]
        
        erros = mensagens_de_erro()
        
        logger = logging.getLogger("airflow.task")
        
        logger.info("Iniciando execução AGENDADA...")

        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{folder_project_name}/{rawdata_folder_name}/raw_list_{timestamp}.json"

        # Consulta API
        logger.info(f"Consultando API: {api_url}")
        try:
            response = requests.get(api_url, timeout=15)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            logger.error("Timeout na chamada da API.")
            raise ValueError(erros["error_api_timeout"])

        data = response.json()

        if not isinstance(data, list):
            raise ValueError(erros["error_invalidapi_response"])

        logger.info(f"Total de registros recebidos: {len(data)}")

        json_data = json.dumps(data, ensure_ascii=False, indent=2)

        logger.info(f"Salvando arquivo no S3: {filename}")
        s3_hook.load_string(
            string_data=json_data,
            key=filename,
            bucket_name=bucket_name,
            replace=True,
            encoding="utf-8"
        )

        if s3_hook.check_for_key(key=filename, bucket_name=bucket_name):
            logger.info(f"Arquivo salvo com sucesso: {filename}")
            ti.xcom_push(key="bronze_filepath_api", value=filename)
            ti.xcom_push(key="timestamp_api", value=timestamp)
            return True
        else:
            logger.error(f"Falha ao salvar o arquivo: {filename}")
            raise ValueError(erros["error_saving_file"])

    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise
