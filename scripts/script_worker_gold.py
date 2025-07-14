import logging
import io
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from script_utils import mensagens_de_erro
from script_utils import salva_parquet


def processa_arquivos_parquet(aws_conn_id: str, source_bucket_name: str, project_folder_name: str, aggregated_folder_name: str, **kwargs) -> None:
    """
    Description:
        Processa um arquivo Parquet da camada Silver no S3, cria dois DataFrames agregados por país/estado e 
        por país/estado/tipo de cervejaria, e os salva em pastas específicas no S3.

    Args:
        aws_conn_id (str): ID da conexão AWS configurada no Airflow para autenticação no S3.
        source_bucket_name (str): Nome do bucket S3 onde os arquivos estão localizados.
        project_folder_name (str): Nome da pasta raiz do projeto no bucket S3.
        aggregated_folder_name (str): Nome da pasta onde os arquivos agregados serão salvos.
        **kwargs: Contexto do Airflow, incluindo 'ti' (TaskInstance) para operações XCom.

    Returns:
        bool: Retorna True se os arquivos agregados forem salvos com sucesso.

    Raises:
        ValueError: Se o timestamp ou caminho do arquivo não forem encontrados via XCom, 
        o arquivo de entrada não existir no S3 ou houver falha ao salvar os arquivos agregados.
        Exception: Se ocorrer um erro inesperado durante o processamento ou salvamento dos arquivos.
    """
    try:
        ti = kwargs["ti"]
        
        erros = mensagens_de_erro()
        
        logger = logging.getLogger("airflow.task")

        # Verifica tipo da run
        run_type = ti.xcom_pull(task_ids="check_manual_execution", key="work_type")
        
        # Define input_timestamp com base no tipo de execução
        if run_type == "manual":
            input_timestamp = ti.xcom_pull(task_ids="pega_info_arquivo_task", key="timestamp_nome_arquivo")
            logger.info(f"Iniciando execução MANUAL...")
        else:
            input_timestamp = ti.xcom_pull(task_ids="salva_dados_api_task", key="timestamp_api")
            logger.info(f"Iniciando execução AGENDADA...")

        input_path = ti.xcom_pull(task_ids="processa_arquivos_task", key="silver_filepath")

        if not input_timestamp:
            logger.info(f"Erro ao adquirir timestamp via XCOM.")
            raise ValueError(erros['error_xcom_info'])

        if not input_path:
            logger.info(f"Erro ao adquirir filepath via XCOM.")
            raise ValueError(erros['error_xcom_info'])

        logger.info(f"Valores XCOM recebidos - timestamp: {input_timestamp}, silver_filepath: {input_path}")

        hook = S3Hook(aws_conn_id=aws_conn_id)

        if not hook.check_for_key(key=input_path, bucket_name=source_bucket_name):
            raise ValueError(erros['error_file_notfound'])

        logger.info(f"Lendo arquivo Parquet: s3://{source_bucket_name}/{input_path}")
        buffer = io.BytesIO()
        obj = hook.get_key(key=input_path, bucket_name=source_bucket_name)
        obj.download_fileobj(buffer)
        buffer.seek(0)
        df_consolidado = pd.read_parquet(buffer)
        logger.info(f"DataFrame consolidado lido ({len(df_consolidado)} linhas):\n{df_consolidado.head().to_string()}")

        parquet_key = f"{project_folder_name}/{aggregated_folder_name}/df_aggregated.01_{input_timestamp}.parquet"
        parquet_key_02 = f"{project_folder_name}/{aggregated_folder_name}/df_aggregated.02_{input_timestamp}.parquet"

        df_agg_01 = df_consolidado.groupby(['country', 'state']).size().reset_index(name='total_registros')
        logger.info(f"DataFrame Agregado 01 criado")
        sucesso = salva_parquet(df_agg_01, aws_conn_id, source_bucket_name, parquet_key, **kwargs)
        if not sucesso:
            raise ValueError(erros['error_saving_file'])

        df_agg_02 = df_consolidado.groupby(['country', 'state', 'brewery_type']).size().reset_index(name='total_registros')
        logger.info(f"DataFrame Agregado 02 criado")
        sucesso_02 = salva_parquet(df_agg_02, aws_conn_id, source_bucket_name, parquet_key_02, **kwargs)
        if not sucesso_02:
            raise ValueError(erros['error_saving_file'])

        logger.info(f"Execução concluída com sucesso.")
        return True

    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise
