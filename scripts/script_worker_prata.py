import logging
import json
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from script_utils import salva_parquet, mensagens_de_erro

logger = logging.getLogger("airflow.task")


def valida_estrutura_df(df: pd.DataFrame, json_list: list, df_datatype: dict) -> tuple[bool, str | None, pd.DataFrame | None]:
    """
    Description:
        Valida a estrutura de um DataFrame verificando a presença de colunas obrigatórias e ajustando tipos de dados conforme esquema.

    Args:
        df (pd.DataFrame): DataFrame a ser validado.
        json_list (list): Lista de colunas obrigatórias que devem estar presentes no DataFrame.
        df_datatype (dict): Dicionário especificando os tipos de dados esperados para as colunas do DataFrame.

    Returns:
        tuple[bool, str | None, pd.DataFrame | None]: Tupla contendo:
            - Booleano indicando se o DataFrame é válido (True) ou não (False).
            - ID do registro (se disponível) ou mensagem de erro para rastreamento.
            - DataFrame com tipos ajustados se válido; None se inválido.

    Raises:
        ValueError: Não aplicável diretamente, mas erros de validação são retornados na tupla como False com mensagem.
        Exception: Se ocorrer um erro inesperado durante a validação do DataFrame.
    """
    try:
        

        logger.info(f"Lista de colunas obrigatórias: {json_list}")

        logger.info(f"Esquema do dataframe: {df_datatype}")

        # Verifica se colunas obrigatórias existem
        missing_columns = [col for col in json_list if col not in df.columns]
        if missing_columns:
            item_id = df.get("id", pd.Series(["ID não informado"])).iloc[0]
            logger.warning(f"DF ignorado para o ID {item_id} — faltam colunas: {missing_columns}")
            return False, item_id, None

        item_id = df["id"].iloc[0] if "id" in df.columns else "ID não informado"

        # Substitui valores nulos conhecidos por pd.NA
        if df_datatype:
            df = df.replace(['', 'null', 'None', 'NULL', 'N/A'], pd.NA)
            logger.info(f"DF após replace de nulos: \n{df}")

        # Ajusta esquema
        df = df.astype(df_datatype)
        logger.info(f"DF após astype: \n{df.dtypes}")

        logger.info(f"DF válido para o ID {item_id}")
        return True, item_id, df

    except Exception as e:
        logger.warning(f"Falha ao validar DataFrame com ID {item_id}: {e}")
        return False, item_id, None


def processa_arquivos(aws_conn_id:str, source_bucket_name:str, project_folder_name:str, work_folder: str, json_list: list, df_datatype: dict, **kwargs) -> None:
    """
    Description:
        Processa arquivos JSON da camada Bronze no S3, valida estrutura e tipos de dados, consolida em um DataFrame e salva como arquivo Parquet na camada Silver.

    Args:
        aws_conn_id (str): ID da conexão AWS configurada no Airflow para autenticação no S3.
        source_bucket_name (str): Nome do bucket S3 onde os arquivos de origem estão localizados.
        project_folder_name (str): Nome da pasta raiz do projeto no bucket S3.
        work_folder (str): Nome da subpasta onde o arquivo Parquet consolidado será salvo.
        json_list (list): Lista de colunas obrigatórias para validação do DataFrame.
        df_datatype (dict): Dicionário especificando os tipos de dados esperados para as colunas do DataFrame.
        **kwargs: Contexto do Airflow, incluindo 'ti' (TaskInstance) para operações XCom.

    Returns:
        bool: Retorna True se o processamento e salvamento do arquivo Parquet forem bem-sucedidos.

    Raises:
        ValueError: Se o caminho do arquivo ou timestamp não forem encontrados via XCom, o arquivo de entrada não existir, estiver vazio, não for uma lista JSON válida ou o salvamento do Parquet falhar.
        Exception: Se ocorrer um erro inesperado durante o processamento ou salvamento do arquivo.
    """
    try:

        ti = kwargs["ti"]

        erros = mensagens_de_erro()

        #verifica tipo da run
        run_type = ti.xcom_pull(task_ids="check_manual_execution", key="work_type")

        if run_type == "manual":
            # Recupera informações execução manual
            logger.info(f"Iniciando execução MANUAL...")
            key = ti.xcom_pull(task_ids="pega_info_arquivo_task", key="bronze_filepath_manual")
            timestamp = ti.xcom_pull(task_ids="pega_info_arquivo_task", key="timestamp_nome_arquivo")
        
        else:
            # Recupera informações execução agendada
            logger.info(f"Iniciando execução AGENDADA...")
            key = ti.xcom_pull(task_ids="salva_dados_api_task", key="bronze_filepath_api")
            timestamp = ti.xcom_pull(task_ids="salva_dados_api_task", key="timestamp_api")

        if not key or not timestamp:
            raise ValueError(erros['error_xcom_info'])

        logger.info(f"Valores XCom recebidos - bronze_filepath: {key}, timestamp: {timestamp}")

        hook = S3Hook(aws_conn_id=aws_conn_id)

        # Verifica se o arquivo existe
        if not hook.check_for_key(key=key, bucket_name=source_bucket_name):
            raise ValueError(erros['error_file_notfound'])

        parquet_path = f"{project_folder_name}/{work_folder}/df_validado_{timestamp}.parquet"

        logger.info(f"Processando arquivo s3://{source_bucket_name}/{key}")
        content = hook.read_key(key=key, bucket_name=source_bucket_name)

        if not content.strip():
            raise ValueError(erros['error_empty_file'])

        data = json.loads(content)

        if not isinstance(data, list):
            raise ValueError(erros['error_check_file_content'])

        logger.info(f"Total de registros no JSON: {len(data)}")
        
        lista_dfs = []
        ids_descartados = []

        for item in data:
            df_raw = pd.json_normalize(item)
            is_valid, item_id, df = valida_estrutura_df(df_raw, json_list, df_datatype)

            if is_valid and df is not None:
                lista_dfs.append(df)
            else:
                ids_descartados.append(item_id)
    
        logger.warning(f"IDs descartados: {ids_descartados}")

        if lista_dfs:
            df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        else:
            df_consolidado = pd.DataFrame()
        
        logger.info(f"DataFrame Consolidado criado com sucesso ({len(df_consolidado)} linhas):\n{df_consolidado.head().to_string()}")

        # Salva DF como parquet
        sucesso = salva_parquet(df_consolidado, aws_conn_id, source_bucket_name, parquet_path, **kwargs)
        logger.info(f"DataFrame Consolidado salvo com sucesso.")
        
        if not sucesso:
            raise ValueError(erros['error_saving_file'])

        # Exporta o caminho do arquivo
        ti.xcom_push(key="silver_filepath", value=parquet_path)
        logger.info(f"Execução concluída com sucesso")
        return True

    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise
