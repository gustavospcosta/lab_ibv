import logging
import io
import hashlib
import pandas as pd
import re
import json
from botocore.exceptions import ClientError
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import hashlib
import logging
from airflow.models import Variable

logger = logging.getLogger("airflow.task")


def mensagens_de_erro() -> dict:
    """
    Description:
        Obtém o dicionário completo de erros da variável do Airflow, convertendo as chaves para letras minúsculas.

    Args:
        None.

    Returns:
        dict: Dicionário com as mensagens de erro, onde as chaves são strings em letras minúsculas.

    Raises:
        ValueError: Se a variável "ERRORS_DICT" não existir ou estiver malformada.
        Exception: Se ocorrer um erro inesperado ao acessar ou processar a variável do Airflow.
    """
    try:
        errors_from_airflow = Variable.get("ERRORS_DICT", deserialize_json=True)
        dict_erros = {k.lower(): v for k, v in errors_from_airflow.items()}
        return dict_erros

    except Exception as e:
        logger.error(f"Erro ao obter variável 'ERRORS_DICT': {str(e)}")
        raise ValueError("Não foi possível carregar o dicionário de erros.")


def valida_estrutura_bucket(aws_conn_id: str, bucket_name: str, folder_project_name: str, subfolders_list: list) -> bool:
    """
    Description:
        Valida a existência de um bucket S3, uma pasta de projeto e suas subpastas especificadas.

    Args:
        aws_conn_id (str): ID da conexão AWS configurada no Airflow.
        bucket_name (str): Nome do bucket S3 a ser validado.
        folder_project_name (str): Nome da pasta do projeto dentro do bucket.
        subfolders_list (list): Lista de nomes das subpastas a serem validadas dentro da pasta do projeto.

    Returns:
        bool: Retorna True se o bucket, a pasta do projeto e todas as subpastas especificadas existirem.

    Raises:
        ValueError: Se o bucket não existir, a pasta do projeto não for encontrada ou qualquer subpasta estiver ausente.
        Exception: Se ocorrer um erro inesperado durante a validação da estrutura do bucket.
    """
    try:
        erros = mensagens_de_erro()
        
        logger.info(f"Iniciando validação do bucket '{bucket_name}'")
        
        # Conexao S3
        hook = S3Hook(aws_conn_id=aws_conn_id)
        s3_client = hook.get_conn()
        
        try:
               s3_client.head_bucket(Bucket=bucket_name)
        except ClientError:
               raise ValueError(erros['error_mainbucket_missing'])
        
        logger.info(f"Bucket '{bucket_name}' existe.")
        
        project_prefix = f"{folder_project_name.strip('/')}/"
        
        resp_proj = s3_client.list_objects_v2(
               Bucket=bucket_name,
               Prefix=project_prefix,
               MaxKeys=1
        )
        
        if resp_proj.get("KeyCount", 0) == 0:
               raise ValueError(erros['error_folder_missing'])
        
        logger.info(f"Pasta do projeto '{project_prefix}' encontrada.")
        
        for subfolder in subfolders_list:
               sub_prefix = f"{project_prefix}{subfolder.strip('/')}/"
               resp = s3_client.list_objects_v2(
                   Bucket=bucket_name,
                   Prefix=sub_prefix,
                   MaxKeys=1
               )

               if resp.get("KeyCount", 0) == 0:
                   raise ValueError(erros['error_folder_missing'])

               logger.info(f"Subpasta '{subfolder}' encontrada.")
        
        logger.info(f"Estrutura do projeto '{folder_project_name}' validada com sucesso.")
        return True
    
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise


def pega_info_arquivo(aws_conn_id: str, bucket_name: str, **kwargs) -> bool:
    """
    Description:
        Obtém e valida informações de um arquivo JSON especificado no S3 para execuções manuais, extraindo o timestamp do nome do arquivo e verificando seu conteúdo.

    Args:
        aws_conn_id (str): ID da conexão com a AWS configurada no Airflow.
        bucket_name (str): Nome do bucket S3 onde o arquivo manual está localizado.
        **kwargs: Contexto do Airflow, contendo o objeto 'ti' (TaskInstance) para operações XCom.

    Returns:
        bool: Retorna True se o arquivo for validado com sucesso e suas informações forem exportadas via XCom.

    Raises:
        ValueError: Se a variável 'MANUAL_FILE_PATH' não for encontrada, o formato do nome do arquivo for inválido, o arquivo não existir, estiver vazio ou seu conteúdo não for uma lista.
        Exception: Se ocorrer um erro inesperado durante o processo de validação ou leitura do arquivo.
    """
    try:
        ti = kwargs["ti"] 
        
        erros = mensagens_de_erro()
        
        logger.info(f"Iniciando execução MANUAL...")
        
        hook = S3Hook(aws_conn_id=aws_conn_id)

        try:
            manual_file_path = Variable.get("MANUAL_FILE_PATH").lstrip('/')
        
        except KeyError:
            raise ValueError(erros['error_variavel_notfound'])
               
        path_pattern = r'^[^/]+/[^/]+/raw_list_(\d{8}_\d{6})\.json$'

        match = re.match(path_pattern, manual_file_path)
        if not match:
            raise ValueError(erros['error_validation_fail'])
        
        # Captura o timestamp da regex
        timestamp_arquivo = match.group(1)
        logger.info(f"Timestamp extraído do arquivo: {timestamp_arquivo}")

        # Valida arquivo raw
        if not hook.check_for_key(key=manual_file_path, bucket_name=bucket_name):
            raise ValueError(erros['error_file_notfound'])
        
        obj = hook.get_key(key=manual_file_path, bucket_name=bucket_name)
        total_bytes_transferred = obj.content_length

        if total_bytes_transferred == 0:
            raise ValueError(erros['error_empty_file'])
        
        content = hook.read_key(key=manual_file_path, bucket_name=bucket_name)
        all_breweries = json.loads(content)
        
        if not isinstance(all_breweries, list):
            raise ValueError(erros['error_check_file_content'])
        
        logger.info(f"Total de registros no arquivo raw: {len(all_breweries)}")
        
        ti.xcom_push(key="bronze_filepath_manual", value=manual_file_path)
        ti.xcom_push(key="timestamp_nome_arquivo", value=timestamp_arquivo)
        logger.info(f"Execução realizada com sucesso")
        return True
    
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise


def salva_parquet(df: pd.DataFrame, aws_conn_id: str, bucket_name: str, parquet_key: str, **kwargs) -> bool:
    """
    Description:
        Salva um DataFrame pandas como um arquivo Parquet no Amazon S3 e valida sua existência.

    Args:
        df (pd.DataFrame): DataFrame pandas a ser salvo no formato Parquet.
        aws_conn_id (str): ID da conexão AWS configurada no Airflow para autenticação no S3.
        bucket_name (str): Nome do bucket S3 onde o arquivo Parquet será armazenado.
        parquet_key (str): Caminho completo (incluindo prefixos e nome do arquivo) para salvar o arquivo Parquet no bucket.
        **kwargs: Argumentos adicionais do contexto do Airflow, incluindo 'ti' (TaskInstance) para operações XCom.

    Returns:
        bool: Retorna True se o arquivo Parquet for salvo com sucesso e confirmado no S3.

    Raises:
        ValueError: Se o arquivo Parquet for enviado, mas a verificação de existência no S3 falhar.
        Exception: Se ocorrer um erro inesperado durante o processo de salvamento ou validação do arquivo no S3.
    """
    try:
        ti = kwargs["ti"]

        erros = mensagens_de_erro()

        # Verifica o tipo de execução
        run_type = ti.xcom_pull(task_ids="check_manual_execution", key="work_type")

        if run_type == "manual":
            logger.info("Iniciando execução MANUAL...")
        else:
            logger.info("Iniciando execução AGENDADA...")

        # Converte DataFrame para buffer Parquet
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Envia para o S3
        hook = S3Hook(aws_conn_id=aws_conn_id)
        hook.load_file_obj(
            file_obj=buffer,
            key=parquet_key,
            bucket_name=bucket_name,
            replace=True
        )

        # Valida se o arquivo foi salvo corretamente
        if hook.check_for_key(key=parquet_key, bucket_name=bucket_name):
            logger.info(f"Arquivo salvo com sucesso {bucket_name}/{parquet_key}")
            logger.info("Execução concluída com sucesso")
            return True
        else:
            raise ValueError(erros["error_file_notfound"])

    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise


def move_arquivo_processado(aws_conn_id: str, bucket_name: str, processed_folder_name: str, **kwargs) -> bool:
    """
    Description:
        Move um arquivo JSON da camada Bronze no S3 para a pasta de processados, valida a integridade com hash MD5 e remove o arquivo original se a cópia for válida.

    Args:
        aws_conn_id (str): ID da conexão AWS configurada no Airflow para autenticação no S3.
        bucket_name (str): Nome do bucket S3 onde o arquivo está localizado.
        processed_folder_name (str): Nome da pasta de destino para o arquivo processado.
        **kwargs: Argumentos adicionais do contexto do Airflow, incluindo 'ti' (TaskInstance) para operações XCom.

    Returns:
        bool: Retorna True se o arquivo for movido com sucesso, validado e o original removido.

    Raises:
        ValueError: Se o caminho do arquivo não for encontrado no XCom, o arquivo não estiver na pasta raw.data, o arquivo original não existir ou a validação do hash MD5 falhar.
        Exception: Se ocorrer um erro inesperado durante o processo de movimentação, validação ou exclusão do arquivo.
    """
    try:
        ti = kwargs["ti"]
        
        erros = mensagens_de_erro()
        
        logger = logging.getLogger("airflow.task")
        
        # Verifica tipo da run
        run_type = ti.xcom_pull(task_ids="check_manual_execution", key="work_type")

        if run_type == "manual":
            # Recupera informações execução manual
            source_file = ti.xcom_pull(task_ids="pega_info_arquivo_task", key="bronze_filepath_manual")
            logger.info(f"Iniciando execução MANUAL...")
            logger.info(f"Arquivo a ser movido: {source_file}")

        else:
            # Recupera informações execução agendada
            source_file = ti.xcom_pull(task_ids="salva_dados_api_task", key="bronze_filepath_api")
            logger.info(f"Iniciando execução AGENDADA...")
            logger.info(f"Arquivo a ser movido: {source_file}")
        
        if not source_file:
            raise ValueError(erros['error_xcom_info'])

        if "/raw.data/" not in source_file:
            raise ValueError(erros['error_folder_missing']) 

        processed_file = source_file.replace("/raw.data/", f"/{processed_folder_name}/")
        
        logger.info("Movendo: %s → %s", source_file, processed_file)

        hook = S3Hook(aws_conn_id=aws_conn_id)

        if not hook.check_for_key(key=source_file, bucket_name=bucket_name):
            raise ValueError(erros['error_file_notfound'])

        hook.copy_object(
            source_bucket_key=source_file,
            dest_bucket_key=processed_file,
            source_bucket_name=bucket_name,
            dest_bucket_name=bucket_name
        )

        def calcula_md5_bytes(data: bytes) -> str:
            return hashlib.md5(data).hexdigest()

        original_data = hook.get_key(source_file, bucket_name).get()["Body"].read()
        copied_data = hook.get_key(processed_file, bucket_name).get()["Body"].read()

        if calcula_md5_bytes(original_data) != calcula_md5_bytes(copied_data):
            raise ValueError(erros['error_validation_fail']) 

        hook.get_conn().delete_object(Bucket=bucket_name, Key=source_file)
        
        logger.info("Arquivo movido com sucesso: s3://%s/%s", bucket_name, processed_file)
        logger.info("Arquivo original deletado: s3://%s/%s", bucket_name, source_file)
        logger.info(f"Execução concluída com sucesso")
        return True

    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise


def deleta_arquivo_processado(aws_conn_id: str, bucket_name: str, **kwargs) -> bool:
    """
    Description:
        Deleta o arquivo Parquet consolidado da camada Silver no S3, utilizando o caminho obtido via XCom da task anterior.

    Args:
        aws_conn_id (str): ID da conexão AWS configurada no Airflow para autenticação no S3.
        bucket_name (str): Nome do bucket S3 onde o arquivo está armazenado.
        **kwargs: Argumentos adicionais do contexto do Airflow, incluindo 'ti' (TaskInstance) para operações XCom.

    Returns:
        bool: Retorna True se o arquivo for deletado com sucesso.

    Raises:
        ValueError: Se o caminho do arquivo não for encontrado via XCom ou se o arquivo não existir no S3.
        Exception: Se ocorrer um erro inesperado durante o processo de exclusão do arquivo.
    """
    try:
        ti = kwargs["ti"]
        
        erros = mensagens_de_erro()
                
        hook = S3Hook(aws_conn_id=aws_conn_id)

        # Verifica tipo da execução
        run_type = ti.xcom_pull(task_ids="check_manual_execution", key="work_type")

        if run_type == "manual":
            logger.info("Iniciando execução MANUAL...")
        else:
            logger.info("Iniciando execução AGENDADA...")

        # Recupera o caminho do arquivo a ser deletado via XCom
        filepath = ti.xcom_pull(task_ids="processa_arquivos_task", key="silver_filepath")
        
        logger.info(f"Arquivo a ser deletado: {filepath}")

        if not filepath:
            raise ValueError(erros['error_xcom_info'])

        logger.info(f"Arquivo a ser deletado: s3://{bucket_name}/{filepath}")

        # Verifica se o arquivo existe no S3
        if not hook.check_for_key(key=filepath, bucket_name=bucket_name):
            raise ValueError(erros['error_file_notfound'])

        # Deleta arquivo
        hook.get_conn().delete_object(Bucket=bucket_name, Key=filepath)
        logger.info(f"Arquivo deletado com sucesso: s3://{bucket_name}/{filepath}")

        return True

    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise
