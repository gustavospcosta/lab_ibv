from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from logging import getLogger
from datetime import datetime, timedelta
from script_utils import valida_estrutura_bucket, pega_info_arquivo, move_arquivo_processado, deleta_arquivo_processado
from script_email import enviar_email_customizado
from script_worker_bronze import salva_dados_api
from script_worker_gold import processa_arquivos_parquet
from script_worker_prata import processa_arquivos


def check_manual_execution(**kwargs):
    """
    Description:
        Verifica a existência da variável 'MANUAL_FILE_PATH' no Airflow para determinar se a execução é manual ou agendada, definindo o fluxo da próxima tarefa.

    Args:
        **kwargs: Contexto do Airflow, contendo o objeto 'ti' (TaskInstance) para operações XCom.

    Returns:
        str: O task_id da próxima tarefa a ser executada ('pega_info_arquivo_task' para execução manual ou 'salva_dados_api_task' para execução agendada).

    Raises:
        ValueError: Não aplicável diretamente, mas pode ser levantado implicitamente por falhas na obtenção de variáveis ou XCom.
        Exception: Se 'ti' não estiver presente nos kwargs ou ocorrer um erro inesperado durante a verificação.
    """

    logger = getLogger("airflow.task")
    
    if "ti" not in kwargs:
        logger.error("Erro: TaskInstance ('ti') não fornecida no contexto")
        raise KeyError("TaskInstance não fornecida no contexto")
    
    ti = kwargs["ti"]
    
    try:
        manual_file_path = Variable.get("MANUAL_FILE_PATH")
        logger.info(f"Execução manual detectada: {manual_file_path}")
        ti.xcom_push(key="work_type", value="manual")
        ti.xcom_push(key="bronze_filepath", value=manual_file_path.lstrip('/'))
        return "pega_info_arquivo_task"
    except KeyError:
        logger.info("Execução agendada detectada: prosseguindo com tarefa de API")
        return "salva_dados_api_task"

default_args = {
    "owner": "Gustavo",
    "start_date": datetime(2025, 1, 1)
}

with DAG(
    dag_id="main",
    schedule="*/30 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["datapipeline cervejarias", "ETL"],
    max_active_runs=3
) as dag:

    # BranchPythonOperator para decidir o caminho de execução
    branch_task = BranchPythonOperator(
        task_id="check_manual_execution",
        python_callable=check_manual_execution
    )

    # EmptyOperator para marcar o início da DAG
    inicio_dag_task = EmptyOperator(
        task_id="inicio_dag",
        on_success_callback=lambda context: enviar_email_customizado(context, "INICIO_DAG")
    )

    # PythonOperator para validar diretorios em um bucket
    valida_diretorios_task = PythonOperator(
        task_id="valida_diretorios_task",
        python_callable=valida_estrutura_bucket,
        op_kwargs={
            "aws_conn_id": Variable.get("AWS_CONN_ID"),
            "bucket_name": Variable.get("MAIN_BUCKET_NAME"),
            "folder_project_name": Variable.get("PROJECT_FOLDER_NAME"),
            "subfolders_list": Variable.get("PROJECT_FOLDER_LIST", deserialize_json=True)
        },
        retries=2,
        retry_delay=timedelta(minutes=1),
        on_success_callback=lambda context: enviar_email_customizado(context, "TASK_SUCESSO"),
        on_failure_callback=lambda context: enviar_email_customizado(context, "TASK_ERRO")
    )

    # PythonOperator para salvar dados de uma API em um bucket
    salva_dados_api_task = PythonOperator(
        task_id="salva_dados_api_task",
        python_callable=salva_dados_api,
        op_kwargs={
            "aws_conn_id": Variable.get("AWS_CONN_ID"),
            "bucket_name": Variable.get("MAIN_BUCKET_NAME"),
            "folder_project_name": Variable.get("PROJECT_FOLDER_NAME"),
            "rawdata_folder_name": Variable.get("RAWDATA_FOLDER_NAME"),
            "api_url": Variable.get("API_URL")
        },
        retries=5,
        retry_delay=timedelta(minutes=3),
        on_success_callback=lambda context: enviar_email_customizado(context, "TASK_SUCESSO"),
        on_failure_callback=lambda context: enviar_email_customizado(context, "TASK_ERRO")
    )

    # PythonOperator para processar um arquivo fornecido manualmente
    pega_info_arquivo_task = PythonOperator(
        task_id="pega_info_arquivo_task",
        python_callable=pega_info_arquivo,
        op_kwargs={
            "aws_conn_id": Variable.get("AWS_CONN_ID"),
            "bucket_name": Variable.get("MAIN_BUCKET_NAME")
        },
        retries=5,
        retry_delay=timedelta(minutes=3),
        on_success_callback=lambda context: enviar_email_customizado(context, "TASK_SUCESSO"),
        on_failure_callback=lambda context: enviar_email_customizado(context, "TASK_ERRO")
    )

    # EmptyOperator como ponto de junção para as ramificações antes das tarefas subsequentes
    join_branch = EmptyOperator(task_id="join_branch")

    # PythonOperator para processar arquivos
    processa_arquivos_task = PythonOperator(
        task_id="processa_arquivos_task",
        python_callable=processa_arquivos,
        op_kwargs={
            "aws_conn_id": Variable.get("AWS_CONN_ID"),
            "source_bucket_name": Variable.get("MAIN_BUCKET_NAME"),
            "project_folder_name": Variable.get("PROJECT_FOLDER_NAME"),
            "work_folder": Variable.get("CLEANDATA_FOLDER_NAME"),
            "json_list": Variable.get("JSON_FIELDS_LIST", deserialize_json=True),
            "df_datatype": Variable.get("DF_DATATYPE_SCHEMA", deserialize_json=True),
        },
        retries=5,
        retry_delay=timedelta(minutes=3),
        on_success_callback=lambda context: enviar_email_customizado(context, "TASK_SUCESSO"),
        on_failure_callback=lambda context: enviar_email_customizado(context, "TASK_ERRO"),
        trigger_rule="one_success" # <-- AQUI ESTÁ A MUDANÇA CRÍTICA
    )

    # PythonOperator para processar arquivos parquet
    processa_arquivos_parquet_task = PythonOperator(
        task_id="processa_arquivos_parquet_task",
        python_callable=processa_arquivos_parquet,
        op_kwargs={
            "aws_conn_id": Variable.get("AWS_CONN_ID"),
            "source_bucket_name": Variable.get("MAIN_BUCKET_NAME"),
            "project_folder_name": Variable.get("PROJECT_FOLDER_NAME"),
            "aggregated_folder_name": Variable.get("AGGREGATED_FOLDER_NAME")
        },
        retries=5,
        retry_delay=timedelta(minutes=3),
        on_success_callback=lambda context: enviar_email_customizado(context, "TASK_SUCESSO"),
        on_failure_callback=lambda context: enviar_email_customizado(context, "TASK_ERRO")
    )

    # PythonOperator para mover arquivos processados
    move_arquivo_processado_task = PythonOperator(
        task_id="move_arquivos_processados_task",
        python_callable=move_arquivo_processado,
        op_kwargs={
            "aws_conn_id": Variable.get("AWS_CONN_ID"),
            "bucket_name": Variable.get("MAIN_BUCKET_NAME"),
            "processed_folder_name": Variable.get("PROCESSED_FOLDER_NAME")
        },
        retries=5,
        retry_delay=timedelta(minutes=3),
        on_success_callback=lambda context: enviar_email_customizado(context, "TASK_SUCESSO"),
        on_failure_callback=lambda context: enviar_email_customizado(context, "TASK_ERRO")
    )
    
    # PythonOperator para deletar arquivos processados
    deleta_arquivo_processado_task = PythonOperator(
        task_id="deleta_arquivos_processados_task",
        python_callable=deleta_arquivo_processado,
        op_kwargs={
            "aws_conn_id": Variable.get("AWS_CONN_ID"),
            "bucket_name": Variable.get("MAIN_BUCKET_NAME")
        },
        retries=5,
        retry_delay=timedelta(minutes=3),
        on_success_callback=lambda context: enviar_email_customizado(context, "TASK_SUCESSO"),
        on_failure_callback=lambda context: enviar_email_customizado(context, "TASK_ERRO")
    )

    # EmptyOperator para marcar o fim da DAG
    fim_dag_task = EmptyOperator(
        task_id="fim_dag_task",
        on_success_callback=lambda context: enviar_email_customizado(context, "FIM_DAG_SUCESSO")
    )

    # Definir dependências das tarefas
    # Tarefas iniciais
    inicio_dag_task >> valida_diretorios_task >> branch_task

    # Caminho 1: Execução agendada
    branch_task >> salva_dados_api_task >> processa_arquivos_task

    # Caminho 2: Execução manual
    branch_task >> pega_info_arquivo_task >> processa_arquivos_task

    # Convergência e tarefas subsequentes
    processa_arquivos_task >> join_branch
    join_branch >> processa_arquivos_parquet_task >> move_arquivo_processado_task >> deleta_arquivo_processado_task >> fim_dag_task
