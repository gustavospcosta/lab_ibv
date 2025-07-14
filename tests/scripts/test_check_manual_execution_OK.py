import pytest
from dag_main import check_manual_execution

def test_check_manual_execution_manual(mock_airflow, variaveis_teste, mock_airflow_variables):
    """
    Testa o caminho de execução manual quando a variável MANUAL_FILE_PATH está presente.
    """
    # Arrange
    manual_path = "breweries.project/raw.data/raw_list_20250707_104109.json"
    variaveis_teste["MANUAL_FILE_PATH"] = manual_path  # Configura o caminho manual no dicionário de variáveis
    assert variaveis_teste["MANUAL_FILE_PATH"] is not None, "MANUAL_FILE_PATH não pode ser None"

    # Act
    task_id = check_manual_execution(ti=mock_airflow)

    # Assert
    # Verifica se o retorno é para o task correto
    assert task_id == "pega_info_arquivo_task"

    # Verifica se os XComs foram enviados corretamente
    mock_airflow.xcom_push.assert_any_call(key="work_type", value="manual")
    # Valor do XCom com barras iniciais removidas, conforme a função
    mock_airflow.xcom_push.assert_any_call(key="bronze_filepath", value=manual_path.lstrip('/'))


def test_check_manual_execution_agendada(mock_airflow, variaveis_teste, mock_airflow_variables):
    """
    Testa o caminho de execução agendada quando a variável MANUAL_FILE_PATH não está presente.
    """
    # Arrange
    # Garante que MANUAL_FILE_PATH não está em variaveis_teste para simular KeyError
    if "MANUAL_FILE_PATH" in variaveis_teste:
        del variaveis_teste["MANUAL_FILE_PATH"]

    # Act
    task_id = check_manual_execution(ti=mock_airflow)

    # Assert
    # Verifica se o retorno é para o task correto
    assert task_id == "salva_dados_api_task"

    # Verifica que nenhum XCom foi enviado
    mock_airflow.xcom_push.assert_not_called()