import pytest
from unittest.mock import patch
from datetime import datetime
from unittest.mock import MagicMock
from script_email import enviar_email_customizado
import boto3


def test_enviar_email_customizado_sucesso(variaveis_teste, mock_mensagens_de_erro):
    """
    Testa sucesso ao enviar e-mail.
    """
    
    # Arrange
    mock_send_email = MagicMock()
    context = {
        "dag": MagicMock(dag_id="example_dag"),
        "task_instance": MagicMock(task_id="example_task"),
        "execution_date": datetime(2025, 7, 7, 10, 0, 0)
    }

    # Mock send_email do Airflow
    with patch("script_email.send_email", mock_send_email):
        # Act
        result = enviar_email_customizado(context=context, tipo_evento="task_falha")

    # Assert
    assert result is True
    assert mock_send_email.called


def test_enviar_email_customizado_parametros_invalidos(variaveis_teste, mock_mensagens_de_erro):
    """
    Testa falha na validação de parâmetros (destinatário vazio).
    """

    # Arrange
    mock_send_email = MagicMock()
    context = {
        "dag": MagicMock(dag_id="example_dag"),
        "task_instance": MagicMock(task_id="example_task"),
        "execution_date": datetime(2025, 7, 7, 10, 0, 0),
        "logical_date": datetime(2025, 7, 7, 10, 0, 0)
    }

    # Simula tipo incorreto para acionar o erro
    variaveis_teste["EMAIL_LIST"] = "nao_e_uma_lista"

    # Mock send_email do Airflow
    with patch("script_email.send_email", mock_send_email):
        # Act & Assert
        with pytest.raises(ValueError) as exc:
            enviar_email_customizado(context=context, tipo_evento="task_falha")
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_EMAIL_LIST"]
        assert not mock_send_email.called


def test_enviar_email_customizado_erro_inesperado(variaveis_teste, mock_mensagens_de_erro):
    """
    Testa erro inesperado ao enviar e-mail.
    """
    
    # Arrange
    mock_send_email = MagicMock(side_effect=Exception("Erro de conexao."))
    context = {
        "dag": MagicMock(dag_id="example_dag"),
        "task_instance": MagicMock(task_id="example_task"),
        "execution_date": datetime(2025, 7, 7, 10, 0, 0)
    }
    
    variaveis_teste["EMAIL_LIST"] = ["recipient@example.com"]

    # Mock send_email do Airflow
    with patch("script_email.send_email", mock_send_email):
        # Act & Assert
        with pytest.raises(Exception) as exc:
            enviar_email_customizado(context=context, tipo_evento="task_falha")
        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_CONEXAO"]

