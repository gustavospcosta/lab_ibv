import pytest
from botocore.exceptions import ClientError
from script_utils import valida_estrutura_bucket
from unittest.mock import MagicMock
from unittest.mock import patch


def test_valida_estrutura_bucket_sucesso(s3_mock, variaveis_teste):
    """
    Testa sucesso na execução.
    """
    
    # Arrange
    bucket_name = variaveis_teste["MAIN_BUCKET_NAME"]
    folder_project = variaveis_teste["PROJECT_FOLDER_NAME"]
    subfolders = variaveis_teste["PROJECT_FOLDER_LIST"]

    # Configura o mock para simular respostas do list_objects_v2
    with patch.object(s3_mock, "list_objects_v2") as mock_list_objects:
        # Primeiro, cria respostas para cada subpasta (indicam que cada uma existe)
        subfolder_responses = []
        for subfolder in subfolders:
            subfolder_responses.append({
                "KeyCount": 1,
                "Contents": [{"Key": f"{folder_project}/{subfolder}/dummy.txt"}]
            })
        
        # Depois, cria a resposta para a pasta do projeto (indica que ela existe)
        project_folder_response = {
            "KeyCount": 1,
            "Contents": [{"Key": f"{folder_project}/dummy.txt"}]
        }
        
        # Combina as respostas
        mock_list_objects.side_effect = [project_folder_response] + subfolder_responses

        # Act
        result = valida_estrutura_bucket(
            aws_conn_id="aws_default",
            bucket_name=bucket_name,
            folder_project_name=folder_project,
            subfolders_list=subfolders
        )

        # Assert
        assert result is True

def test_valida_estrutura_bucket_inexistente(s3_mock, variaveis_teste):
    """
    Testa falha na execução.
    """
    
    # Arrange
    bucket_name = "bucket.inexistente"
    folder_project = variaveis_teste["PROJECT_FOLDER_NAME"]
    subfolders = variaveis_teste["PROJECT_FOLDER_LIST"]

    # Configura o mock para simular bucket inexistente
    with patch.object(s3_mock, "head_bucket") as mock_head_bucket:
        mock_head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "head_bucket"
        )

        # Act & Assert
        with pytest.raises(ValueError) as exc:
            valida_estrutura_bucket(
                aws_conn_id="aws_default",
                bucket_name=bucket_name,
                folder_project_name=folder_project,
                subfolders_list=subfolders
            )

        assert str(exc.value) == variaveis_teste["ERRORS_DICT"]["ERROR_MAINBUCKET_MISSING"]

