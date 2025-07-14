import pytest
from airflow.models import Variable
import logging
from script_utils import mensagens_de_erro

# Configura o logger para testes
logger = logging.getLogger(__name__)

def test_mensagens_de_erro_sucesso(variaveis_teste, mock_airflow_variables):
    """
    Testa se a função mensagens_de_erro retorna corretamente o dicionário quando a variável ERROS_DICT está presente.
    """
    # A fixture mock_airflow_variables já configura Variable.get para retornar variaveis_teste
    resultado = mensagens_de_erro()
    
    # Verifica se o resultado é um dicionário
    assert isinstance(resultado, dict), "O resultado deve ser um dicionário"
    
    # Verifica se as chaves foram convertidas para minúsculas
    for key in resultado.keys():
        assert key.islower(), f"A chave '{key}' não está em minúsculas"
    
    # Verifica se os valores correspondem aos esperados
    expected_dict = {k.lower(): v for k, v in variaveis_teste["ERRORS_DICT"].items()}
    assert resultado == expected_dict, "O dicionário retornado não corresponde ao esperado"


def test_mensagens_de_erro_variavel_nao_encontrada(monkeypatch):
    """
    Testa se a função mensagens_de_erro levanta ValueError quando a variável ERROS_DICT não existe.
    """
    # Sobrescreve o comportamento de Variable.get para simular KeyError
    monkeypatch.setattr(Variable, "get", lambda *args, **kwargs: (_ for _ in ()).throw(KeyError("ERRORS_DICT")))
    
    with pytest.raises(ValueError) as exc_info:
        mensagens_de_erro()
    
    assert str(exc_info.value) == "Não foi possível carregar o dicionário de erros.", \
        "A mensagem de erro não corresponde à esperada"


def test_mensagens_de_erro_formato_invalido(monkeypatch):
    """
    Testa se a função mensagens_de_erro levanta ValueError quando a variável ERROS_DICT está malformada.
    """
    # Sobrescreve Variable.get para retornar um valor inválido (não um dicionário)
    monkeypatch.setattr(Variable, "get", lambda *args, **kwargs: "valor_invalido")
    
    with pytest.raises(ValueError) as exc_info:
        mensagens_de_erro()
    
    assert str(exc_info.value) == "Não foi possível carregar o dicionário de erros.", \
        "A mensagem de erro não corresponde à esperada"
