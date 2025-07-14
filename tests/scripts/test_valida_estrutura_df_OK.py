import pytest
import pandas as pd
from script_worker_prata import valida_estrutura_df


def test_valida_estrutura_df_sucesso(variaveis_teste, mock_mensagens_de_erro):
    """
    Testa sucesso na validação do DataFrame.
    """

    # Arrange
    json_list = variaveis_teste.get("JSON_FIELDS_LIST")
    df_datatype = variaveis_teste.get("DF_DATATYPE_SCHEMA")

    data = {col: [None] for col in json_list}
    data["id"] = ["123"]
    data["name"] = ["Brewery A"]
    data["country"] = ["Brazil"]
    data["state"] = ["SP"]
    data["brewery_type"] = ["micro"]
    data["city"] = ["Sao Paulo"]
    data["postal_code"] = ["01000-000"]
    data["longitude"] = [-46.633309]
    data["latitude"] = [-23.550520]

    df = pd.DataFrame(data)

    print(f"Esquema esperado: {df_datatype}")
    print(f"DF de teste preparado: \n{df}")
    print(f"Colunas do DF de teste: {df.columns.tolist()}")

    # Act
    is_valid, item_id, df_result = valida_estrutura_df(df, json_list, df_datatype)

    # Assert
    print(f"Resultado: is_valid={is_valid}, item_id={item_id}, df_result=\n{df_result}")
    assert is_valid is True, f"Falha na validação: is_valid={is_valid}"
    assert item_id == "123"
    assert df_result is not None
    assert df_result.dtypes.to_dict() == df_datatype

    df_expected_after_astype = df.astype(df_datatype)
    pd.testing.assert_frame_equal(df_result, df_expected_after_astype, check_dtype=True)


def test_valida_estrutura_df_valores_nulos(variaveis_teste, mock_mensagens_de_erro):
    """
    Testa conversão de valores nulos para pd.NA.
    """

    # Arrange
    json_list = variaveis_teste.get("JSON_FIELDS_LIST")
    df_datatype = variaveis_teste.get("DF_DATATYPE_SCHEMA")

    data = {col: [None] for col in json_list}
    data["id"] = ["123"]
    data["name"] = ["None"]
    data["country"] = ["NULL"]
    data["state"] = ["N/A"]
    data["brewery_type"] = ["regional"]
    data["city"] = ["Curitiba"]
    data["postal_code"] = ["80000-000"]
    data["longitude"] = [-49.273]
    data["latitude"] = [-25.429]
    data["phone"] = [""]
    data["website_url"] = ["https://example.com"]
    data["street"] = ["Rua X"]

    df = pd.DataFrame(data)

    print(f"Esquema esperado: {df_datatype}")
    print(f"DF de teste (com nulos) preparado: \n{df}")

    # Act
    is_valid, item_id, df_result = valida_estrutura_df(df, json_list, df_datatype)

    # Assert
    print(f"Resultado: is_valid={is_valid}, item_id={item_id}, df_result=\n{df_result}")
    assert is_valid is True, f"Falha na validação: is_valid={is_valid}"
    assert item_id == "123"
    assert df_result is not None

    dtypes_result = {col: str(dtype) for col, dtype in df_result.dtypes.items()}
    dtypes_expected = {col: str(dtype) for col, dtype in df_datatype.items()}
    assert dtypes_result == dtypes_expected, f"Dtypes divergentes:\nEsperado: {dtypes_expected}\nObtido: {dtypes_result}"


def test_valida_estrutura_df_colunas_ausentes(variaveis_teste, mock_mensagens_de_erro):
    """
    Testa falha por colunas ausentes.
    """

    # Arrange
    json_list = variaveis_teste.get("JSON_FIELDS_LIST")
    df_datatype = variaveis_teste.get("DF_DATATYPE_SCHEMA")

    df = pd.DataFrame({
        "id": ["123"],
        "name": ["Brewery A"],
        "country": ["Brazil"]
        
    })

    print(f"DF de teste (colunas ausentes) preparado: \n{df}")
    print(f"Colunas do DF de teste: {df.columns.tolist()}")

    # Act
    is_valid, item_id, df_result = valida_estrutura_df(df, json_list, df_datatype)

    # Assert
    print(f"Resultado: is_valid={is_valid}, item_id={item_id}, df_result=\n{df_result}")
    assert is_valid is False, f"A validação deveria falhar para colunas ausentes, mas passou."
    assert item_id == "123"
    assert df_result is None, "O DataFrame resultante deveria ser None quando a validação falha."


def test_valida_estrutura_df_tipo_invalido(variaveis_teste, mock_mensagens_de_erro):
    """
    Testa falha por tipo de dado inválido.
    """

    # Arrange
    json_list = variaveis_teste.get("JSON_FIELDS_LIST")

    invalid_df_datatype = variaveis_teste.get("DF_DATATYPE_SCHEMA").copy()
    
    invalid_df_datatype["state"] = "int"

    data = {col: [None] for col in json_list}
    data["id"] = ["123"]
    data["name"] = ["Brewery A"]
    data["country"] = ["Brazil"]
    data["state"] = ["SP"]
    data["brewery_type"] = ["micro"]
    data["city"] = ["Florianopolis"]
    data["postal_code"] = ["88000-000"]
    data["longitude"] = [-48.548]
    data["latitude"] = [-27.594]
    data["phone"] = ["123456789"]
    data["website_url"] = ["https://another.com"]
    data["street"] = ["Rua Z"]

    df = pd.DataFrame(data)

    print(f"Esquema de dados (com tipo inválido) esperado: {invalid_df_datatype}")
    print(f"DF de teste (tipo inválido) preparado: \n{df}")

    # Act
    is_valid, item_id, df_result = valida_estrutura_df(df, json_list, invalid_df_datatype)

    # Assert
    print(f"Resultado: is_valid={is_valid}, item_id={item_id}, df_result=\n{df_result}")
    assert is_valid is False, "A validação deveria falhar para tipo de dado inválido, mas passou."
    assert item_id == "123"
    assert df_result is None, "O DataFrame resultante deveria ser None quando a validação falha."


def test_valida_estrutura_df_erro_inesperado(variaveis_teste, mock_mensagens_de_erro):
    """
    Testa erro inesperado na conversão de tipos.
    """

    # Arrange
    json_list = variaveis_teste.get("JSON_FIELDS_LIST")
    df_datatype = variaveis_teste.get("DF_DATATYPE_SCHEMA")

    data = {col: [None] for col in json_list}
    data["id"] = ["123"]
    data["name"] = ["Brewery A"]
    data["country"] = ["Brazil"]
    data["state"] = ["SP"]
    data["brewery_type"] = ["micro"]
    data["city"] = ["Rio de Janeiro"]
    data["postal_code"] = ["20000-000"]
    data["longitude"] = [-43.209]
    data["latitude"] = [-22.906]
    data["phone"] = ["987654321"]
    data["website_url"] = ["https://mybrewery.com"]
    data["street"] = ["Rua Alfa"]

    df = pd.DataFrame(data)

    print(f"DF de teste (erro inesperado) preparado: \n{df}")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(pd.DataFrame, "astype", lambda self, dtype: (_ for _ in ()).throw(ValueError("Erro de conversão simulado!")))

        # Act
        is_valid, item_id, df_result = valida_estrutura_df(df, json_list, df_datatype)

        # Assert
        print(f"Resultado: is_valid={is_valid}, item_id={item_id}, df_result=\n{df_result}")
        assert is_valid is False, "A validação deveria falhar devido ao erro simulado, mas passou."
        assert item_id == "123"
        assert df_result is None, "O DataFrame resultante deveria ser None quando a validação falha."
