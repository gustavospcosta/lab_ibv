import os
import pytest
from airflow.models import DagBag

# Caminho absoluto para a pasta onde estão as DAGs
DAGS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "dags"))

def test_dag_integrity():
    """
    Testa se todas as DAGs do diretório estão:
    - Sem erro de importação;
    - Sem ciclos;
    - Com IDs únicos;
    - Incluindo a DAG principal chamada 'main'.
    """
    dag_bag = DagBag(dag_folder=DAGS_PATH, include_examples=False)

    # Verifica erros de importação
    assert not dag_bag.import_errors, f"Erros de importação encontrados: {dag_bag.import_errors}"

    # Verifica se ao menos uma DAG foi carregada
    assert len(dag_bag.dags) > 0, "Nenhuma DAG foi carregada."

    # Verifica se a DAG esperada está entre as DAGs carregadas
    assert "main" in dag_bag.dags, "A DAG 'main' não foi encontrada no DagBag."

    # Exibe as DAGs carregadas para debug
    print("DAGs carregadas:", list(dag_bag.dags.keys()))
