import logging
from airflow.models import Variable
from airflow.utils.email import send_email
from jinja2 import Template
from script_utils import mensagens_de_erro


def enviar_email_customizado(context, tipo_evento: str) -> bool:
    """
    Description:
        Envia um e-mail customizado com base no tipo de evento, utilizando templates e lista de destinatários configurados no Airflow.

    Args:
        context (dict): Contexto do Airflow contendo informações da DAG, task e data de execução.
        tipo_evento (str): Tipo de evento que determina qual template de e-mail será usado.

    Returns:
        bool: Retorna True se o e-mail for enviado com sucesso.

    Raises:
        ValueError: Se a lista de destinatários não for válida ou o template de e-mail correspondente ao tipo de evento não for encontrado ou estiver mal configurado.
        Exception: Se ocorrer um erro inesperado durante o processo de envio do e-mail.
    """
    try:
        
        erros = mensagens_de_erro()
        
        logger = logging.getLogger("airflow.task")
        
        lista_destinatarios = Variable.get("EMAIL_LIST", deserialize_json=True)
        
        if not isinstance(lista_destinatarios, list):
            raise ValueError(erros['error_email_list'])

        lista_templates = Variable.get("EMAIL_TEMPLATE_LIST", deserialize_json=True)

        dag_id = context.get('dag').dag_id
        task_id = context.get('task_instance').task_id
        execution_date = context.get('logical_date') or context.get('execution_date')
        exec_str = execution_date.strftime("%Y-%m-%d %H:%M:%S")

        template = next((t for t in lista_templates if t['tipo_evento'].lower() == tipo_evento.lower()), None)
        
        if not template:
            raise ValueError(erros['error_email_template'])
        
        if "assunto_email" not in template or "corpo_email" not in template:
            raise ValueError(erros['error_email_template'])

        subject = Template(template["assunto_email"]).render(dag_id=dag_id, task_id=task_id, exec_str=exec_str)
        body = Template(template["corpo_email"]).render(dag_id=dag_id, task_id=task_id, exec_str=exec_str)

        send_email(to=lista_destinatarios, subject=subject, html_content=body)
        logger.info(f"[EMAIL] Enviado com sucesso: {tipo_evento} → {lista_destinatarios}")
        return True

    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise
