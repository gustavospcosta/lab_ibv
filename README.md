# Projeto Airflow

Este repositório foi criado para um desafio técnico. O projeto consiste em uma DAG (Directed Acyclic Graph) do Apache Airflow que processa dados de cervejarias obtidos por meio da API Open Brewery DB e os salva em arquivos Parquet no serviço S3 da AWS. A DAG suporta dois modos de execução: **agendada** (a cada 30 minutos) e **manual** (com base em um arquivo JSON fornecido pelo usuário).

---

## Visão Geral da DAG

A DAG, chamada `main`, é responsável por orquestrar o pipeline de dados, que inclui a obtenção, validação, transformação e armazenamento de dados no bucket S3. O fluxo de dados segue uma abordagem de camadas (Bronze, Silver, Gold), garantindo a integridade e a qualidade dos dados em cada etapa. A DAG também envia notificações por e-mail para eventos específicos, como início, sucesso ou falha de tarefas e da própria DAG.

### Objetivos da DAG
- **Obtenção de Dados**: Coleta dados de cervejarias da API Open Brewery DB (execução agendada) ou de um arquivo JSON fornecido (execução manual).
- **Validação**: Verifica a estrutura do bucket S3, a validade dos arquivos e a conformidade dos dados com um esquema predefinido.
- **Processamento**: Transforma os dados brutos em formatos limpos e agregados, salvando-os como arquivos Parquet.
- **Armazenamento**: Organiza os dados em pastas específicas no bucket S3 (`raw.data`, `clean.data`, `aggregated.data`, `processed.data`).
- **Notificações**: Envia e-mails customizados para informar o status da execução.

### Modos de Execução
- **Agendada**: A DAG é executada a cada 30 minutos, obtendo dados diretamente da API Open Brewery DB.
- **Manual**: O usuário fornece um arquivo JSON no bucket S3, que é processado pela DAG. Este modo é útil para testes ou ingestão de dados específicos.

---

## Estrutura do Repositório

O repositório contém os seguintes diretórios e arquivos principais:

- **dags/**: Contém o arquivo `dag_main.py`, que define a DAG principal.
- **scripts/**: Contém os scripts Python que implementam as tarefas da DAG:
  - `script_email.py`: Funções para envio de e-mails customizados.
  - `script_worker_bronze.py`: Funções para obtenção e salvamento de dados brutos.
  - `script_worker_prata.py`: Funções para validação e transformação de dados brutos em dados limpos.
  - `script_worker_gold.py`: Funções para agregação e salvamento de dados finais.
  - `script_utils.py`: Funções utilitárias, como validação de bucket e movimentação de arquivos.
- **tests/**: Contém testes unitários para todas as funções, além de arquivos de exemplo:
  - `tests/files/raw_list_20250707_104109.json`: Arquivo JSON de exemplo para execução manual.
  - `tests/files/variables.json`: Arquivo com variáveis de configuração do Airflow.
  - Arquivos de teste para cada função (ex.: `test_pega_info_arquivo_OK.py`, `test_processa_arquivos_OK.py`, etc.).
- **.env**: Arquivo de configuração de ambiente para o Docker.
- **docker-compose.yaml**: Arquivo de configuração do Docker para o Airflow.

---

## Fluxo de Informações (Downstream)

O fluxo de informações na DAG segue uma estrutura linear com validações em cada etapa. As tarefas são organizadas de forma que a saída de uma tarefa (via XCom) é usada como entrada para a próxima. Abaixo está o fluxo geral:

1. **Validação da Estrutura do Bucket** (`valida_estrutura_bucket`):
   - Verifica a existência do bucket principal (`bucket.warehouse`) e das subpastas do projeto (`breweries.project/raw.data`, `breweries.project/clean.data`, etc.).
   - Usa a função `valida_estrutura_bucket` do `script_utils.py`.
   - Se a estrutura for inválida, a DAG falha com a mensagem de erro definida em `ERRORS_DICT["ERROR_MAINBUCKET_MISSING"]` ou `ERRORS_DICT["ERROR_FOLDER_MISSING"]`.

2. **Verificação de Modo de Execução** (`check_manual_execution`):
   - Determina se a execução é manual ou agendada com base no parâmetro `manual` passado no trigger da DAG.
   - Armazena o tipo de execução (`manual` ou `agendada`) em XCom com a chave `work_type`.

3. **Obtenção de Dados**:
   - **Execução Agendada** (`salva_dados_api`):
     - Chama a API Open Brewery DB (`https://api.openbrewerydb.org/v1/breweries`).
     - Salva os dados brutos em um arquivo JSON no bucket S3 (`raw.data/raw_list_<timestamp>.json`).
     - Usa a função `salva_dados_api` do `script_worker_bronze.py`.
     - Validações:
       - Verifica se a resposta da API é uma lista válida.
       - Garante que o arquivo foi salvo corretamente no S3.
       - Levanta erros como `ERROR_API_TIMEOUT`, `ERROR_INVALIDAPI_RESPONSE` ou `ERROR_SAVING_FILE` em caso de falha.
     - Salva em XCom: `bronze_filepath_api` (caminho do arquivo JSON) e `timestamp_api` (timestamp do arquivo).
   - **Execução Manual** (`pega_info_arquivo`):
     - Lê o caminho do arquivo JSON fornecido pelo usuário via variável Airflow `MANUAL_FILE_PATH`.
     - Valida o nome do arquivo (deve seguir o padrão `raw_list_<timestamp>.json`) e verifica se ele existe no S3.
     - Usa a função `pega_info_arquivo` do `script_utils.py`.
     - Validações:
       - Verifica se a variável `MANUAL_FILE_PATH` existe (`ERROR_VARIAVEL_NOTFOUND`).
       - Valida o formato do nome do arquivo (`ERROR_VALIDATION_FAIL`).
       - Confirma a existência do arquivo no S3 (`ERROR_FILE_NOTFOUND`).
       - Verifica se o arquivo não está vazio (`ERROR_EMPTY_FILE`) e contém uma lista JSON válida (`ERROR_CHECK_FILE_CONTENT`).
     - Salva em XCom: `bronze_filepath_manual` (caminho do arquivo) e `timestamp_nome_arquivo` (timestamp extraído).

4. **Processamento de Dados Brutos (Camada Silver)** (`processa_arquivos`):
   - Lê o arquivo JSON do S3 (usando `bronze_filepath_manual` ou `bronze_filepath_api`).
   - Valida a estrutura do DataFrame com a função `valida_estrutura_df` do `script_worker_prata.py`.
   - Validações:
     - Garante que todas as colunas esperadas estão presentes (definidas em `JSON_FIELDS_LIST`).
     - Converte os tipos de dados conforme o esquema `DF_DATATYPE_SCHEMA`.
     - Substitui valores nulos (ex.: "None", "NULL", "N/A") por `pd.NA`.
     - Levanta erros como `ERROR_CHECK_FILE_CONTENT` (se o JSON não for uma lista) ou `ERROR_FILE_NOTFOUND` (se o arquivo não existir).
   - Salva o DataFrame validado como um arquivo Parquet em `clean.data/df_validado_<timestamp>.parquet`.
   - Usa a função `salva_parquet` do `script_utils.py`.
   - Salva em XCom: `silver_filepath` (caminho do arquivo Parquet).

5. **Agregação de Dados (Camada Gold)** (`processa_arquivos_parquet`):
   - Lê o arquivo Parquet da camada Silver (`silver_filepath`).
   - Gera dois arquivos Parquet agregados:
     - `df_aggregated.01_<timestamp>.parquet`: Contém dados agregados (ex.: contagem por estado).
     - `df_aggregated.02_<timestamp>.parquet`: Contém dados agregados (ex.: contagem por tipo de cervejaria).
   - Usa a função `processa_arquivos_parquet` do `script_worker_gold.py`.
   - Validações:
     - Verifica a existência do arquivo Parquet no S3 (`ERROR_FILE_NOTFOUND`).
     - Garante que os dados podem ser lidos corretamente.
     - Confirma que os arquivos agregados foram salvos com sucesso (`ERROR_SAVING_FILE`).
   - Salva os arquivos no diretório `aggregated.data`.

6. **Movimentação de Arquivo Processado** (`move_arquivo_processado`):
   - Move o arquivo JSON bruto da pasta `raw.data` para `processed.data` (apenas na execução manual).
   - Usa a função `move_arquivo_processado` do `script_utils.py`.
   - Validações:
     - Verifica a existência do arquivo de origem (`ERROR_FILE_NOTFOUND`).
     - Confirma que o arquivo foi movido corretamente.
     - Levanta `ERROR_XCOM_INFO` se o caminho do arquivo não estiver disponível em XCom.

7. **Envio de Notificações por E-mail**:
   - Envia e-mails para eventos como início da DAG, sucesso ou falha de tarefas, e sucesso ou falha da DAG.
   - Usa a função `enviar_email_customizado` do `script_email.py`.
   - Validações:
     - Verifica se a lista de destinatários (`EMAIL_LIST`) é válida (`ERROR_EMAIL_LIST`).
     - Garante que o template de e-mail existe (`ERROR_EMAIL_TEMPLATE`).
     - Trata erros de conexão com o serviço de e-mail (`ERROR_CONEXAO`).
   - Os templates de e-mail são definidos em `EMAIL_TEMPLATE_LIST` no arquivo `variables.json`.

---

## Tarefas da DAG

As tarefas da DAG são implementadas como operadores Python no Airflow, cada uma correspondendo a uma função específica:

1. **valida_estrutura_bucket_task**:
   - Função: `valida_estrutura_bucket`
   - Objetivo: Validar a existência do bucket e das subpastas.
   - Entrada: Variáveis `MAIN_BUCKET_NAME`, `PROJECT_FOLDER_NAME`, `PROJECT_FOLDER_LIST`.
   - Saída: `True` (se válido) ou levanta exceção.
   - Arquivo: `script_utils.py`

2. **check_manual_execution_task**:
   - Função: Verifica o modo de execução (`manual` ou `agendada`).
   - Objetivo: Determinar o fluxo da DAG (API ou arquivo manual).
   - Entrada: Parâmetro `manual` do trigger da DAG.
   - Saída: XCom com `work_type`.
   - Arquivo: Definido diretamente na DAG (`dag_main.py`).

3. **salva_dados_api_task** (execução agendada):
   - Função: `salva_dados_api`
   - Objetivo: Obter dados da API e salvá-los no S3.
   - Entrada: `API_URL`, `MAIN_BUCKET_NAME`, `PROJECT_FOLDER_NAME`, `RAWDATA_FOLDER_NAME`.
   - Saída: XCom com `bronze_filepath_api` e `timestamp_api`.
   - Arquivo: `script_worker_bronze.py`

4. **pega_info_arquivo_task** (execução manual):
   - Função: `pega_info_arquivo`
   - Objetivo: Validar e obter informações do arquivo JSON manual.
   - Entrada: Variável Airflow `MANUAL_FILE_PATH`, `MAIN_BUCKET_NAME`, `AWS_CONN_ID`.
   - Saída: XCom com `bronze_filepath_manual` e `timestamp_nome_arquivo`.
   - Arquivo: `script_utils.py`

5. **processa_arquivos_task**:
   - Função: `processa_arquivos`
   - Objetivo: Processar dados brutos, validar estrutura e salvar como Parquet.
   - Entrada: `bronze_filepath_manual` ou `bronze_filepath_api`, `JSON_FIELDS_LIST`, `DF_DATATYPE_SCHEMA`.
   - Saída: XCom com `silver_filepath`.
   - Arquivo: `script_worker_prata.py`

6. **processa_arquivos_parquet_task**:
   - Função: `processa_arquivos_parquet`
   - Objetivo: Agregar dados e salvar arquivos Parquet na camada Gold.
   - Entrada: `silver_filepath`, `MAIN_BUCKET_NAME`, `PROJECT_FOLDER_NAME`, `AGGREGATED_FOLDER_NAME`.
   - Saída: `True` (se bem-sucedido).
   - Arquivo: `script_worker_gold.py`

7. **move_arquivo_processado_task** (execução manual):
   - Função: `move_arquivo_processado`
   - Objetivo: Mover o arquivo JSON bruto para a pasta `processed.data`.
   - Entrada: `bronze_filepath_manual`, `MAIN_BUCKET_NAME`, `PROCESSED_FOLDER_NAME`.
   - Saída: `True` (se bem-sucedido).
   - Arquivo: `script_utils.py`

8. **enviar_email_task** (múltiplas instâncias):
   - Função: `enviar_email_customizado`
   - Objetivo: Enviar notificações por e-mail para eventos específicos.
   - Entrada: `EMAIL_LIST`, `EMAIL_TEMPLATE_LIST`, contexto da DAG.
   - Saída: `True` (se bem-sucedido).
   - Arquivo: `script_email.py`

---

## Instruções de Configuração

### Pré-requisitos
- Conta AWS com acesso ao S3 e IAM.
- Docker instalado localmente.
- Arquivo JSON de exemplo (`raw_list_20250707_104109.json`) para execução manual.
- Arquivo de variáveis (`variables.json`) para configuração do Airflow.

### Configuração da AWS
1. **Criação de Usuário e Conexão**:
   - Acesse o IAM da AWS e crie um usuário.
   - Gere uma chave de acesso:
     - Na seção "Sumário" do usuário, clique em "Criar chave de acesso".
     - Escolha "Código Local" e salve a **AWS Access Key ID** e a **AWS Secret Access Key**.
   - Crie um grupo de trabalho no IAM.
   - Associe a política `AmazonS3FullAccess` ao grupo.
   - Adicione o usuário ao grupo.

2. **Estrutura de Diretórios no S3**:
   - Crie o bucket principal: `bucket.warehouse`.
   - Crie a pasta raiz do projeto: `breweries.project/`.
   - Crie as subpastas:
     - `aggregated.data`
     - `raw.data`
     - `processed.data`
     - `clean.data`
   - Estrutura final:
     - `bucket.warehouse/breweries.project/aggregated.data`
     - `bucket.warehouse/breweries.project/raw.data`
     - `bucket.warehouse/breweries.project/processed.data`
     - `bucket.warehouse/breweries.project/clean.data`

### Configuração do Airflow
1. **Instalação via Docker**:
   - Clone o repositório e navegue até o diretório do projeto.
   - No arquivo `docker-compose.yaml`, configure as variáveis de e-mail:
     - `AIRFLOW__SMTP__SMTP_HOST`: Endereço do servidor SMTP.
     - `AIRFLOW__SMTP__SMTP_PORT`: Porta do servidor SMTP.
     - `AIRFLOW__SMTP__SMTP_STARTTLS`: `True`.
     - `AIRFLOW__SMTP__SMTP_SSL`: `False`.
     - `AIRFLOW__SMTP__SMTP_USER`: Usuário do serviço de e-mail.
     - `AIRFLOW__SMTP__SMTP_PASSWORD`: Senha do serviço de e-mail.
     - `AIRFLOW__SMTP__SMTP_MAIL_FROM`: E-mail remetente.
     - `AIRFLOW__EMAIL__EMAIL_BACKEND`: Backend de e-mail (ex.: `airflow.utils.email.send_email_smtp`).
   - Configure a `FERNET_KEY`:
     - Gere uma chave com `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`.
     - Adicione a chave em `AIRFLOW__CORE__FERNET_KEY`.
   - No arquivo `.env`, configure o diretório raiz do projeto:
     - `AIRFLOW_PROJ_DIR=/home/SEU-USUARIO/airflow`

2. **Importação de Variáveis**:
   - Acesse o Airflow Web UI (geralmente em `http://localhost:8080`).
   - Navegue até `Admin > Variables`.
   - Clique em "Import Variables" e selecione o arquivo `tests/files/variables.json`.

3. **Configuração da Conexão AWS**:
   - No Airflow Web UI, acesse `Admin > Connections`.
   - Clique no botão "+" para criar uma nova conexão.
   - Preencha os campos:
     - **Connection Id**: `aws_default`
     - **Connection Type**: `Amazon Web Services`
     - **AWS Access Key ID**: Sua chave de acesso.
     - **AWS Secret Access Key**: Sua chave secreta.
   - Clique em "Save".

### Estrutura Local de Arquivos
Valide a estrutura de diretórios no seu ambiente local:
- `/home/SEU-USUARIO/DIRETORIO-PROJETO/.env`
- `/home/SEU-USUARIO/DIRETORIO-PROJETO/docker-compose.yaml`
- `/home/SEU-USUARIO/DIRETORIO-PROJETO/dags/dag_main.py`
- `/home/SEU-USUARIO/DIRETORIO-PROJETO/scripts/script_email.py`
- `/home/SEU-USUARIO/DIRETORIO-PROJETO/scripts/script_worker_bronze.py`
- `/home/SEU-USUARIO/DIRETORIO-PROJETO/scripts/script_worker_prata.py`
- `/home/SEU-USUARIO/DIRETORIO-PROJETO/scripts/script_worker_gold.py`
- `/home/SEU-USUARIO/DIRETORIO-PROJETO/scripts/script_utils.py`
- `/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/dags/test_dag_main.py`
- `/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/conftest.py`
- Arquivos de teste em `/tests/scripts/` (ex.: `test_pega_info_arquivo_OK.py`, `test_processa_arquivos_OK.py`, etc.).
- Arquivos de exemplo em `/tests/files/` (`raw_list_20250707_104109.json`, `variables.json`).

---

## Executando a DAG

### Execução Manual
1. Coloque o arquivo JSON no bucket S3:
   - Diretório: `bucket.warehouse/breweries.project/raw.data`
   - Exemplo: Use o arquivo `tests/files/raw_list_20250707_104109.json`.
2. Acesse o bash do container Airflow:
   ```bash
   docker exec -it airflow-airflow-webserver-1 bash
3. Configure a variável `MANUAL_FILE_PATH`: `airflow variables set MANUAL_FILE_PATH "breweries.project/raw.data/raw_list_20250707_104109.json"`
4. Dispare a DAG: `airflow dags trigger -c '{"manual": true}' main`
5. Após a execução, remova a variável: `airflow variables delete MANUAL_FILE_PATH`

### Execução Agendada
- A DAG está configurada para executar automaticamente a cada 30 minutos.
- Certifique-se de que a conexão AWS (`aws_default`) e as variáveis do Airflow (definidas em `variables.json`) estão corretamente configuradas no Airflow Web UI.
- Não é necessário configurar variáveis manuais para este modo, pois a DAG obtém os dados diretamente da API Open Brewery DB.

---

## Testes Unitários

O repositório inclui testes unitários localizados em `/tests/scripts/`, cobrindo cenários de sucesso e falha para todas as funções principais:
- Validação de bucket: `test_valida_estrutura_bucket_OK.py`
- Obtenção de informações de arquivo: `test_pega_info_arquivo_OK.py`
- Processamento de dados brutos: `test_processa_arquivos_OK.py`
- Agregação de dados: `test_processa_arquivos_parquet_OK.py`
- Movimentação de arquivos: `test_move_arquivo_processado_OK.py`
- Envio de e-mails: `test_enviar_email_customizado_OK.py`
- Validação de DataFrame: `test_valida_estrutura_df_OK.py`
- Salvamento de Parquet: `test_salva_parquet_OK.py`
- Obtenção de dados da API: `test_salva_dados_api_OK.py`
- Mensagens de erro: `test_mensagens_erro_OK.py`

Para executar os testes: 
1. Acesse o bash do container Airflow: `docker exec -it airflow-airflow-webserver-1 bash`
2. Navegue até o diretório de testes: `cd /tests/scripts/`
3. Execute os testes (por exemplo): `pytest test_move_arquivo_processado_OK.py`

---

## Variáveis de Configuração

As variáveis de configuração estão definidas em `tests/files/variables.json` e devem ser importadas no Airflow Web UI. As principais variáveis são:
- `MAIN_BUCKET_NAME`: `bucket.warehouse`
- `PROJECT_FOLDER_NAME`: `breweries.project`
- `PROJECT_FOLDER_LIST`: `["aggregated.data", "raw.data", "processed.data", "clean.data"]`
- `RAWDATA_FOLDER_NAME`: `raw.data`
- `CLEANDATA_FOLDER_NAME`: `clean.data`
- `AGGREGATED_FOLDER_NAME`: `aggregated.data`
- `PROCESSED_FOLDER_NAME`: `processed.data`
- `API_URL`: `https://api.openbrewerydb.org/v1/breweries`
- `JSON_FIELDS_LIST`: Colunas esperadas (ex.: `id`, `name`, `brewery_type`)
- `DF_DATATYPE_SCHEMA`: Esquema de tipos de dados (ex.: `id: string`, `latitude: float64`)
- `EMAIL_LIST`: Lista de e-mails (ex.: `["gustavo490@gmail.com"]`)
- `EMAIL_TEMPLATE_LIST`: Templates de e-mail para eventos
- `ERRORS_DICT`: Mensagens de erro (ex.: `ERROR_FILE_NOTFOUND`, `ERROR_API_TIMEOUT`)

Para importar:
1. Acesse o Airflow Web UI: `http://localhost:8080`
2. Vá em `Admin > Variables`
3. Clique em `Import Variables` e selecione `tests/files/variables.json`

---

## Considerações Finais

O projeto oferece um pipeline robusto com validações em todas as etapas, garantindo a qualidade dos dados. A integração com o Airflow permite automação e monitoramento, enquanto os testes unitários asseguram a confiabilidade do código. Para depuração, consulte os logs no Airflow Web UI ou execute os testes unitários.

Se houver problemas, verifique:
- Conexão AWS no Airflow
- Estrutura do bucket S3
- Logs das tarefas no Airflow Web UI
- Resultados dos testes unitários
