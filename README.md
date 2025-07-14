### Projeto INBEV ###
Este repositório foi criado para o desafio técnico da INBEV.
O código consiste em uma DAG do Airflow que processa dados obtidos através de uma API e os salva em arquivos do tipo parquet em um bucket no serviço S3 da AWS.
A DAG suporta execução agendada (a cada 30 minutos) e manual:
  -	Para executar manualmente:
    -	Coloque o arquivo json na diretório "bucket.warehouse/breweries.project/raw.data" (baixe o arquivo de exemplo "tests/filers/raw_list_20250707_104109.json" neste repositório)
    -	Acesse o bash do Airflow com o comando: "docker exec -it airflow-airflow-webserver-1 bash", substitua "airflow-airflow-webserver-1" pelo nome correto do seu container
    -	Excute o comando: ""airflow variables set MANUAL_FILE_PATH "breweries.project/raw.data/raw_list_20250707_104109.json""
    -	Em seguida, execute o comando: "airflow dags trigger -c '{"manual": true}' main"
    -	Após a execução manual, rode o comando: "airflow variables delete MANUAL_FILE_PATH"
    
Abaixo segue as instruções OBRIGATÓRIAS de infraestrutura para fazê-lo rodar:
-	Crie o usuário e conexão com permissão de leitura/escrita nos buckets do serviço S3:
  -	Acesse o IAM da AWS e crie seu usuário
  -	Após criar seu usuário, na seção "Sumário", clique na opção "Criar chave de acesso", em seguida escolha "Código Local" e no clique no botão "próximo", então salve sua SUA-CHAVE-DE-ACESSO e SEU-SEGREDO-DE-ACESSO  
  -	Acesse o IAM da AWS e crie um grupo de trabalho
  -	Associe a politica "AmazonS3FullAccess" ao seu grupo de trabalho
  -	Adicione o seu usuário ao grupo de trabalho

-	Crie a estrutura de diretórios no serviço S3 da AWS com as seguintes nomenclaturas:
  -	Bucket principal: bucket.warehouse
  -	Pasta raiz do projeto: breweries.project/
  -	Subpasta 1: aggregated.data
  -	Subpasta 2: raw.data
  -	Subpasta 3: processed.data
  -	Subpasta 4: clean.data
  -	Visão final dos diretórios: 
    -	bucket.warehouse/breweries.project/aggregated.data
    -	bucket.warehouse/breweries.project/raw.data
    -	bucket.warehouse/breweries.project/processed.data
    - bucket.warehouse/breweries.project/clean.data

-	Crie uma instância do Airflow no Docker utilizando os arquivos contidos neste repositório:
  -	No arquivo docker-compose.yaml, modifique as entradas com os valores corretos do seu serviço de email para habilitar o envio de emails de alerta:
    -	AIRFLOW__SMTP__SMTP_HOST:
    -	AIRFLOW__SMTP__SMTP_PORT:
    -	AIRFLOW__SMTP__SMTP_STARTTLS: "True"
    -	AIRFLOW__SMTP__SMTP_SSL: "False"
    -	AIRFLOW__SMTP__SMTP_USER:
    -	AIRFLOW__SMTP__SMTP_PASSWORD:
    -	AIRFLOW__SMTP__SMTP_MAIL_FROM:
    -	AIRFLOW__EMAIL__EMAIL_BACKEND:
  -	No arquivo docker-compose.yaml, modifique a entrada abaixo com o valor da sua FERNET KEY:
    -	AIRFLOW__CORE__FERNET_KEY:   
  -	No arquivo .env, modifique a entrada abaixo informando o diretório raiz (local) do projeto, por exemplo "=/home/SEU-USUARIO/airflow":
    -	AIRFLOW_PROJ_DIR=
  -	Importe as variáveis no Airflow Web UI, acesse:
    -	Menu "Admin", submenu "Variables"
    -	Clique no botão "Import Variables" e na janela que se abrirá, aponte o caminho para o arquivo json (baixe o arquivo "tests/filers/variables.json" neste repositório)
  -	Crie a conexão com a AWS no Airflow Web UI, acesse:
    -	Menu "Admin", submenu "Connections"
    -	Clique no botão com o simbolo " + "
    -	Na janela que se abrirá, preencha os campos:
      -	Connection Id: "aws_default" (não mude)
      -	Connection Type: "Amazon Web Services"
      -	AWS Access Key ID: SUA-CHAVE-DE-ACESSO
      -	AWS Secret Access Key: SEU-SEGREDO-DE-ACESSO
      -	Clique no botão "Save"

-	Valide a estrutura local dos diretórios e arquivos:
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/.env
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/docker-compose.yaml
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/dags/dag_main.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/scripts/script_email.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/scripts/script_worker_bronze.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/scripts/script_worker_prata.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/scripts/script_worker_gold.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/scripts/script_utils.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/dags/test_dag_main.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/conftest.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/test_deleta_arquivo_processado_OK.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/test_enviar_email_customizado_OK.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/test_mensagens_erro_OK.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/test_move_arquivo_processado_OK.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/test_pega_info_arquivo_OK.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/test_processa_arquivos_OK.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/test_processa_arquivos_parquet_OK.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/test_salva_dados_api_OK.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/test_salva_parquet_OK.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/test_valida_estrutura_bucket_OK.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/scripts/test_valida_estrutura_df_OK.py
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/files/raw_list_20250707_104109.json
  -	/home/SEU-USUARIO/DIRETORIO-PROJETO/tests/files/variables.json
