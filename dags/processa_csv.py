# Bibliotecas
import os
from datetime import datetime
import pandas as pd

# Bibliotecas do AirFlow
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Configurações
DIRETORIO_BASE = './dados'
DIRETORIO_OUTPUT = './dados/output'
ARQUIVO_TEMPLATE = 'dados_clima_{data}.csv'

def valida_arquivo(**kwargs):
    '''
    Função: valida_arquivo()
    Descrição: Essa função Valida se o arquivo dados_clima_YYYYMMMDD.csv existe.
    Se o arquivo existir, aciona a proxima task 'processa_csv_task', se não existir, finaliza o processo sem erro.
    '''
    # Lógica da função que valida o arquivo e retorna o próximo passo
    data_execucao = kwargs['ds']  # Data de execução da DAG (YYYY-MM-DD)
    data_formatada = datetime.strptime(data_execucao, '%Y-%m-%d').strftime('%Y%m%d')
    
    arquivo_esperado = os.path.join(DIRETORIO_BASE, ARQUIVO_TEMPLATE.format(data=data_formatada))
    
    message = f"Valida se arquivo {arquivo_esperado} existe no diretório {DIRETORIO_BASE}."
    print(message)

    if not os.path.exists(arquivo_esperado):
        # Se o arquivo não existir, enviar mensagem ao Microsoft Teams
        message = f"O arquivo {arquivo_esperado} não foi encontrado no diretório {DIRETORIO_BASE}."
        payload = {
            "ATENCAO": message
        }
        #requests.post(TEAMS_WEBHOOK_URL, json=payload)
        print(payload)
        
        return 'fim_task'
    
    return 'processa_csv_task'  # Certifique-se de que este task_id existe no DAG

def processa_csv(**kwargs):
    '''
    Função: processa_csv()
    Descrição: Essa função carrega o arquivo dados_clima_YYYYMMMDD.csv em um dataframe do pandas 
    e grava em uma saída.
    Pose se aplicar alguma lógica de tratamento dos dados nessa etapa
    '''
    # Função que processa o CSV
    print("Inicio do processo do CSV")
    data_execucao = kwargs['ds']
    data_formatada = datetime.strptime(data_execucao, '%Y-%m-%d').strftime('%Y%m%d')
    
    arquivo_entrada = os.path.join(DIRETORIO_BASE, ARQUIVO_TEMPLATE.format(data=data_formatada))
    arquivo_saida = os.path.join(DIRETORIO_OUTPUT, 'dados_clima_tratado.csv')
    
    # Carregando o arquivo no dataframe Pandas
    df = pd.read_csv(arquivo_entrada)
    
    # Exemplo de transformação: Adicionando uma coluna com a data do processamento
    df['data_processamento'] = data_execucao
    
    # Salvando o arquivo transformado
    if not os.path.exists(DIRETORIO_OUTPUT):
        os.makedirs(DIRETORIO_OUTPUT)
    
    df.to_csv(arquivo_saida, index=False)

    message = f"O arquivo {arquivo_entrada} foi encontrado e processado com SUCESSO! O novo arquivo foi salvo no diretório {arquivo_saida}!"
    print(message)

with DAG(
    dag_id='processa_csv',
    description='DAG para processar arquivo de clima diariamente',
    start_date=days_ago(1),
    schedule_interval='* * * * *',  # Executa diariamente em um determinado horário. Pesquisar como configurar https://crontab.guru/#1_*_*_*_*
    catchup=False
) as dag:

    inicio_task = DummyOperator(
        task_id='inicio'
        )

    valida_arquivo_task = PythonOperator(
        task_id='valida_arquivo',  # Certifique-se de que este task_id está correto
        python_callable=valida_arquivo
    )

    processa_csv_task = PythonOperator(
        task_id='processa_csv_task',  # Certifique-se de que este task_id está correto
        python_callable=processa_csv
    )

    fim_task = DummyOperator(
        task_id='fim'        
    )

    inicio_task >> valida_arquivo_task >> processa_csv_task >> fim_task
