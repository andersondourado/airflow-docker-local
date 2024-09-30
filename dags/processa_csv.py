# Bibliotecas do Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Biblotecas aux
import os
import pandas as pd
from datetime import datetime, timedelta


## Variáveis globais ##
# Diretório onde os arquivos serão procurados e processados
BASE_DIR = './dados'
INPUT_DIR = os.path.join(BASE_DIR, '')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

# Função para obter a data do dia anterior em formato YYYYMMDD, com base na data de referÊncia.
def get_yesterday_date(date_ref):
    date_ref = datetime.strptime(date_ref, '%Y-%m-%d')
    return (date_ref - timedelta(days=1)).strftime('%Y%m%d')


def valida_arquivo(**kwargs):
    '''
    Função para validar se o arquivo existe no diretório.
    Se o arquivo existir, vai para a task 'processa_csv'
    Se o arquivo não existir, vai para o 'fim' e finaliza a DAG
    '''
    # Recebe a data de execução da DAG
    date_exec = kwargs['ds']  # 'ds' é a data de execussão passada pelo Airflow, variável tipo String com a formtação 'YYYY-MM-DD'. Podemos usar a 'execution_date', porém a formatação é tipo datetime. Exemplo: '2024-09-28 00:00:00'
    # Pega a data de referência e retira 1 dia
    yesterday_date = get_yesterday_date(date_exec)
    # Nome esperado do arquivo
    filename = f'dados_clima_{yesterday_date}.csv'
    # Caminho completo do arquivo
    file_path = os.path.join(INPUT_DIR, filename)

    if os.path.exists(file_path):  # Se o arquivo existir, continue com o processamento
        message = f"O arquivo {filename} foi encontrado no diretório {INPUT_DIR}."
        print(message)  # Mensagem de log no console do Airflow
        # Salva o nome do arquivo em uma vaiável global do Airflow para usar na task/função processa_csv 
        kwargs['ti'].xcom_push(key='file_path', value=file_path)
        return 'processa_csv'  # Direciona para a task 'task_processa_csv'
    else:
        message = f"ATENÇÃO: O arquivo {filename} não foi encontrado no diretório {INPUT_DIR}."
        print(message)  # Mensagem de log de erro no console do Airflow
        return 'fim'  # Direciona para a task 'task_fim', pois o arquivo não foi encontrado

def processa_csv(**kwargs):
    '''
    Função para processar o CSV e gravar o arquivo processado/tratado na pasta de output dos dados
    '''
    ## Recebe a data de execução da DAG
    #date_exec = kwargs['ds']  # 'ds' é a data de execussão passada pelo Airflow, variável tipo String com a formtação 'YYYY-MM-DD'. Podemos usar a 'execution_date', porém a formatação é tipo datetime. Exemplo: '2024-09-28 00:00:00'
    ## Pega a data de referência e retira 1 dia
    #yesterday_date = get_yesterday_date(date_exec)
    ## Nome esperado do arquivo
    #filename = f'dados_clima_{yesterday_date}.csv'
    ## Caminho completo do arquivo
    #file_path = os.path.join(INPUT_DIR, filename)

    # Carrega o nome do arquivo na vaiável local file_path
    file_path = kwargs['ti'].xcom_pull(task_ids='valida_arquivo', key='file_path')

    # Leitura do arquivo CSV
    df = pd.read_csv(file_path)

    # Adiciona uma nova coluna com a data de processamento
    df['data_processamento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Salva o arquivo tratado no diretório de output
    output_file_path = os.path.join(OUTPUT_DIR, 'dados_clima_tratado.csv')
    df.to_csv(output_file_path, index=False)

    message = f"Arquivo {file_path} processado com SUCESSO."
    print(message)  # Mensagem de log no console do Airflow


# Definição da DAG
default_args = {
    'owner': 'airflow',  # Dado informativo, para fins de organização do Airflow
    'retries': 2,  # Indica o número de tentativas de re-execução caso uma task falhe. Nesse caso, se uma task falhar, ela será reexecutada uma vez antes de ser marcada como "failed" permanentemente.
    'retry_delay': timedelta(minutes=1),  # Define o tempo de espera entre cada tentativa de re-execução. No exemplo, há um intervalo de 1 minuto entre uma falha e a próxima tentativa de execução da task.
    # Outros parâmetros que podem ser usados no default_args ou na DAG:
    #- depends_on_past: Define se uma task depende do sucesso da execução anterior (na execução anterior da DAG). Se True, uma task não será executada se a execução anterior da DAG falhou ou foi ignorada.
    #- email_on_failure e email_on_retry: Configura se o Airflow deve enviar um e-mail para o owner ou para uma lista de e-mails quando a DAG ou task falhar ou for reexecutada.
    #- end_date:Define a data de término da DAG. A DAG não será mais agendada após essa data.
}

# A DAG será executada a cada minuto (* * * * *)
with DAG(
    'processa_dados_csv',  # id/nome da DAG
    default_args=default_args,
    description='DAG para processar arquivos CSV de clima',
    schedule_interval='* * * * *', # O schedule_interval define a periodicidade em que a DAG será executada. Ele pode ser configurado de várias formas: '@daily': Executa a DAG uma vez por dia. | '@hourly': Executa a DAG a cada hora. | '@once': Executa a DAG apenas uma vez. | Pode ser também uma instância de timedelta, como timedelta(days=1) para executar diariamente.
    start_date=days_ago(1),  # O start_date define a partir de qual data a DAG está habilitada para começar a executar.
    catchup=False  # O catchup define se o Airflow deve executar todas as execuções programadas que foram "perdidas" entre o start_date e o momento atual. Para evitar a execução retroativa de DAGs pendentes. Se a data de início for no passado e catchup estiver ativado, o Airflow tentará "recuperar" as execuções pendentes desde o start_date até a data atual.
    # Um ponto importante: a start_date não é retroativa — a DAG não vai executar para períodos anteriores ao valor definido aqui, mesmo que existam dados para isso.
) as dag:

    # Task de início
    task_inicio = PythonOperator(
        task_id='inicio',
        python_callable=lambda: print("Iniciando o processamento...")
    )

    # Task de validação do arquivo
    task_valida_arquivo = BranchPythonOperator(
        task_id='valida_arquivo',
        python_callable=valida_arquivo,
        # provide_context=True  # Não é mais necessário no Airflow 2.9.3
    )

    # Task de processamento do CSV
    task_processa_csv = PythonOperator(
        task_id='processa_csv',
        python_callable=processa_csv,
        # provide_context=True  # Não é mais necessário no Airflow 2.9.3
    )

    # Task final
    task_fim = DummyOperator(
        task_id='fim'
    )

    ## Definição do fluxo da DAG ##
    # O fluxo começa com 'inicio' e vai para 'valida_arquivo'
    task_inicio >> task_valida_arquivo
    # Se 'valida_arquivo' retornar 'processa_csv', executa 'processa_csv' e depois 'fim', se retornar 'fim' finaliza o processo
    task_valida_arquivo >> [task_fim, task_processa_csv]
    # Se 'valida_arquivo' retornar 'fim', finaliza o processo
    task_fim
    # Se 'valida_arquivo' retornar 'processa_csv', executa 'processa_csv' e depois 'fim'
    task_processa_csv >> task_fim
