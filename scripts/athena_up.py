import boto3
import pandas as pd
import time

# Defina as variáveis
database = 'fiap_tech2_diabetes'  # Nome do banco de dados no Athena
query = 'SELECT * FROM dados_analise_diabetes'  # Query a ser executada

# Defina a região do seu Athena (ex: us-east-1)
REGION_NAME = 'us-east-1'


# Função para executar uma query no Athena
def execute_athena_query() -> str:
    """
    Executa uma consulta SQL no Amazon Athena.

    Esta função envia uma consulta SQL para ser executada no Amazon Athena e retorna o ID de execução da query.

    Returns:
    str: O ID de execução da query, necessário para verificar o status e obter os resultados.
    """

    athena_client = boto3.client('athena', region_name=REGION_NAME)
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': 's3://bucketml-cbd0711c-1d5c-4227-bfef-e2c9e8c55bdc/Dados_diabetes_temp'
            # Mesmo sem querer armazenar, o Athena precisa de um bucket temporário
        }
    )
    return response['QueryExecutionId']


# Função para verificar o status da execução da query
def check_query_execution(query_execution_id) -> bool:
    """
    Verifica o status de uma query executada no Amazon Athena.

    Esta função verifica repetidamente o status da execução de uma query no Athena até que a execução seja concluída
    ou falhe. Ela retorna True se a query for concluída com sucesso e False se a execução falhar ou for cancelada.

    Parameters:
    query_execution_id (str): O ID de execução da query a ser verificada.

    Returns:
    bool: True se a query for bem-sucedida, False se falhar ou for cancelada.
    """

    athena_client = boto3.client('athena', region_name=REGION_NAME)
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']

        if status == 'SUCCEEDED':
            print("Query concluída com sucesso.")
            return True
        elif status in ['FAILED', 'CANCELLED']:
            print(f"Query falhou ou foi cancelada com status: {status}")
            # return False

        print("Aguardando a execução da query...")
        time.sleep(2)


# Função para obter os resultados da query
def get_query_results(query_execution_id) -> pd.DataFrame:
    """
    Obtém os resultados de uma query executada no Amazon Athena.

    Esta função busca os resultados da query já executada no Athena e os retorna como um DataFrame do Pandas.

    Parameters:
    query_execution_id (str): O ID de execução da query cujos resultados devem ser obtidos.

    Returns:
    pd.DataFrame: Um DataFrame contendo os resultados da query, onde a primeira linha é ignorada, pois contém o cabeçalho.
    """

    athena_client = boto3.client('athena', region_name=REGION_NAME)
    results_paginator = athena_client.get_paginator('get_query_results')
    results_iter = results_paginator.paginate(QueryExecutionId=query_execution_id)

    column_info = []
    rows = []

    for results in results_iter:
        for row in results['ResultSet']['Rows']:
            rows.append([col.get('VarCharValue', None) for col in row['Data']])
        if not column_info:  # Primeira iteração, pegar os nomes das colunas
            column_info = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    # Converter para um DataFrame do Pandas
    return pd.DataFrame(rows[1:], columns=column_info)  # Ignorar a primeira linha que é o cabeçalho
