import pandas as pd
import matplotlib.pyplot as plt

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from scripts.download import baixar_e_extrair_dataset_kaggle
from scripts.process_up import processa_arquivo
from scripts.athena_up import execute_athena_query, check_query_execution, get_query_results

# Nome do dataset no Kaggle que você quer baixar
dataset_name = "akshaydattatraykhare/diabetes-dataset"
output_dir = '/diabetes-predict-indian/downloads'

def definir_classe(valor_target):
    """
    Define a classe de diabetes baseada no valor do target.

    Parameters:
    valor_target (int): O valor da coluna target, onde 1 indica diabético e 0 indica não diabético.

    Returns:
    str: 'diabetico' se valor_target for 1, 'nao_diabetico' caso contrário.
    """
    if valor_target == 1:
        return 'diabetico'
    else:
        return 'nao_diabetico'


def main():
    """
    Função principal que orquestra o download, processamento e análise do dataset de diabetes.
    Ela executa as seguintes operações:
    1. Baixa o dataset do Kaggle e extrai os arquivos.
    2. Processa o arquivo e salva no S3.
    3. Executa uma consulta no Amazon Athena e processa os resultados.
    4. Treina modelos de Machine Learning e avalia o desempenho dos modelos.
    5. Exibe a importância das colunas e as métricas de avaliação do modelo.
    """

    # Executa o processo de download e extração
    baixar_e_extrair_dataset_kaggle(dataset_name, output_dir)

    # Caminho do arquivo local da B3
    c_arquivo = output_dir + '/diabetes.csv'
    c_bucket = 'bucketml-cbd0711c-1d5c-4227-bfef-e2c9e8c55bdc'

    # Processar o arquivo e salvar no S3
    resultado = processa_arquivo(c_arquivo, c_bucket)
    print(f"Resultado do processamento: {resultado}")

    # Executar a consulta
    query_execution_id = execute_athena_query()
    print(f"Executando query com ID: {query_execution_id}")

    # Checar o status da query
    if check_query_execution(query_execution_id):
        # Obter os resultados da query em um DataFrame
        df_diabetes = get_query_results(query_execution_id)
        print(df_diabetes.head())  # Exibir as primeiras linhas do DataFrame
    else:
        print("A execução da query falhou ou foi cancelada.")

    print(df_diabetes.columns)
    df_diabetes = df_diabetes.rename(columns={
        'pregnancies': 'qtd_gravidez',
        'glucose': 'glicose',
        'bloodpressure': 'pressao_sanguinea',
        'skinthickness': 'gordura_subcutanea',
        'insulin': 'insulina',
        'bmi': 'imc',
        'diabetespedigreefunction': 'tendencia_diabetes',
        'age': 'idade',
        'outcome': 'target'
    })

    colunas_convert = ['qtd_gravidez','glicose','pressao_sanguinea','gordura_subcutanea','insulina','imc','tendencia_diabetes','idade','target']
    df_diabetes[colunas_convert] = df_diabetes[colunas_convert].apply(pd.to_numeric, errors='coerce')

    # df_diabetes.qtd_gravidez = pd.to_numeric(df_diabetes.qtd_gravidez, errors='coerce')
    print(df_diabetes.dtypes)

    indice_coluna_referencia = df_diabetes.columns.get_loc('idade')
    df_diabetes.insert(indice_coluna_referencia + 1, 'classe', '')
    df_diabetes['classe'] = df_diabetes['target'].apply(definir_classe)
    df_diabetes.head()

    x = df_diabetes[[
        'qtd_gravidez',
        'glicose',
        'pressao_sanguinea',
        'gordura_subcutanea',
        'insulina',
        'imc',
        'tendencia_diabetes',
        'idade',
    ]]

    y = df_diabetes['classe']

    # Treinar o modelo de RandomForest
    model = RandomForestClassifier()
    model.fit(x, y)

    # Visualizar a importância das colunas
    importances = model.feature_importances_
    feature_names = x.columns

    # Plotar as importâncias
    plt.figure(figsize=(10, 6))
    plt.barh(feature_names, importances)
    plt.xlabel('Importância')
    plt.title('Importância das Colunas no RandomForest')
    plt.show()

    # Dividindo o conjunto de dados em 70% para treinamento e 30% para teste
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3, random_state=23)

    # criando e treinando o modelo de regressão Logistica
    # modelo = LogisticRegression()
    modelo = LogisticRegression(solver='lbfgs', max_iter=3000)
    modelo.fit(x_train, y_train)

    # Fazendo previsões sobre o conjunto de dados de teste
    y_pred = modelo.predict(x_test)

    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, average="micro")
    recall = recall_score(y_test, y_pred, average="micro")
    f1 = f1_score(y_test, y_pred, average="micro")

    print("Accuracy", accuracy)
    print("Precision", precision)
    print("Recall", recall)
    print("F1 Score", f1)


if __name__ == "__main__":
    main()
