import boto3
import pandas as pd
from datetime import datetime
import tempfile
import os
import re


def processa_arquivo(c_arquivo, c_bucket) -> str:
    """
        Processa um arquivo CSV da B3, converte para formato Parquet e faz o upload para o Amazon S3.

        Esta função lê o arquivo CSV localizado em `cArquivo`, processa os dados, converte para Parquet
        e faz o upload para um bucket S3 especificado em `cBucket`. O nome do arquivo no S3 contém a
        data de processamento como partição.

        Parameters:
        cArquivo (str): Caminho do arquivo CSV a ser processado.
        cBucket (str): Nome do bucket S3 onde o arquivo Parquet será salvo.

        Returns:
        str: Mensagem de sucesso após o upload do arquivo Parquet para o S3.
        """

    # Ler o arquivo CSV usando pandas, ignorando a primeira linha e removendo colunas extras
    dados = pd.read_csv(c_arquivo, sep=',', decimal='.', encoding='latin1', usecols=range(9))

    dProcess = datetime.now().strftime('%Y-%m-%d')

    # Criar um arquivo temporário para salvar o Parquet
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as arquivo_teporario:
        parquet_arquivo = arquivo_teporario.name

    # Converter o DataFrame para Parquet
    dados.to_parquet(parquet_arquivo, index=False, engine='pyarrow')

    # Upload do arquivo Parquet para o S3 com partição diária
    s3 = boto3.client('s3')
    s3_chave = f'Dados_diabetes/diabetes_dados_{dProcess}.parquet'
    s3.upload_file(parquet_arquivo, c_bucket, s3_chave)

    # Remover o arquivo temporário
    os.remove(parquet_arquivo)

    return 'Dados convertidos para Parquet e salvos no S3 com partição diária.'


