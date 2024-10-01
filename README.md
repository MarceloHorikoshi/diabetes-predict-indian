# Projeto de Análise de Diabetes

Este projeto tem como objetivo realizar o download, processamento e análise de dados relacionados ao diabetes usando técnicas de aprendizado de máquina. O projeto utiliza um dataset do Kaggle, que é processado e armazenado no Amazon S3, além de consultas realizadas no Amazon Athena para a obtenção de informações. 

## Estrutura do Projeto

- **main.py**: Arquivo principal que orquestra o processo de download, processamento, e análise do dataset.
- **download.py**: Contém funções para baixar e extrair datasets do Kaggle.
- **process_up.py**: Processa um arquivo CSV, converte para Parquet, e faz o upload para o Amazon S3.
- **athena_up.py**: Executa consultas no Amazon Athena e processa os resultados.

## Pré-requisitos

- **Python 3.8+**
- Pacotes especificados no arquivo `requirements.txt`
- Credenciais configuradas para o **Amazon S3** e **Amazon Athena**
- Arquivo `kaggle.json` para autenticação na API do Kaggle na raíz do projeto

### Instalação

1. Clone este repositório.
   ```bash
   git clone https://seu_repositorio.git
   ```
2. Em um terminal, execute o comando a baixo para iniciar o processamento do código:   
   ```bash
   python main.py
   ```

3. Aguarde o processamento, em breve irá aparecer toda a executação do código com os resultados das análises.