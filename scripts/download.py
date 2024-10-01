import kaggle
import os


def configurar_kaggle_api() -> None:
    """
    Verifica e configura a autenticação da API do Kaggle.

    Esta função verifica se o arquivo `kaggle.json` está no local correto
    para autenticação com a API do Kaggle. Se o arquivo não estiver no diretório padrão
    (~/.kaggle), ele é movido para lá, configurando assim as permissões corretas.

    Raises:
    FileNotFoundError: Se o arquivo `kaggle.json` não for encontrado no diretório especificado.

    """

    # Verifica se o arquivo kaggle.json está na raiz do projeto
    kaggle_json_path = os.path.join(os.getcwd(), '../kaggle.json')

    # Caminho onde o Kaggle espera o arquivo (~/.kaggle/kaggle.json)
    kaggle_dir = os.path.join(os.path.expanduser('~'), '.kaggle')
    dest_kaggle_json_path = os.path.join(kaggle_dir, '../kaggle.json')

    # Se o arquivo não estiver no local correto, move para lá
    if not os.path.exists(dest_kaggle_json_path):
        if not os.path.exists(kaggle_dir):
            os.makedirs(kaggle_dir)

        if os.path.exists(kaggle_json_path):
            # Copiar o kaggle.json da raiz do projeto para ~/.kaggle/
            with open(kaggle_json_path, 'r') as f:
                with open(dest_kaggle_json_path, 'w') as df:
                    df.write(f.read())
            os.chmod(dest_kaggle_json_path, 0o600)
            print(f'Arquivo kaggle.json copiado para {dest_kaggle_json_path}')
        else:
            raise FileNotFoundError(f"Arquivo kaggle.json não encontrado em {kaggle_json_path}")

    kaggle.api.authenticate()


# Baixar o dataset diretamente via API do Kaggle
def baixar_e_extrair_dataset_kaggle(dataset_name: str, output_dir: str) -> None:
    """
    Faz o download e a extração de um dataset do Kaggle.

    Esta função baixa e extrai um dataset específico do Kaggle utilizando a API do Kaggle.
    O dataset é extraído diretamente para o diretório de saída especificado.

    Parameters:
    dataset_name (str): O nome do dataset no Kaggle a ser baixado.
    output_dir (str): O diretório de destino onde o dataset será extraído.

    """
    configurar_kaggle_api()
    print(f"Baixando o dataset {dataset_name}...")
    kaggle.api.dataset_download_files(dataset_name, path=output_dir, unzip=True)
    print(f"Dataset {dataset_name} baixado e extraído com sucesso em {output_dir}")
