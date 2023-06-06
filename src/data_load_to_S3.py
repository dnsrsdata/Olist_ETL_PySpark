import boto3
import sys

def load_data_to_S3(path, bucket_name, key):
    """Carrega os dados para o S3

    Args:
        path (str): caminho do arquivo
        bucket_name (str): nome do bucket para onde o dado será enviado_
        key (str): nome do arquivo no bucket
    """
    s3 = boto3.client('s3')
    s3.upload_file(path, bucket_name, key)
    
    return print('Arquivo carregado com sucesso!')

def main():
    
    if len(sys.argv) == 4:
        path, bucket_name, key = sys.argv[1:]
        print('Carregando os dados...')
        load_data_to_S3(path, bucket_name, key)
    
    
if __name__ == '__main__':
    main()