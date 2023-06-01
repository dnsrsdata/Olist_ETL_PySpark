import warnings
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import StringType, ArrayType

warnings.filterwarnings('ignore')

# Iniciando sessão no Spark
spark = SparkSession.builder.appName('Cluster').getOrCreate()

def load_data(path):
    """
    Função para carregar dados no Spark
    """
    return spark.read.csv(path, header=True, sep=',')

def transform_data(dataframe):
    """
    Função para transformar os dados
    """
    # Com os dados importados, vamos criar uma função para transformar os dados
    func_ceps = lambda cep: cep.split(' a ')
    func_remove_sufix = lambda cep: [cep[0].split('-')[0], cep[1].split('-')[0]]
    func_busca_todos_ceps = lambda cep: [cep for cep in range(int(cep[0]), 
                                                          int(cep[1])+1)]
    
    # Criando UDFs
    ceps_udf = udf(func_ceps, StringType())
    prefix_cep = udf(func_remove_sufix, StringType())
    todos_os_ceps = udf(func_busca_todos_ceps, ArrayType(StringType()))
    
    # Aplicando as funções
    dataframe = dataframe.withColumn('ceps', ceps_udf('postcode_range'))
    dataframe = dataframe.withColumn('cep_prefixes', prefix_cep('ceps'))
    dataframe = dataframe.withColumn('todos_cep_prefix', 
                                 todos_os_ceps('cep_prefixes'))
    
    # Criando uma linha com cada CEP
    dados_to_save = dataframe.select(dataframe.locality, 
                                 explode(dataframe.todos_cep_prefix) \
                                     .alias('cep_prefix'))
                       
    return dados_to_save

def main():
    
    if len(sys.argv) == 2:
        path_data = sys.argv[1]
        
        # Carregando os dados
        print('Carregando os dados...')
        df = load_data(path_data)
        
        # Transformando os dados
        print('Transformando os dados...')
        df = transform_data(df)
        
        # Salvando os dados
        print('Salvando os dados...')
        df.write.mode('overwrite').parquet('data/external/cep_prefixes_processed.parquet')
        
if __name__ == '__main__':
    main()
    
    



