import os
import sys
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_customers_table(path_customers, spark, cep_dataframe):
    """ Transforma a tabela de clientes.

    Args:
        path_customers (str): caminho para a tabela de clientes
        spark(SparkSession): sessão 
        cep_dataframe (DataFrame): dataframe com os dados de CEP
    """
    # carregando os dados
    customers = spark.read.csv(path_customers, header=True)
    
    # Criando views temporárias com os dados
    cep_dataframe.createOrReplaceTempView("ceps")
    customers.createOrReplaceTempView("customers")
    
    # Limpando a tabela de clientes
    customers = spark.sql("SELECT cs.customer_id,  \
                            cs.customer_unique_id, \
                            cs.customer_zip_code_prefix, \
                            LOWER(cp.locality) AS customer_city,  \
                            LOWER(cs.customer_state) AS customer_state \
                            FROM customers cs  \
                            LEFT JOIN ceps cp  \
                            ON CAST(cs.customer_zip_code_prefix AS INT) = cp.cep_prefix")
    
    return customers

def transform_geolocation_table(path_geolocation, spark, cep_dataframe):
    """ Transforma a tabela de geolocalização.

    Args:
        path_geolocation (str): caminho para a tabela de geolocalização
        spark(SparkSession): sessão 
        cep_dataframe (DataFrame): dataframe com os dados de CEP
    """
    # carregando os dados
    geolocation = spark.read.csv(path_geolocation, header=True)
    
    # Criando views temporárias com os dados
    cep_dataframe.createOrReplaceTempView("ceps")
    geolocation.createOrReplaceTempView("geolocation")
    
    # Limpando a tabela de geolocalização
    geolocation = spark.sql("SELECT g.geolocation_zip_code_prefix,  \
                            CAST(g.geolocation_lat AS DOUBLE),  \
                            CAST(g.geolocation_lng AS DOUBLE),  \
                            LOWER(cp.locality) AS geolocation_city, \
                            g.geolocation_city AS geolocation_city_original, \
                            LOWER(geolocation_state) AS geolocation_state \
                            FROM geolocation AS g  \
                            LEFT JOIN ceps cp  \
                            ON CAST(g.geolocation_zip_code_prefix AS INT) = cp.cep_prefix")
    
    # Como a coluna geolocation_city_CEP possui valores missing, terei que inputar 
    # os valores originais da coluna geolocation_city. Para isso, irei dividir a
    # tabela em duas, uma com os valores missing e outra com os valores não missing
    # e depois concatená-las

    geolocation_com_missing = geolocation.filter("geolocation_city IS NULL")
    geolocation = geolocation.filter("geolocation_city IS NOT NULL")
    geolocation_com_missing = geolocation_com_missing.withColumn('geolocation_city', 
                                                                 col("geolocation_city_original"))
    geolocation = geolocation.union(geolocation_com_missing)

    # Também será necessário dropar a coluna original, que contém inconsistencias 
    geolocation = geolocation.drop(col('geolocation_city_original'))

    return geolocation

def transform_order_items_table(path_order_items, spark):
    """ Transforma a tabela de itens de pedidos.

    Args:
        path_order_items (str): caminho para a tabela de itens de pedidos
        spark(SparkSession): sessão 
    """
    # carregando os dados
    order_items = spark.read.csv(path_order_items, header=True)
    
    # Criando views temporárias com os dados
    order_items.createOrReplaceTempView("order_items")
    
    # Limpando a tabela de itens de pedidos
    order_items = spark.sql("SELECT order_id, \
                          order_item_id, \
                          product_id, \
                          seller_id, \
                          CAST(shipping_limit_date AS TIMESTAMP), \
                          CAST(price AS FLOAT), \
                          CAST(freight_value AS FLOAT)  \
                          FROM order_items")
    
    return order_items

def transform_order_payments_table(path_order_payments, spark):
    """ Transforma a tabela de pagamentos de pedidos.

    Args:
        path_order_payments (str): caminho para a tabela de pagamentos de pedidos
        spark(SparkSession): sessão 
    """
    # carregando os dados
    order_payments = spark.read.csv(path_order_payments, header=True)
    
    # Criando views temporárias com os dados
    order_payments.createOrReplaceTempView("order_payments")
    
    # Limpando a tabela de pagamentos de pedidos
    order_payments = spark.sql("SELECT order_id, \
                             CAST(payment_sequential AS INT), \
                             payment_type, \
                             CAST(payment_installments AS INT), \
                             CAST(payment_value AS FLOAT)  \
                            FROM order_payments")
    
    return order_payments

def transform_orders_table(path_orders, spark):
    """ Transforma a tabela de pedidos.

    Args:
        path_orders (str): caminho para a tabela de pedidos
        spark(SparkSession): sessão 
    """
    # carregando os dados
    orders = spark.read.csv(path_orders, header=True)
    
    # Criando views temporárias com os dados
    orders.createOrReplaceTempView("orders")
    
    # Limpando a tabela de pedidos
    orders = spark.sql("SELECT order_id,  \
                     customer_id,  \
                     order_status,  \
                     CAST(order_purchase_timestamp AS TIMESTAMP),  \
                     CAST(order_approved_at AS TIMESTAMP),  \
                     CAST(order_delivered_carrier_date AS TIMESTAMP),  \
                     CAST(order_delivered_customer_date AS TIMESTAMP),  \
                     CAST(order_estimated_delivery_date AS DATE)  \
                    FROM orders")
    
    return orders

def transform_products_table(path_products, spark):
    """ Transforma a tabela de produtos.

    Args:
        path_products (str): caminho para a tabela de produtos
        spark(SparkSession): sessão 
    """
    # carregando os dados
    products = spark.read.csv(path_products, header=True)
    
    # Criando views temporárias com os dados
    products.createOrReplaceTempView("products")
    
    # Limpando a tabela de produtos
    products = spark.sql("SELECT product_id,  \
                        LOWER(product_category_name) AS product_category_name,  \
                        CAST(product_name_lenght AS INT),  \
                        CAST(product_description_lenght AS INT),  \
                        CAST(product_photos_qty AS INT),  \
                        CAST(product_weight_g AS INT),  \
                        CAST(product_length_cm AS INT),  \
                        CAST(product_height_cm AS INT),  \
                        CAST(product_width_cm AS INT)  \
                      FROM products")
    
    return products

def transform_sellers_table(path_sellers, spark, cep_dataframe):
    """ Transforma a tabela de vendedores.

    Args:
        path_sellers (str): caminho para a tabela de vendedores
        spark(SparkSession): sessão 
        cep_dataframe (DataFrame): dataframe com os dados de CEP
    """
    # carregando os dados
    sellers = spark.read.csv(path_sellers, header=True)
    
    # Criando views temporárias com os dados
    sellers.createOrReplaceTempView("sellers")
    cep_dataframe.createOrReplaceTempView("ceps")
    
    # Normalizando os valores da coluna seller_state
    sellers = spark.sql("SELECT s.seller_id,  \
                        s.seller_zip_code_prefix,  \
                        LOWER(cp.locality) AS seller_city,  \
                        LOWER(s.seller_city) AS seller_city_original,  \
                        LOWER(seller_state) AS seller_state  \
                        FROM sellers  s\
                        LEFT JOIN ceps cp  \
                        ON CAST(s.seller_zip_code_prefix AS INT) = cp.cep_prefix")

    # Como a nova coluna sellers_city possui valores missing, terei que inputar 
    # os valores originais da coluna sellers_city onde há falta. Para isso, irei 
    # dividir a tabela em duas, uma com os valores missing e outra com os valores 
    # não missing e depois concatená-las

    sellers_com_missing = sellers.filter("seller_city IS NULL")
    sellers = sellers.filter("seller_city IS NOT NULL")
    sellers_com_missing = sellers_com_missing.withColumn('seller_city', col("seller_city_original"))
    sellers = sellers.union(sellers_com_missing)

    # Também será necessário dropar a coluna original, que contém inconsistencias 
    sellers = sellers.drop(col('seller_city_original'))

    return sellers

def load_data_to_S3(path, bucketname):
    """Carrega os dados para o S3

    Args:
        path (str): caminho da pasta com os dados
        bucketname (str): nome do bucket
    """
    s3 = boto3.client('s3')
    for root,dirs,files in os.walk(path):
        for file in files:
            s3.upload_file(os.path.join(root,file),
                           bucketname, 
                           'raw/' + root[root.find('m/') + 2:])

def main():
    
    if len(sys.argv) == 9:
    
        # Iniciando sessão no Spark
        spark = SparkSession.builder.appName('Cluster').getOrCreate()

        # Salvando os paths
        customers_path, geolocation_path, order_items_path, order_payments_path, \
        orders_path, products_path, sellers_path, ceps_path = sys.argv[1:]
        
        # carrega os dados de CEP
        print("Carregando os dados de CEP")
        cep_dataframe = spark.read.parquet(ceps_path)
        
        
        # Transformando as tabelas
        print("Transformando as tabelas...")
        customers = transform_customers_table(customers_path, spark, cep_dataframe)
        geolocation = transform_geolocation_table(geolocation_path, spark, cep_dataframe)
        order_items = transform_order_items_table(order_items_path, spark)
        order_payments = transform_order_payments_table(order_payments_path, spark)
        orders = transform_orders_table(orders_path, spark)
        products = transform_products_table(products_path, spark)
        sellers = transform_sellers_table(sellers_path, spark, cep_dataframe)
        print("Tabelas transformadas com sucesso!")
        
        # Salvando as tabelas
        print("Salvando as tabelas...")
        customers.write.mode('overwrite').parquet("data/interim/customers.parquet")
        geolocation.write.mode('overwrite').parquet("data/interim/geolocation.parquet")
        order_items.write.mode('overwrite').parquet("data/interim/order_items.parquet")
        order_payments.write.mode('overwrite').parquet("data/interim/order_payments.parquet")
        orders.write.mode('overwrite').parquet("data/interim/orders.parquet")
        products.write.mode('overwrite').parquet("data/interim/products.parquet")
        sellers.write.mode('overwrite').parquet("data/interim/sellers.parquet")
        print("Tabelas salvas com sucesso!")
        
        # Buscando o caminho das pastas
        diretorios = [root for root,dirs,files in os.walk('data/interim')]
        print(diretorios)

        # Carregando para o S3
        print("Carregando os dados para o S3...")
        for diretorio in diretorios:
            load_data_to_S3(diretorio, 
                            "elasticbeanstalk-sa-east-1-239752289020")
        print("Dados carregados com sucesso!")
    
if __name__ == "__main__":
    main()
    