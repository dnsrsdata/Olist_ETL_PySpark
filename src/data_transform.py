# Autor: Daniel Soares
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, month, hour, datediff, dayofweek
from pyspark.sql.types import StringType

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

def load_reviews_table(path_reviews, spark):
    """carrega os dados da tabela reviews

    Args:
        path_reviews (str): path da tabela reviews
        spark (SparkSession): Sessão spark
    """
    # carregando os dados
    reviews = spark.read.csv(path_reviews, header=True)
    
    return reviews

def load_product_categories_table(path_product_categories, spark):
    """carrega os dados da tabela product_categories

    Args:
        path_product_categories (str): path da tabela product_categories
        spark (SparkSession): Sessão spark
    """
    # carregando os dados
    product_category = spark.read.csv(path_product_categories, header=True)
    
    return product_category

def create_region_column(table, col_with_state, new_col_name):
    """Cria a coluna de região

    Args:
        table (SparkDf): tabela que será criada a 
        col_with_state (str): coluna com o estado
        new_col_name (str): nome da nova coluna
        
    Returns:
        SparkDf: tabela com a nova coluna
    """
    # Criando lista de regiões  
    estados_norte = ['ac', 'am', 'ro', 'rr', 'ap', 'to', 'pa']
    estados_nordeste = ['ma', 'pi', 'ce', 'rn', 'pb', 'pe', 'al', 'ba', 'se']
    estados_centro = ['mt', 'ms', 'df', 'go']
    estados_sudeste = ['mg', 'sp', 'es', 'rj']
    estados_sul = ['pr', 'sc', 'rs']

    # Criando função 
    estados_binarizer_func = lambda estado: 'norte' if estado in estados_norte else \
                            ('nordeste' if estado in estados_nordeste else \
                            ('centro-oeste' if estado in estados_centro else \
                            ('sudeste' if estado in estados_sudeste else 'sul')))

    estados_binarizer_func_udf = udf(estados_binarizer_func, StringType())
    
    table = table.withColumn(new_col_name, 
                             estados_binarizer_func_udf(col_with_state))
    
    return table

def binarizer_product_category(table, col_with_category, new_col_name):
    """Binariza a coluna de categoria de produtos

    Args:
        table (SoarkDF): tabela que será criada a coluna
        col_with_category (str): coluna com a categoria de produtos
        new_col_name (str): nome da nova coluna
        
    Returns:
        SparkDf: tabela com a nova coluna
    """
    # Criando novas categorias
    eletronicos_e_tecnologia = ['pcs', 'pc_gamer', 'tablets_impressao_imagem', 
                                'telefonia_fixa', 'telefonia', 
                                'informatica_acessorios', 'eletronicos', 'audio', 
                                'consoles_games', 'dvds_blu_ray']

    moda_e_acessorios = ['fashion_roupa_masculina', 'fashion_roupa_feminina',
                        'fashion_roupa_infanto_juvenil',
                        'fashion_underwear_e_moda_praia',
                        'fashion_bolsas_e_acessorios', 'fashion_calcados',
                        'fashion_esporte']

    casa_e_decoracao = ['moveis_decoracao', 'moveis_colchao_e_estofado', 
                        'moveis_cozinha_area_de_servico_jantar_e_jardim', 
                        'moveis_quarto', 'moveis_sala', 'utilidades_domesticas', 
                        'cama_mesa_banho', 'casa_construcao', 'casa_conforto', 
                        'casa_conforto_2']

    livros_e_educacao = ['livros_tecnicos', 'livros_importados', 
                        'livros_interesse_geral']

    beleza_e_saude = ['beleza_saude', 'fraldas_higiene', 'perfumaria']

    brinquedos_e_jogos = ['brinquedos', 'jogos', 'instrumentos_musicais']

    alimentos_e_bebidas = ['alimentos_bebidas', 'bebidas', 'alimentos']

    artigos_para_festas = ['artigos_de_festas', 'artigos_de_natal']

    arte_e_artesanato = ['artes', 'cine_foto', 'artes_e_artesanato']

    ferramentas_e_construcao = ['construcao_ferramentas_construcao', 
                                'construcao_ferramentas_seguranca', 
                                'construcao_ferramentas_jardim', 
                                'construcao_ferramentas_iluminacao', 
                                'construcao_ferramentas_ferramentas']

    esporte_e_lazer = ['esporte_lazer']

    outros = ['cool_stuff', 'flores', 'industria_comercio_e_negocios', 
            'malas_acessorios', 'seguros_e_servicos', 'market_place', 
            'relogios_presentes', 'papelaria', 'climatizacao', 
            'sinalizacao_e_seguranca', 'agro_industria_e_comercio', 
            'cds_dvds_musicais', 'musica', 'eletroportateis']
    
    # Criando a função
    produto_binarizer_func = lambda categoria: 'eletronicos e tecnologia' if categoria in eletronicos_e_tecnologia else \
                                              ('moda e acessorios' if categoria in moda_e_acessorios else \
                                              ('casa e decoracao' if categoria in casa_e_decoracao else \
                                              ('livros e educacao' if categoria in livros_e_educacao else \
                                              ('beleza e saude' if categoria in beleza_e_saude else \
                                              ('brinquedos e jogos' if categoria in brinquedos_e_jogos else \
                                              ('alimentos_e_bebidas' if categoria in alimentos_e_bebidas else \
                                              ('artigos para festas' if categoria in artigos_para_festas else \
                                              ('arte e artesanato' if categoria in arte_e_artesanato else \
                                              ('ferramentas e construcao' if categoria in ferramentas_e_construcao else \
                                              ('esporte e lazer' if categoria in esporte_e_lazer else 'outros'))))))))))   

    produto_binarizer_func_udf = udf(produto_binarizer_func, StringType())

    # Criando nova coluna
    table = table.withColumn(new_col_name, 
                             produto_binarizer_func_udf(col_with_category))


    return table

def create_days_of_week_column(table, col_with_date, new_col_name):
    """Cria dados referente aos dias da semana

    Args:
        table (SparkDF): tabela que será criada a coluna
        col_with_date (str): coluna com a data
        new_col_name (str): nome da nova coluna
        
    Returns:
        SparkDF: tabela com a nova coluna
    """
    # Obtendo o dia da semana da compra
    table = table.withColumn(new_col_name, 
                               dayofweek(col_with_date))

    # Criando uma função para obter o nome do dia da semana
    days_num_func = lambda dia: 'sunday' if dia == 1 else  \
            ('monday' if dia == 2 else  \
            ('tuesday' if dia == 3 else  \
            ('wednesday' if dia == 4 else  \
            ('thursday' if dia == 5 else  \
            ('friday' if dia == 6 else 'saturday')))))

    days_num_func_udf = udf(days_num_func, StringType())

    # Aplicando a função
    table = table.withColumn('order_name_day_of_week', 
                               days_num_func_udf('order_day_of_week'))
    
    
    return table

def create_month_column(table, col_with_date, new_col_name):
    """Cria dados referente ao mês

    Args:
        table (SparkDF): tabela que será criada a coluna
        col_with_date (str): coluna com a data
        new_col_name (str): nome da nova coluna
        
    Returns:
        SparkDF: tabela com a nova coluna
    """
    # Obtendo o mês da compŕa
    table = table.withColumn(new_col_name, month(col_with_date))

    # Criando uma função para nomear os meses
    mes_num_func = lambda mes: 'january' if mes == 1 else  \
            ('february' if mes == 2 else  \
            ('march' if mes == 3 else  \
            ('april' if mes == 4 else  \
            ('may' if mes == 5 else  \
            ('june' if mes == 6 else  \
            ('july' if mes == 7 else  \
            ('august' if mes == 8 else  \
            ('september' if mes == 9 else  \
            ('october' if mes == 10 else  \
            ('november' if mes == 11 else 'december'))))))))))

    mes_num_func_udf = udf(mes_num_func, StringType())

    # Aplicando a função
    table = table.withColumn('order_name_month', mes_num_func_udf('order_month'))


    return table

def create_new_features(customers_table, sellers_table, products_table, products_category_table, orders_table):
    """Cria novas features a partir dos datasets

    Args:
        customers_table (Spark DataFrame): tabela de clientes
        sellers_table (Spark Dataframe): tabela de vendedores
        products_table (Spark Dataframe): tabela de produtos
        products_category_table (Spark Dataframe): tabela de categorias de produtos
        orders_table (Spark Dataframe): tabela de pedidos
    """
    # Criando coluna de região para os clientes e vendedores
    customers_table = create_region_column(customers_table, 'customer_state', 'customer_region')
    sellers_table = create_region_column(sellers_table, 'seller_state', 'seller_region')
    products_category_table = binarizer_product_category(products_category_table, 'product_category_name', 'sub_product_category')
    
    # Alterando a unidade de medida das colunas
    products_table = products_table.withColumn('product_weight_kg', col('product_weight_g')/1000)
    products_table = products_table.withColumn('product_length_m', col('product_length_cm')/100)
    products_table = products_table.withColumn('product_height_m', col('product_height_cm')/100)
    products_table = products_table.withColumn('product_width_m', col('product_width_cm')/100)

    # Criando novos dados a partir de colunas de data
    orders_table = create_days_of_week_column(orders_table, 'order_purchase_timestamp', 'order_day_of_week')
    orders_table = create_month_column(orders_table, 'order_purchase_timestamp', 'order_month')
    orders_table = orders_table.withColumn('order_delivery_days', datediff('order_delivered_customer_date', 'order_purchase_timestamp'))
    orders_table = orders_table.withColumn('order_hour', hour('order_purchase_timestamp'))
    orders_table = orders_table.withColumn('order_processing_days', datediff('order_approved_at', 'order_purchase_timestamp'))
    
    return customers_table, sellers_table, products_table, products_category_table, orders_table

def save_data(customers_df, geolocalization_df, order_items_df, order_payments_df, order_reviews_df, orders_df, products_df, sellers_df, product_category_df):
    """Salva os dados processados em parquet.
    """
    
    customers_df.write.mode('overwrite').parquet('data/processed/customers.parquet')
    geolocalization_df.write.mode('overwrite').parquet('data/processed/geolocation.parquet')
    order_items_df.write.mode('overwrite').parquet('data/processed/order_items.parquet')
    order_payments_df.write.mode('overwrite').parquet('data/processed/order_payments.parquet')
    order_reviews_df.write.mode('overwrite').parquet('data/processed/order_reviews.parquet')
    orders_df.write.mode('overwrite').parquet('data/processed/orders.parquet')
    products_df.write.mode('overwrite').parquet('data/processed/products.parquet')
    sellers_df.write.mode('overwrite').parquet('data/processed/sellers.parquet')
    product_category_df.write.mode('overwrite').parquet('data/processed/product_category.parquet')
    
def main():
    
    if len(sys.argv) == 11:
    
        # Iniciando sessão no Spark
        spark = SparkSession.builder.appName('Cluster').getOrCreate()

        # Salvando os paths
        customers_path, geolocation_path, order_items_path, order_payments_path,\
        order_reviews_path, orders_path, products_path, sellers_path, \
        products_category_path, ceps_path = sys.argv[1:]
        
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
        product_category = load_product_categories_table(products_category_path, spark)
        order_reviews = load_reviews_table(order_reviews_path, spark)
        print("Tabelas transformadas com sucesso!")
        
        # Cria novas features
        print("Criando novas features...")
        customers, sellers, products, products_category, orders = create_new_features(customers, 
                                                                                      sellers, 
                                                                                      products, 
                                                                                      product_category, 
                                                                                      orders)
        print("Novas features criadas com sucesso!")
        
        # Salvando as tabelas
        print("Salvando as tabelas...")
        save_data(customers, geolocation, order_items, order_payments, order_reviews, orders, products, sellers, product_category)
        print("Tabelas salvas com sucesso!")
    
if __name__ == "__main__":
    main()
    