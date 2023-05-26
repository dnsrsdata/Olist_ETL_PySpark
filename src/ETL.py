import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lower, to_timestamp, month, hour, datediff, dayofweek
from pyspark.sql.types import StringType
from pyspark.ml.feature import Tokenizer


def load_data(customer_path, geolocalization_path, order_items_path,
              order_payment_path, order_review_path, orders_path,
              products_path, sellers_path, product_category_path,
              header=True):
    """
    Carrega os dados dos paths especificados e retorna no formato spark dataframe
    """
    # Para realizar a leitra dos dados, precisaremos iniciar o spark
    spark = SparkSession.builder.appName('SpSession').getOrCreate()

    # Lendos os dados csv
    customers = spark.read.csv("../data/raw/olist_customers_dataset.csv", header=True)
    geolocalization = spark.read.csv("../data/raw/olist_geolocation_dataset.csv", header=True)
    order_items = spark.read.csv("../data/raw/olist_order_items_dataset.csv", header=True)
    order_payments = spark.read.csv("../data/raw/olist_order_payments_dataset.csv", header=True)
    order_reviews = spark.read.csv("../data/raw/olist_order_reviews_dataset.csv", header=True)
    orders = spark.read.csv("../data/raw/olist_orders_dataset.csv", header=True)
    products = spark.read.csv("../data/raw/olist_products_dataset.csv", header=True)
    sellers = spark.read.csv("../data/raw/olist_sellers_dataset.csv", header=True)
    product_category = spark.read.csv("../data/raw/product_category_name_translation.csv", header=True)
    return customers, geolocalization, order_items, order_payments, order_reviews, orders, products, sellers, \
        product_category


def join_and_transform_data(customers, geolocalization, order_items, order_payments, order_reviews, orders, products,
                            sellers,
                            product_category):
    """
    Realiza o join dos dados e os transforma e retorna no formato spark dataframe
    """
    # Antes de realizar as operações, é necessário iniciar o spark
    spark = SparkSession.builder.appName('SpSession').getOrCreate()

    # Para realizar o join e tranformações usando SQL, é necessário criar uma view temporária
    customers.createOrReplaceTempView("customers")
    geolocalization.createOrReplaceTempView('geolocalization')
    order_items.createOrReplaceTempView('order_items')
    order_payments.createOrReplaceTempView('order_payments')
    order_reviews.createOrReplaceTempView('order_reviews')
    orders.createOrReplaceTempView('orders')
    products.createOrReplaceTempView('products')
    sellers.createOrReplaceTempView('sellers')
    product_category.createOrReplaceTempView('product_category')

    # Unindo as tabelas
    orders_full = spark.sql("WITH geo_sellers AS  \
                            ( \
                            SELECT s.seller_zip_code_prefix, \
                            s.seller_city, \
                            s.seller_id,\
                            s.seller_state, \
                            g.geolocation_lat, \
                            g.geolocation_lng \
                            FROM sellers s  \
                            FULL OUTER JOIN geolocalization g  \
                            ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix \
                            ),  \
                            item_info AS \
                            ( \
                            SELECT oi.order_id, \
                            oi.order_item_id, \
                            oi.product_id, \
                            oi.seller_id, \
                            oi.shipping_limit_date, \
                            oi.price, \
                            oi.freight_value, \
                            p.product_category_name, \
                            p.product_name_lenght, \
                            p.product_description_lenght,  \
                            p.product_photos_qty, \
                            p.product_weight_g,  \
                            p.product_length_cm, \
                            p.product_height_cm, \
                            p.product_width_cm  \
                            FROM order_items oi  \
                            LEFT JOIN products p  \
                            ON oi.product_id = p.product_id \
                            )  \
                            SELECT o.order_id, \
                            o.customer_id, \
                            ii.product_id,  \
                            o.order_status, \
                            CAST(o.order_estimated_delivery_date AS date), \
                            CAST(o.order_purchase_timestamp AS timestamp), \
                            CAST(o.order_approved_at AS timestamp),  \
                            CAST(o.order_delivered_carrier_date AS timestamp), \
                            CAST(o.order_delivered_customer_date AS timestamp),  \
                            CAST(or.review_score AS int), \
                            LOWER(or.review_comment_title) AS review_comment_title, \
                            LOWER(or.review_comment_message) AS review_comment_message, \
                            CAST(or.review_creation_date AS date), \
                            CAST(or.review_answer_timestamp AS timestamp),  \
                            CAST(op.payment_sequential AS int), \
                            op.payment_type, \
                            CAST(op.payment_installments AS int),  \
                            CAST(op.payment_value AS float), \
                            c.customer_city, \
                            LOWER(c.customer_state) AS customer_state,  \
                            gs.seller_city, \
                            LOWER(gs.seller_state) AS seller_state, \
                            CAST(ii.shipping_limit_date AS timestamp), \
                            CAST(ii.price AS float), \
                            CAST(ii.freight_value AS float), \
                            ii.product_category_name, \
                            CAST(ii.product_name_lenght AS int), \
                            CAST(ii.product_description_lenght AS int),  \
                            CAST(ii.product_photos_qty AS int), \
                            CAST(ii.product_weight_g AS int),  \
                            CAST(ii.product_length_cm AS int), \
                            CAST(ii.product_height_cm AS int), \
                            CAST(ii.product_width_cm AS int),  \
                            CASE  \
                                WHEN c.customer_state IN ('ac', 'am', 'ro', 'rr', 'pa', 'ap', 'to') THEN 'norte'  \
                                WHEN c.customer_state IN ('ma', 'pi', 'ce', 'rn', 'pb', 'pe', 'se', 'al', 'ba') THEN 'nordeste'  \
                                WHEN c.customer_state IN ('mt', 'ms', 'go', 'df') THEN 'centro-oeste'  \
                                WHEN c.customer_state IN ('sp', 'rj', 'es', 'mg') THEN 'sudeste'  \
                                ELSE 'sul'  \
                            END AS customer_region, \
                            CASE  \
                                WHEN gs.seller_state IN ('ac', 'am', 'ro', 'rr', 'pa', 'ap', 'to') THEN 'norte'  \
                                WHEN gs.seller_state IN ('ma', 'pi', 'ce', 'rn', 'pb', 'pe', 'se', 'al', 'ba') THEN 'nordeste'  \
                                WHEN gs.seller_state IN ('mt', 'ms', 'go', 'df') THEN 'centro-oeste'  \
                                WHEN gs.seller_state IN ('sp', 'rj', 'es', 'mg') THEN 'sudeste'  \
                                ELSE 'sul'  \
                            END AS seller_region, \
                            CASE  \
                                WHEN ii.product_category_name IN ('fashion_roupa_masculina', 'fashion_roupa_feminina', 'fashion_roupa_infanto_juvenil', 'fashion_underwear_e_moda_praia', 'fashion_bolsas_e_acessorios', 'fashion_calcados', 'fashion_esporte') THEN 'moda e acessorios'  \
                                WHEN ii.product_category_name IN ('moveis_decoracao', 'moveis_colchao_e_estofado', 'moveis_cozinha_area_de_servico_jantar_e_jardim', 'moveis_quarto', 'moveis_sala', 'utilidades_domesticas', 'cama_mesa_banho', 'casa_construcao', 'casa_conforto', 'casa_conforto_2') THEN 'casa e decoracao'  \
                                WHEN ii.product_category_name IN ('livros_tecnicos', 'livros_importados','livros_interesse_geral') THEN 'livros e educacao'  \
                                WHEN ii.product_category_name IN ('beleza_saude', 'fraldas_higiene', 'perfumaria') THEN 'beleza e saude'  \
                                WHEN ii.product_category_name IN ('brinquedos', 'jogos', 'instrumentos_musicais') THEN 'brinquedos e jogos'  \
                                WHEN ii.product_category_name IN ('alimentos_bebidas', 'bebidas', 'alimentos') THEN 'alimentos e bebidas'  \
                                WHEN ii.product_category_name IN ('artigos_de_festas', 'artigos_de_natal') THEN 'artigos para festas'  \
                                WHEN ii.product_category_name IN ('artes', 'cine_foto', 'artes_e_artesanato') THEN 'artes e artesanato'  \
                                WHEN ii.product_category_name IN ('construcao_ferramentas_construcao', 'construcao_ferramentas_seguranca', 'construcao_ferramentas_jardim', 'construcao_ferramentas_iluminacao', 'construcao_ferramentas_ferramentas') THEN 'construcao e ferramentas'  \
                                WHEN ii.product_category_name IN ('esporte_lazer') THEN 'esporte e lazer'  \
                                ELSE 'outros'  \
                            END AS product_sub_category, \
                            ii.product_weight_g / 1000 AS product_weight_kg, \
                            ii.product_length_cm / 100 AS product_length_m, \
                            ii.product_height_cm / 100 AS product_height_m, \
                            ii.product_width_cm / 100 AS product_width_m, \
                            DAYOFWEEK(o.order_purchase_timestamp) AS order_weekday, \
                            MONTH(o.order_purchase_timestamp) AS order_month, \
                            HOUR(o.order_purchase_timestamp) AS order_hour, \
                            DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS order_delivery_time, \
                            DATEDIFF(o.order_approved_at, o.order_purchase_timestamp) AS order_approval_time, \
                            CASE \
                                WHEN order_weekday = 1 THEN 'sunday'  \
                                WHEN order_weekday = 2 THEN 'monday'  \
                                WHEN order_weekday = 3 THEN 'tuesday'  \
                                WHEN order_weekday = 4 THEN 'wednesday'  \
                                WHEN order_weekday = 5 THEN 'thursday'  \
                                WHEN order_weekday = 6 THEN 'friday'  \
                                ELSE 'saturday'  \
                            END AS order_weekday_name, \
                            CASE \
                                WHEN order_month = 1 THEN 'january'  \
                                WHEN order_month = 2 THEN 'february'  \
                                WHEN order_month = 3 THEN 'march'  \
                                WHEN order_month = 4 THEN 'april'  \
                                WHEN order_month = 5 THEN 'may'  \
                                WHEN order_month = 6 THEN 'june'  \
                                WHEN order_month = 7 THEN 'july'  \
                                WHEN order_month = 8 THEN 'august'  \
                                WHEN order_month = 9 THEN 'september'  \
                                WHEN order_month = 10 THEN 'october'  \
                                WHEN order_month = 11 THEN 'november'  \
                                ELSE 'december'  \
                            END AS order_month_name, \
                            CASE  \
                                WHEN or.review_score <= 3 THEN 'bad'  \
                                ELSE 'good'  \
                            END AS review_score_class \
                            FROM orders o \
                            LEFT JOIN order_reviews or  \
                            ON o.order_id = or.order_id  \
                            LEFT JOIN order_payments op  \
                            ON o.order_id = op.order_id  \
                            LEFT JOIN customers c  \
                            ON o.customer_id = c.customer_id  \
                            LEFT JOIN item_info ii   \
                            ON o.order_id = ii.order_id  \
                            LEFT JOIN geo_sellers gs  \
                            ON ii.seller_id = gs.seller_id")
    return orders_full


    