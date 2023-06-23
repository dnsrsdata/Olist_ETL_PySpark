# Olist ETL

### Sobre o problema

Esse conjunto de dados foi generosamente fornecido pela Olist, a maior loja de
departamentos dos mercados brasileiros. A Olist conecta pequenas empresas de
todo o Brasil a canais sem problemas e com um único contrato. Esses
comerciantes podem vender seus produtos através da Olist Store e enviá-los
diretamente aos clientes usando os parceiros de logística da Olist.

Depois que um cliente compra o produto na Olist Store, um vendedor é notificado
para atender a esse pedido. Depois que o cliente recebe o produto ou a data
estimada de entrega é devida, o cliente recebe uma pesquisa de satisfação por
e-mail, onde pode anotar a experiência de compra e anotar alguns comentários.

Diante disso, é de se esperar que uma grande quantidade de dados seja gerada,
dando a empresa a oportunidade de tomar decisões baseadas em dados.

### Questão de Negócio

A Olist deseja acompanhar as métricas de vendas para alavancar o negócio. Embora
já possua os dados, é necessário trata-los para então usa-los.

### Sobre os dados

Este é um conjunto de dados públicos de comércio eletrônico brasileiro de
pedidos feitos em Loja Olist. O conjunto de dados possui informações de 100 mil
pedidos de 2016 a 2018 feitos em vários mercados no Brasil. Seus recursos
permitem visualizar um pedido de várias dimensões: do status do pedido, preço,
pagamento e desempenho do frete à localização do cliente, atributos do produto
e, finalmente, análises escritas pelos clientes. Também lançamos um conjunto de
dados de geolocalização que relaciona códigos postais brasileiros a coordenadas
lat / lng.

São dados comerciais reais, anonimizados e as referências às empresas e
parceiros no texto da revisão foram substituídas pelos nomes das grandes casas
de Game of Thrones.

Segue um schema com a relação entre os dados:

![sch](images/schema.png)

### Solução

Um script em python foi desenvolvido, onde os dados são tratados, dão origem a
novas features e finalmente salvos.

### Característica dos dados

Todas as colunas, em cada tabela, são do tipo string. Além disso, devido ao uso
da vírugla (",") como separador na tabela de reviews, um erro na leitura foi
detectado, misturando os dados.

### Instruções

Siga os passos abaixo para executar o projeto localmente.

A versão do python utilizada neste projeto foi a 3.10.6, sugiro que use a mesma
versão para evitar problemas de compatibilidade.

1. Clone o repositório

```sh
    git clone https://github.com/dnsrsdata/Olist_ETL_PySpark
```

2. Instale as dependências

```sh
    pip install -r requirements.txt
```

3. Execute o seguinte comando no terminal para preparar os dados externos

```sh
    python3 src/External_Data_Transform.py "data/external/postcode_ranges.xlsx - Sheet1.csv"
```

4. Execute o seguinte comando para executar o script de ETL

```sh
    python3 src/data_transform.py "data/raw/olist_customers_dataset.csv" "data/raw/olist_geolocation_dataset.csv" "data/raw/olist_order_items_dataset.csv" "data/raw/olist_order_payments_dataset.csv" "data/raw/olist_order_reviews_dataset.csv" "data/raw/olist_orders_dataset.csv" "data/raw/olist_products_dataset.csv" "data/raw/olist_sellers_dataset.csv" "data/raw/product_category_name_translation.csv" "data/external/cep_prefixes_processed.parquet"
```

## Descrição dos aquivos

    - data
    | - external
    | |- cep_prefixes_processed.parquet  # dados referente ao cep e a cidade relacionada a ele
    | |- postcode_ranges.xlsx - Sheet1.csv  # dados brutos do cep e cidades relacionada a ele
    |- processed
    | |- customers.parquet  # dados referente aos clientes
    | |- geolocation.parquet  # dados referentes a geolocalização
    | |- order_items.parquet  # dados referentes aos itens dos pedidos
    | |- order_payments.parquet  # dados refereentes aos pagamentos dos pedidos
    | |- order_reviews.parquet  # dados refereentes as reviews dos pedidos
    | |- orders.parquet  # dados referentes aos pedidos
    | |- product_category.parquet  # dados referentes as categorias dos produtos
    | |- products.parquet  # dados referentes aos produtos
    | |- sellers.parquet  # dados refereentes aos vendedores 
    |- raw
    | |- olist_customers_dataset.csv  # dados brutos dos clientes
    | |- olist_geolocation_dataset.csv  # dados brutos da geolocalização
    | |- olist_order_items_dataset.csv  # dados brutos dos itens dos pedidos
    | |- olist_order_payments_dataset.csv  # dados brutos dos pagamentos dos pedidos
    | |- olist_order_reviews_dataset.csv  # dados brutos das reviews dos pedidos
    | |- olist_orders_dataset.csv  # dados brutos dos pedidos
    | |- olist_products_dataset.csv  # dados brutos dos produtos
    | |- olist_sellers_dataset.csv  # dados brutos dos vendedores
    | |- product_category_name_translation.csv  # dados brutos das categorias dos produtos
    |
    - images
    |- schema.png  # imagem com o schema dos dados
    |
    - notebooks
    |- ETL.ipynb  # notebook contendo todo o processo de teste e experimentação aplicada nos dados
    |- external_data_clean.ipynb  # notebook contendo toda experimentação testada para tratar os dados externos
    |
    - src
    |- data_transform.py  # script contendo todo o processo necessário para tratar os dados 
    |- External_Data_Transform.ipynb  # script contendo todo o processo necessário para tratar os dados externos
    |
    - similarity_results
    |- similarity_df.csv  # data containing the similarity of each id to other id
    |
    - README.md  # arquivo contendo as informações do projeto
    - requirements.txt # arquivo contendo os pacotes necessários para executar o projeto

## Results

Os resultados podem ser checados seguindo o seguinte path:

```sh
    data/processed/
```
