# Olist data organization EM DESENVOLVIMENTO

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
A Olist deseja acompanhar as métricas de vendas e embora já possua os dados, é 
necessário trata-los e então realizar a ingestão em um banco SQL, para então 
conectar a uma ferramenta de BI para a disponibilização dos relatórios.

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

```sh
    python3 src/External_Data_Treatment.py "data/external/postcode_ranges.xlsx - Sheet1.csv"
```
