<h1>GoogleCloudETL</h1> 

Repositório para criação de um Pipeline na ![Google Cloud](https://img.shields.io/badge/GoogleCloud-%234285F4.svg?style=for-the-badge&logo=google-cloud&logoColor=white)


<h2 id="objetivo"> OBJETIVO </h2>

Fazer a Ingestão de 6 FlatFiles (.CSV) para criação de um Data Mart de VENDAS

<h2 id="arquitetura"> ARQUITETURA </h2>

![image](https://github.com/user-attachments/assets/c8fbf4d3-413b-4e82-b09c-aeeea1a638c5)

[Link para o Miro Board](https://miro.com/app/board/uXjVKoI0Df8=/?diagramming=)

<h2 id="technologies"> TÉCNOLOGIAS </h2>   

 [![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)

| Recurso               | Finalidade                                          
|----------------------|-----------------------------------------------------
| <kbd>Big Query</kbd>     | Data Lakehouse 
| <kbd>Compute Engine</kbd>     | Hospedagem do Airflow
| <kbd>Airflow</kbd>     | Orquestrador dos Jobs
| <kbd>Cloud Functions</kbd>     | Extração da Aplicação para o Bucket
| <kbd>DataFlow</kbd>     | Jobs do Bucket até O BigQuery
| <kbd>MySQL</kbd>     | Banco usado para a tabela de controle
| <kbd>Secret Manager</kbd>     | Gerenciar os segredos
| <kbd>Cloud Functions</kbd>     | Ingestão de Produção (Bucket 2 Bucket)
| <kbd>PowerBI</kbd>     | Visualização de painél de indicadores do Datamart





<h2 id="ingestão"> INGESTÃO </h2>   

<ul>
 <li><h3>CARGA FULL</h3></li>

- [x] TRUNCATE TRUSTED
- [x] INSERT

 <li><h3>CARGA INCREMENTAL</h3></li>
</ul> 

- [x] Utilização do campo _ModifiedDate_
- [x] Delete Físico

   <h4>°PRIMARY KEY</h4>

| Flat File               | PK                                          
|----------------------|-----------------------------------------------------
| <kbd>Sales.SpecialOfferProduct.csv</kbd>     | SpecialOfferID e ProductID 
| <kbd>Sales.SalesOrderHeader.csv</kbd>     | SalesOrderID
| <kbd>Sales.Sales.OrderDetail.csv</kbd>     | SalesOrderDetailID 
| <kbd>Sales.Customer.csv</kbd>     | CustomerID
| <kbd>Production.Product.csv</kbd>     | ProductNumber
| <kbd>Person.Person.csv</kbd>     | BusinessEntityID


[Link para a Modelagem](https://miro.com/app/board/uXjVKnpgvfI=/)
   <h4>MERGE</h4>
 
     | CASO                         | OPERACAO                                          
     |------------------------------|-----------------------------------------------------
     | INSERT                       | When no matched by _target_ - **INSERT** 
     | UPDATE                       | When matched source = target - **UPDATE**
     | DELETE                       | When not matched by _source_ - **DELETE**
        

<h2 id="return"> INDICADORES DO DATAMART VENDAS </h2>

- [ ] Contagem de Ordens de Pedido que possuem mais de 3 produtos
- [ ] 3 Produtos mais vendidos de cada item a partir da listagem de lead time de produção (dia)
- [ ] Quantidade de Pedidos por Cliente
- [ ] Quantidade de Produtos Vendidos na data de colocação
- [ ] Pedidos que estão a vencer no mês 09/11 com valor maior que $1.000






