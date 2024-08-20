<h1>GoogleCloudETL</h1> 

Repositório para criação de um Pipeline na ![Google Cloud](https://img.shields.io/badge/GoogleCloud-%234285F4.svg?style=for-the-badge&logo=google-cloud&logoColor=white)


<h2 id="objetivo"> OBJETIVO </h2>

Fazer a Ingestão de 6 FlatFiles (.CSV) para criação de um Data Visualization

<h2 id="arquitetura"> ARQUITETURA </h2>

![image](https://github.com/user-attachments/assets/c8fbf4d3-413b-4e82-b09c-aeeea1a638c5)


<h2 id="technologies"> TÉCNOLOGIAS </h2>   

 [![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)

| Recurso               | Finalidade                                          
|----------------------|-----------------------------------------------------
| <kbd>Big Query</kbd>     | Data Warehouse 
| <kbd>Compute Engine</kbd>     | Hospedagem do Airflow
| <kbd>Airflow</kbd>     | Orquestrador dos Jobs
| <kbd>Cloud Functions</kbd>     | Extração da Aplicação para o Bucket
| <kbd>DataFlow</kbd>     | Jobs do Buket até O BigQuery
| <kbd>MySQL</kbd>     | Tabela de controle
| <kbd>Secret Manager</kbd>     | Gerenciar os segredos
| <kbd>Cloud Functions</kbd>     | Ingestão de Produção (Bucket 2 Bucket)
| <kbd>PowerBI</kbd>     | Visualização de painél de indicadores do Datamart





