# GoogleCloudETL ![Google Cloud](https://img.shields.io/badge/GoogleCloud-%234285F4.svg?style=for-the-badge&logo=google-cloud&logoColor=white)
Repositório para criação de um Pipeline na GoogleCloud


<h2 id="objetivo">🚀 OBJETIVO </h2>

Fazer a Ingestão de 6 FlatFiles (.CSV) para criação de um Data Visualization



<h2 id="technologies">💻 TÉCNOLOGIAS </h2>

[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)

Google Cloud Plataform

  °Big Query ---------------------> Data Warehouse
  °Compute Engine ----------------> Hospedagem do Airflow
  °Airflow -----------------------> Orquestrador dos Jobs
  °Cloud Functions ---------------> Extração da Aplicação para o Bucket
  °DataFlow ----------------------> Jobs do Buket até O BigQuery
  °MySQL -------------------------> tabela de controle
  °Secret Manager ----------------> gerenciar os segredos
  °PowerBI -----------------------> visualização de painél de indicadores do Datamart
