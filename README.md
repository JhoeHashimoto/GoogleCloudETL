# GoogleCloudETL
Repositório para criação de um Pipeline na GoogleCloud

[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)

##Objetivo

Fazer a Ingestão de 6 FlatFiles (.CSV) para criação de um Data Visualization



##Tecnologias

Google Cloud Plataform

  °Big Query --------------------------------------------> Data Warehouse
  °Compute Engine ---------------------------------------> Hospedagem do Airflow
  °Airflow ----------------------------------------------> Orquestrador dos Jobs
  °Cloud Functions --------------------------------------> Extração da Aplicação para o Bucket
  °DataFlow ---------------------------------------------> Jobs do Buket até O BigQuery
  °MySQL ------------------------------------------------> tabela de controle
  °Secret Manager ---------------------------------------> gerenciar os segredos
  °PowerBI ----------------------------------------------> visualização de painél de indicadores do Datamart
