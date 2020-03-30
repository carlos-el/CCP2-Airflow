# CCP2-Airflow
Environment for setting up and Airflow workflow that deploys a temperature and humidity forecasting microservice using docker.

This repository includes the barebones for deploying an Airflow workflow that creates an deploys a temperature and humidity forecasting microservice using docker.

The microservice used for this purpose is hosted in this [repository](https://github.com/carlos-el/CCP2-Prediction_Microservice).

The current repo includes the following content:
- The Airflow 'dag' that defines the workflow.
- The directory structure to allow the execution of the workflow.
- The docker-compose file for deploying the docker container used.
- A requirements file with the dependencies needed to make the workflow work.
- The information required to create the database container using mongodb.
