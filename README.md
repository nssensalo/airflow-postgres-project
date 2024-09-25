![gradiant line](images/bluegradient.png)
Purpose
=======
To orchestrate a DAG on Airflow as well as dockerizing airflow, postgres, and pgadmin. The data is scraped from AWS and is simply a table of current Data Engineering books. The DAG is scheduled to run once a day. This project will be expanded upon with more complexity in DAG tasks, including modeling with dbt, creation of aggregated tables and marts, and SCD 2 type upserting. 

Run
===
Install [Docker](https://docs.docker.com/engine/install/)<br>

run: 
`docker compose up` <br> 

view airflow: `localhost:8080` <br>

view pgadmin:`localhost:5050`<br>


username:`admin` <br>

password:`admn`<br>

## Checkout DAG diagram:

![gradiant line](images/air_dagflow.png)

## Query table in pgAdmin:

![gradiant line](images/air_pgadmin2.png)

## Watch for run status:

![gradiant line](images/air_runs.png)

![gradiant line](images/bluegradient.png)
