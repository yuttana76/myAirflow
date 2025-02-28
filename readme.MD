# Monitor Apache Airflow with StatsD | Prometheus | Grafan | part1
https://www.youtube.com/watch?v=xyeR_uFhnD4

Monitor your dag with slack alerts | Apache Airflow tutorials for beginners
https://www.youtube.com/watch?v=2Cbz9Z06KJo

Apache-Airflow
https://github.com/maxcotec/Apache-Airflow/blob/main/Monitoring_Dag/configs/airflow.cfg

### Metrics Configuration
airflow-dashboards
https://github.com/databand-ai/airflow-dashboards

https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html

### Scheduling & Triggers
https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html

### Crontab
https://crontab.guru/#59_*_*_*_*

### A Python statsd client
https://github.com/jsocol/pystatsd


# Virtual Environment in Python
https://www.freecodecamp.org/news/how-to-setup-virtual-environments-in-python/

>pip install virtualenv
>python -m venv venv
>source venv/bin/activate
>pip list
>pip freeze > requirements.txt
>pip install -r requirements.txt
>deactivate


# Comman to run 
Change permission

>chmod o+wX logs
>chmod g+wX log

>chmod o+wX data

>docker compose up airflow-init
>docker compose up airflow-scheduler -d
>docker compose up airflow-webserver -d
>docker compose up statsd-exporter
>docker compose up prometheus
>docker compose up grafana

>docker compose down

# Remove images
docker rmi $(docker images -f dangling=true -q)




## airflow web server
http://localhost:8081

## statsd-exporter
http://localhost:9102/

## prometheus
http://localhost:9092

## grafana
http://localhost:3000/login

Config data source prometheus
http://localhost:9092

Add new dashboard

VARIABLE
AIRFLOW_VAR_FC_API_PASSPOWRD	
AIRFLOW_VAR_FC_API_URL
AIRFLOW_VAR_FC_API_USER

# Postgres

CREATE USER etl_user WITH PASSWORD 'password';
GRANT CONNECT ON DATABASE 'lbdu_db' TO 'etl_user'
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO 'etl_user'