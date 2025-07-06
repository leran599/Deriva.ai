run & build:
    docker compose up --build
trigger the workflow:
    docker exec task1-airflow-1 airflow dags trigger weather_data_pipeline