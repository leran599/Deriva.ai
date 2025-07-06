TASK 1:
run & build:
    docker compose up --build
trigger the workflow:
    docker exec task1-airflow-1 airflow dags trigger weather_data_pipeline

TASK 2:
run & build:
    docker compose up --build

test: 
    curl --location 'http://127.0.0.1:5001/sum_of_squares' \
    --header 'Content-Type: application/json' \
    --data '{
        "length":100
    }'
