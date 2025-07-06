TASK 1: <br>
  run & build <br>
    docker compose up --build <br>
trigger the workflow: <br>
    docker exec task1-airflow-1 airflow dags trigger weather_data_pipeline
    <br><br>
TASK 2: <br>
  run & build: <br>
    docker compose up --build <br>

test: <br>
    curl --location 'http://127.0.0.1:5001/sum_of_squares' \
    --header 'Content-Type: application/json' \
    --data '{
        "length":100
    }'
