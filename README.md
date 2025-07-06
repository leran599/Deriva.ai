TASK 1: <br>
 1.  "cd task1" <br>
 2. run & build: <br>
    "docker compose up --build" <br>
 3. open new terminal window & trigger the workflow: <br>
    "docker exec task1-airflow-1 airflow dags trigger weather_data_pipeline"
    <br><br>
TASK 2: <br>
 1.  "cd task2" <br>
 2. run & build: <br>
    "docker compose up --build" <br>

 3. test: <br>
    "curl --location 'http://127.0.0.1:5001/sum_of_squares' \
    --header 'Content-Type: application/json' \
    --data '{
        "length":100
    }'"
