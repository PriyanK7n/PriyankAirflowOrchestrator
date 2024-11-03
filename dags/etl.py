from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator # SimpleHttpOperator helps to hit an api or call an api using airflow
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook # Postgres Hook is taken from astronomer's hooks registry (https://registry.astronomer.io/providers/apache-airflow-providers-postgres/versions/5.11.0/modules/PostgresHook) to insert data inside Postgres sql database
from airflow.utils.dates import days_ago
import json


## Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=days_ago(1), 
    schedule_interval='@daily',  # every day this dag is run
    catchup=False
) as dag:
    # Task Flow API Usage
    ## step 1: Create the table if it doesnt exists by getting structure from API data that is being returned
    
    @task(task_id="create_table")
    def create_table():
        ## initialize the Postgreshook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection") # connection for postgres 
        ## SQL query to create the table 
        create_table_query="""
            CREATE TABLE IF NOT EXISTS apod_data (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                explanation TEXT,
                url TEXT,
                date DATE,
                media_type VARCHAR(50)
            );
        """
        ## Execute the table creation query
        postgres_hook.run(create_table_query)
    


    ## Step 2: Creating Extract pipeline: Extract the NASA API Data(APOD) - Astronomy Picture of the Day 
    ## https://api.nasa.gov/planetary/apod?api_key=7BbRvxo8uuzas9U3ho1RwHQQCkZIZtJojRIr293q
    ## url = https://api.nasa.gov/{endpoint}?api_key=7BbRvxo8uuzas9U3ho1RwHQQCkZIZtJojRIr293q
    extract_apod = SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id = 'nasa_api',  ## Connection ID Defined In Airflow For NASA API
        endpoint = 'planetary/apod', ## NASA API enpoint for APOD
        method = 'GET',
        data = {"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"}, ## We get the api key from the Airflow's connection
        response_filter = lambda response: response.json() ## Convert response to json
        # log_response=True,
        # do_xcom_push=True  # Enable XCom push to pass data between tasks
    )


    ## Step 3: Creating Transformation step: Transform the data by Picking only the required information that we want to save
    @task(task_id="transform_apod_data")
    def transform_apod_data(response):
        apod_data={
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data


    ## step 4:  Load the transformed data (apod_data) into Postgres SQL
    @task(task_id="load_data_to_postgres")
    def load_data_to_postgres(apod_data):
        ## Initialize the PostgresHook
        postgres_hook=PostgresHook(postgres_conn_id='my_postgres_connection')
        ## Define the SQL Insert Query
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        ## Execute the SQL Query
        postgres_hook.run(insert_query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))


    ## step 5: Verify the data in DBViewer (a tool that helps to connect data to any db postgres/Mysql database)


    ## step 6: Defining the task dependencies
    table_creation = create_table()
    api_response = extract_apod 
    transformed_data = transform_apod_data(api_response.output) # save the output as api response and transf the data
    load_data = load_data_to_postgres(transformed_data)

    table_creation >> api_response >> transformed_data >> load_data
