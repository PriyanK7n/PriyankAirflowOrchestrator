from airflow import DAG
from airflow.decorators import task
from datetime import datetime

"""Apache Airflow introduced the TaskFlow API which allows you to create tasks
using Python decorators like @task. This is a cleaner and more intuitive way
of writing tasks without needing to manually use operators like PythonOperator.
"""
## Defining the DAG 

with DAG (
    dag_id = "math_sequence_ds_with_taskflow_api",
    start_date = datetime(2023, 1, 1),
    schedule_interval = "@once",
    catchup = False 
) as dag :
     # Defining functions for each task inside DAG Block when using Task FLow API and every function should return something
     # every function's return value showsup in xcom in airflow UI
    # Task 1: Start with initial number
    @task
    def start_number():
        initial_value = 10
        print(f"Starting number is {initial_value}")
        return initial_value # number param

    @task # task 2
    def add_five(number):
        new_value = number + 5
        print(f"Adding 5; {number} + 5 = {new_value}")
        return new_value

    @task # task 3
    def multiply_by_two(number):
        new_value = number * 2
        print(f"Multiplying by 2; {number} * 2 = {new_value}")
        return new_value   

    @task # task 4
    def subtract_by_three(number):
        new_value = number - 3
        print(f"Adding 5; {number} - 3 = {new_value}")
        return new_value
    
    @task # task 5
    def square_number(number):
        new_value = number ** 2
        print(f"Squaring Number; {number} ** 2 = {new_value}")
        return new_value

    ## Dependencies: To set the order of execution of tasks
    initial_value = start_number()
    added_value = add_five(initial_value)
    multiply_value = multiply_by_two(added_value)
    subtract_value = subtract_by_three(multiply_value)
    square_value = square_number(subtract_value)




        


    



