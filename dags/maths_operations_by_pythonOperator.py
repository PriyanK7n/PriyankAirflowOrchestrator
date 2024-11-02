"""
Defining a DAG for mathematical calculation:
TASK1:  start_task
TASK2:  add_five_task
TASK3:  multiply_two_task
TASK4:  subtract_three_task
"""


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Note: Airflow 2.0+ automatically injects the context dictionary as a **kwargs parameter in Python functions. So we don’t need provide_context=True to access XComs or task instance metadata within the function.
# Defining functions for each task outside the DAG due to PythonOperator usage
# Each function will be referred by a task ID
# In ariflow the xcom design ensures each task’s XCom values are isolated (don't need to specify when pushing) unless we explicitly pull from another task’s ID.

def start_number(**context):
    context["ti"].xcom_push(key = "current_value", value = 10)
    print("starting number is 10")

def add_five(**context):
    current_value = context["ti"].xcom_pull(key = "current_value", task_ids = "start_task") # task id refers to the task in this case the previous task's xcom value which was pushed
    new_value = current_value + 5
    context["ti"].xcom_push(key = "current_value", value = new_value)# xcom push automatically associates push value with the current task’s ID.
    print(f"Adding by 5; {current_value} + 5 = {new_value}")

def multiply_by_two(**context):
    current_value = context["ti"].xcom_pull(key = "current_value", task_ids = "add_five_task") # In xcom_pull requires explicitly pulling from another task’s ID.  In this case the previous task's xcom value which was pushed
    new_value = current_value * 2
    context["ti"].xcom_push(key = "current_value", value = new_value)
    print(f"Multiplying by 2; {current_value} * 2 = {new_value}")

def subtract_by_three(**context):
    current_value = context["ti"].xcom_pull(key = "current_value", task_ids = "multiply_two_task")
    new_value = current_value - 3 
    context["ti"].xcom_push(key = "current_value", value = new_value)
    print(f"Subtract by 3; {current_value} - 3 = {new_value}")

def square_number(**context):
    current_value = context["ti"].xcom_pull(key = "current_value", task_ids = "subtract_three_task")
    new_value = current_value ** 2
    context["ti"].xcom_push(key = "current_value", value = new_value)
    print(f"Squaring Number; {current_value} ** 2 = {new_value}")
    

# Define a DAG
with DAG(
    dag_id = "math_sequence_ds_basic",
    start_date = datetime(2023, 1, 1),
    schedule_interval = "@once",
    catchup = False
) as dag:

    # Defining all the tasks using PythonOperator
    start_task = PythonOperator(
            task_id = "start_task", # attaches task_id to the task
            python_callable = start_number, # task function's name
            # provide_context = True # provides access to xcom but By default in Airflow 2.0 amd above automatically context (such as XCom and other metadata) is passed
        )

    add_five_task = PythonOperator (
            task_id = "add_five_task", # attaches task_id to the task
            python_callable = add_five, # task function's name
            # provide_context = True # provdes access to xcom
        )

    multiply_two_task = PythonOperator(
            task_id = "multiply_two_task", # attaches task_id to the task
            python_callable = multiply_by_two, # task function's name
            # provide_context = True # provdes access to xcom
        )

    subtract_three_task = PythonOperator(
            task_id = "subtract_three_task", # attaches task_id to the task
            python_callable = subtract_by_three, # task function's name
            # provide_context = True # provides access to xcom
        )

    square_number_task = PythonOperator(
            task_id = "square_number_task", # attaches task_id to the task
            python_callable = square_number, # task function's name            
            # provide_context = True # provdes access to xcom
        )

    ## Dependencies: To set the order of execution of tasks
    start_task >> add_five_task >> multiply_two_task >> subtract_three_task >> square_number_task






    

