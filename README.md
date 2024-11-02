## Airflow Data Pipeline Projects

This repository contains **two mini-projects** and one **big project** demonstrating different approaches and use cases of **Apache Airflow** for building data pipelines. All projects are managed with **Astronomer**, providing a scalable and consistent environment to manage Airflow deployments.

## Project Overview
1. Math Operations Using PythonOperator
2. Math Operations Using TaskFlow API
3. ETL Pipeline with NASA's APOD API

**Note**: Each project is organized in the dags/ directory, highlighting different aspects of Airflow for task orchestration to full ETL pipeline building, and emphasizes best practices in pipeline automation, error handling, and scalability.


## Project 1: Math Operations Using PythonOperator

This mini-project demonstrates mathematical operations using the PythonOperator, with each task defined outside the DAG block and managed explicitly using XCom for data flow.



This mini-project demonstrates mathematical operations using the **PythonOperator**, showcasing how to define tasks and manage data flow between them **explicitly**. **Each function is defined outside the DAG**, and the **XComs are manually handled to pass values between tasks**.

DAG File: dags/maths_operations_by_pythonOperator.py

Key Concepts:
* Manual XCom handling for data sharing between tasks
* Modular task definitions with PythonOperator

Airflow UI Graph:

![PythonOperator Usage - Math Operations DAG](image.png)

## Project 2: Math Operations Using TaskFlow API

This mini-project leverages **Airflow’s TaskFlow API**, offering a more streamlined approach to task definition. Using Python decorators, tasks are defined directly within the DAG, and data flow between tasks is automatically managed.

DAG File: dags/math_operations_by_TaskFlow.py
Key Concepts:
* TaskFlow API with @task decorators
* Automatic XCom handling and intuitive data passing

Airflow UI Graph:

![TaskFlow API - Math Operations DAG](image-1.png)





