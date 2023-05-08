import boto3
import streamlit as st
import pandas as pd
import json

# Initialize Boto3 client for AWS Glue and Step Functions
glue_client = boto3.client('glue')
step_functions_client = boto3.client('stepfunctions')

# Get all step functions starting with "bhanu_chennam"
def get_step_functions():
    step_functions_list = []
    paginator = step_functions_client.get_paginator('list_state_machines')
    for response in paginator.paginate():
        for state_machine in response['stateMachines']:
            if state_machine['name'].startswith('bhanu_chennam'):
                step_functions_list.append(state_machine)
    return step_functions_list

# Get all Glue jobs and their settings
def get_glue_jobs():
    glue_jobs_list = []
    response = glue_client.get_jobs()
    for job in response['Jobs']:
        glue_jobs_list.append({
            'name': job['Name'],
            'created': job['CreatedOn'],
            'glue_version': job['GlueVersion'],
            'default_worker_type': job['DefaultArguments']['--job-bookmark-option']['Default'],
            'default_worker_nodes': job['DefaultArguments']['--default-worker-cores']['Default']
        })
    return glue_jobs_list

# Get the last 100 executions of a Glue job
def get_glue_job_executions(glue_job_name):
    glue_executions_list = []
    response = glue_client.get_job_runs(JobName=glue_job_name, MaxResults=100)
    for execution in response['JobRuns']:
        glue_executions_list.append({
            'id': execution['Id'],
            'status': execution['JobRunState'],
            'start_time': execution['StartedOn'],
            'end_time': execution['CompletedOn'],
            'time_taken': execution['ExecutionTime'],
            'params': json.loads(execution['Arguments'])
        })
    return glue_executions_list

# Get all step functions that use a Glue job
def get_step_functions_using_glue_job(glue_job_name, step_functions_list):
    step_functions_using_glue_job = []
    for step_function in step_functions_list:
        definition = json.loads(step_functions_client.describe_state_machine(stateMachineArn=step_function['stateMachineArn'])['definition'])
        for state in definition['States'].values():
            if state['Type'] == 'Task' and state['Resource'].startswith('arn:aws:states:::glue:startJobRun'):
                task_glue_job_name = state['Parameters']['JobName']
                if task_glue_job_name == glue_job_name:
                    step_functions_using_glue_job.append(step_function['name'])
    return step_functions_using_glue_job

# Streamlit app
def app():
    # Get step functions and glue jobs
    step_functions = get_step_functions()
    glue_jobs = get_glue_jobs()

    # Sidebar menu
    st.sidebar.title("Step Functions and Glue Jobs Dashboard")
    selected_menu = st.sidebar.radio("Select an option", ["Step Functions", "Glue Jobs"])

    # Display Step Functions
    if selected_menu == "Step Functions":
        st.header("Step Functions")
        st.write("Below is a list of all the Step Functions in your AWS account:")

        # Show all step functions in a table
        st.table(step_functions)

        # Show details for a selected Step Function
        selected_step_function_arn = st.selectbox("Select a Step Function to see details", step_functions["arn"].tolist())
        selected_step_function_name = step_functions[step_functions["arn"] == selected_step_function_arn]["name"].values[0]
        st.write(f"Showing details for Step Function: {selected_step_function_name} ({selected_step_function_arn})")

        # Get executions for selected Step Function
        executions = get_step_function_executions(selected_step_function_arn)

        # Show executions for selected Step Function
        st.write(f"Executions for {selected_step_function_name}:")
        st.table(executions.head(100))

    # Display Glue Jobs
    elif selected_menu == "Glue Jobs":
        st.header("Glue Jobs")
        st.write("Below is a list of all the Glue Jobs in your AWS account:")

        # Show all glue jobs in a table
        st.table(glue_jobs)

        # Show details for a selected Glue Job
        selected_glue_job_name = st.selectbox("Select a Glue Job to see details", glue_jobs["name"].tolist())
        selected_glue_job = glue_jobs[glue_jobs["name"] == selected_glue_job_name].iloc[0]
        st.write(f"Showing details for Glue Job: {selected_glue_job_name}")

        # Get executions for selected Glue Job
        executions = get_glue_job_executions(selected_glue_job_name)

        # Show executions for selected Glue Job
        st.write(f"Executions for {selected_glue_job_name}:")
        st.table(executions.head(100))

        # Show Step Functions that use selected Glue Job
        st.write(f"Step Functions using {selected_glue_job_name}:")
        step_functions_using_glue_job = get_step_functions_using_glue_job(selected_glue_job_name, step_functions)
        if len(step_functions_using_glue_job) > 0:
            st.table(step_functions_using_glue_job)
        else:
            st.write("No Step Functions use this Glue Job.")
elif selected_job:
    job = selected_job.split(" - ")[0]
    job_name = job.split(" (")[0]
    job_created = job.split(" (")[1].replace(")", "")
    job_version = glue_jobs[job]["version"]
    job_worker_type = glue_jobs[job]["default_arguments"]["--worker-type"]
    job_num_workers = glue_jobs[job]["default_arguments"]["--number-of-workers"]
    st.write(f"### {job_name}")
    st.write(f"**Created**: {job_created}")
    st.write(f"**Glue Version**: {job_version}")
    st.write(f"**Default Worker Type**: {job_worker_type}")
    st.write(f"**Default Number of Workers**: {job_num_workers}")
    st.write("")
    executions = get_glue_job_executions(job_name)
    if not executions:
        st.write("No executions found for this Glue Job.")
    else:
        st.write(f"### Last {NUM_EXECUTIONS} Executions")
        st.write("")
        for execution in executions[:NUM_EXECUTIONS]:
            status = execution["JobRun"]["JobRunState"]
            start_time = execution["JobRun"]["StartedOn"]
            end_time = execution["JobRun"]["CompletedOn"]
            duration = (end_time - start_time).seconds if end_time else 0
            input_params = execution["JobRun"]["Arguments"]["--job-bookmark-option"]
            st.write(
                f"**Status**: {status} | **Start Time**: {start_time} | **End Time**: {end_time} | **Duration (s)**: {duration}"
            )
            st.write(f"**Input Parameters**: {input_params}")
            st.write("")

    # Get Step Functions using selected Glue Job
    step_functions = get_step_functions()
    step_functions_using_job = get_step_functions_using_glue_job(
        job_name, step_functions
    )
    if not step_functions_using_job:
        st.write("No Step Functions use this Glue Job.")
    else:
        st.write(f"### Step Functions using {job_name}")
        st.write("")
        for step_function in step_functions_using_job:
            st.write(f"- {step_function}")
