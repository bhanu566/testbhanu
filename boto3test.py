import boto3
import json
import streamlit as st

# Load AWS credentials from a file
with open('credentials.json', 'r') as f:
    credentials = json.load(f)

# Create a Boto3 session using the credentials
session = boto3.Session(
    aws_access_key_id=credentials['aws_access_key_id'],
    aws_secret_access_key=credentials['aws_secret_access_key'],
    region_name=credentials['region_name']
)

# Create a Boto3 client for Step Functions
stepfunctions = session.client('stepfunctions')

# Create a Boto3 client for Glue
glue = session.client('glue')

# Retrieve a list of Step Functions with names starting with "bhanu_chennam"
stepfunction_names = []
response = stepfunctions.list_state_machines()
while True:
    for machine in response['stateMachines']:
        if machine['name'].startswith('bhanu_chennam'):
            stepfunction_names.append(machine['name'])
    if 'nextToken' in response:
        response = stepfunctions.list_state_machines(nextToken=response['nextToken'])
    else:
        break

# Create a Streamlit app to display the information
st.title('AWS Step Functions and Glue Jobs')

# Display a dropdown to select a Step Function
selected_stepfunction_name = st.selectbox('Select a Step Function', stepfunction_names)

# Retrieve the Step Function definition
response = stepfunctions.describe_state_machine(stateMachineArn=f'arn:aws:states:{credentials["region_name"]}:{credentials["account_id"]}:stateMachine:{selected_stepfunction_name}')
stepfunction_definition = response['definition']

# Parse the definition to find all Glue jobs used in the Step Function
glue_job_names = []
for state in stepfunction_definition:
    if state['Type'] == 'Task' and state['Resource'].startswith('arn:aws:states:::glue:startJobRun'):
        glue_job_names.append(state['Resource'].split(':')[-1])

# Display a table of Glue jobs used in the Step Function
st.write(f'Glue Jobs used in {selected_stepfunction_name}')
if not glue_job_names:
    st.write('No Glue jobs found in the Step Function')
else:
    table_data = []
    for job_name in glue_job_names:
        table_data.append({
            'Name': job_name
        })
    st.table(table_data)

    # Retrieve the last 100 executions for each Glue job
    for job_name in glue_job_names:
        response = glue.get_job_runs(JobName=job_name, MaxResults=100)
        if response['JobRuns']:
            # Display a table of execution details for the Glue job
            st.write(f'Execution details for {job_name}')
            table_data = []
            for run in response['JobRuns']:
                table_data.append({
                    'Execution ID': run['Id'],
                    'Status': run['JobRunState'],
                    'Time Taken (Seconds)': run['ExecutionTime'] if run['ExecutionTime'] else '',
                    'Input Parameters': run['Arguments'] if run['Arguments'] else ''
                })
            st.table(table_data)
        else:
            st.write(f'No executions found for {job_name}')
