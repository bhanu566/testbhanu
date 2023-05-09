import boto3
import json

# Replace <STATE_MACHINE_ARN> with the ARN of your Step Function state machine
state_machine_arn = '<STATE_MACHINE_ARN>'

# Create a Step Functions client
step_functions = boto3.client('stepfunctions')

# Get the definition of the state machine
definition = step_functions.describe_state_machine(stateMachineArn=state_machine_arn)['definition']

# Parse the definition to extract the Glue job names
glue_jobs = []
for state in json.loads(definition):
    if state.get('Type') == 'Task' and state.get('Resource').startswith('arn:aws:states:::glue:startJobRun'):
        glue_job = state['Resource'].split(':')[-1]
        glue_jobs.append(glue_job)

# Print the list of Glue jobs used in the state machine
print(glue_jobs)
