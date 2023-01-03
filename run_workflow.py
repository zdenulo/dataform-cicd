import time
import json
import logging
from google.cloud import workflows
from google.cloud.workflows import executions

workflows_client = workflows.WorkflowsClient()
execution_client = workflows.executions.ExecutionsClient()


def execute_workflow(gcp_project: str, location: str, workflow_name: str, input_data: dict):
    """Executes a Workflow
    :param gcp_project - name of the GCP Project
    :param location - location of the Cloud Workflow, i.e. us-central1
    :param workflow_name - name of the workflow
    :param input_data - dictionary of input data for the workflow that will be passed as JSON
    """
    parent = workflows_client.workflow_path(gcp_project, location, workflow_name)

    response = execution_client.create_execution(
        request={'parent': parent, 'execution': {'argument': json.dumps(input_data)}}, )
    execution_finished = False
    backoff_delay = 1
    logging.info('Poll every second for result...')
    while not execution_finished:
        execution = execution_client.get_execution(request={"name": response.name})
        execution_finished = execution.state != executions.Execution.State.ACTIVE

        if not execution_finished:
            logging.info('- Waiting for results...')
            time.sleep(backoff_delay)
            backoff_delay *= 2
        else:
            logging.info(f'Execution finished with state: {execution.state.name}')
            logging.info(execution.result)
            return execution.result


if __name__ == '__main__':
    gcp_project = ''
    workflow_name = 'dataform-pipeline'
    location = 'us-central1'

    input_data = {
        'full_refresh': False,
        'branch': 'main',
        'dataset': 'dataform_stage',
        'repository': 'df-test',
        'gcp_project': gcp_project
    }
    execute_workflow(gcp_project, location, workflow_name, input_data)
