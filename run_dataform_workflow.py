import logging
import time

from google.cloud import dataform_v1beta1

df_client = dataform_v1beta1.DataformClient()


def execute_workflow(repo_uri: str, compilation_result: str):
    """Run workflow based on the compilation"""
    request = dataform_v1beta1.CreateWorkflowInvocationRequest(
        parent=repo_uri,
        workflow_invocation=dataform_v1beta1.types.WorkflowInvocation(
            compilation_result=compilation_result
        )
    )

    response = df_client.create_workflow_invocation(request=request)
    name = response.name
    logging.info(f'created workflow invocation {name}')
    return name


def compile_workflow(repo_uri: str, gcp_project, bq_dataset: str, branch: str):
    """Compiles the code"""
    request = dataform_v1beta1.CreateCompilationResultRequest(
        parent=repo_uri,
        compilation_result=dataform_v1beta1.types.CompilationResult(
            git_commitish=branch,
            code_compilation_config=dataform_v1beta1.types.CompilationResult.CodeCompilationConfig(
                default_database=gcp_project,
                default_schema=bq_dataset,
            )
        )
    )
    response = df_client.create_compilation_result(request=request)
    name = response.name
    logging.info(f'compiled workflow {name}')
    return name


def get_workflow_state(workflow_invocation_id: str):
    """Checks the status of a workflow invocation"""
    while True:
        request = dataform_v1beta1.GetWorkflowInvocationRequest(
            name=workflow_invocation_id
        )
        response = df_client.get_workflow_invocation(request)
        state = response.state.name
        logging.info(f'workflow state: {state}')
        if state == 'RUNNING':
            time.sleep(10)
        elif state in ('FAILED', 'CANCELING', 'CANCELLED'):
            raise Exception(f'Error while running workflow {workflow_invocation_id}')
        elif state == 'SUCCEEDED':
            return


def run_workflow(gcp_project: str, location: str, repo_name: str, bq_dataset: str, branch: str):
    """Runs complete workflow, i.e. compile and invoke"""

    repo_uri = f'projects/{gcp_project}/locations/{location}/repositories/{repo_name}'
    compilation_result = compile_workflow(repo_uri, gcp_project, bq_dataset, branch)
    workflow_invocation_name = execute_workflow(repo_uri, compilation_result)
    get_workflow_state(workflow_invocation_name)


if __name__ == '__main__':
    gcp_project = ''
    location = 'us-central1'
    repo_name = 'df-test'
    bq_dataset = 'dataform_tutorial'
    branch = 'main'

    run_workflow(gcp_project, location, repo_name, bq_dataset, branch)
