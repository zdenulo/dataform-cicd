# Dataform CI/CD pipeline example

This repository contains samples of how to execute Dataform workflows on Google Cloud.

More detailed information here: [https://www.the-swamp.info/blog/cicd-pipeline-dataform-google-cloud/](https://www.the-swamp.info/blog/cicd-pipeline-dataform-google-cloud/)  

## Files
`cloudbuild.yaml` - Cloud Build config to execute Cloud Workflow  
`workflow.yaml` - Cloud Workflow that compiles, invokes and checks result of Dataform execution  
`run_dataform_workflow.py` - run Dataform workflow with Python  
`run_workflow.py` - Execute Cloud Workflow (that executes Dataform) with Python