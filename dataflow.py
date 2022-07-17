from google.cloud import dataflow_v1beta3

def create_job_from_template(service_account_file, project_id, inputTopic, bqCollection, bqOutputTable):
    # Create a TemplateServiceClient to create dataflow jobs from templates
    client = dataflow_v1beta3.TemplatesServiceClient.from_service_account_file(service_account_file)
    # define a runtime for the job/worker
    runtime = dataflow_v1beta3.RuntimeEnvironment(
            num_workers=1,
            max_workers=2,
            zone='us-central1-a',
            temp_location="gs://pccproject/dataflow_metadata/" ,
            machine_type="n1-standard-1")

    # Initialize request argument(s)
    request = dataflow_v1beta3.CreateJobFromTemplateRequest(
        # parameters of the template
        parameters= {
            "inputTopic": f"projects/{project_id}/topics/{inputTopic}",
            "outputTableSpec": f"{project_id}:{bqCollection}.{bqOutputTable}"
        },
        # other request arguments
        project_id = "pccreverselogistic",  
        job_name = "ps_to_bq_py",
        gcs_path="gs://dataflow-templates-us-central1/latest/PubSub_to_BigQuery",
        environment=runtime,
        location="us-central1"
    )
    # create the job
    try:
        job = client.create_job_from_template(request=request) 
        print("Job {} created ...".format(job.id))
        return job
    except Exception as e:
        return e
