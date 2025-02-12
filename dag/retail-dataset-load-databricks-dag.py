from airflow import DAG
from airflow.operators.python import PythonOperator #to execute py fun as tasks within our DAG.
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
# from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3, time
from airflow.utils.dates import days_ago


# Specific imports for Databricks
from airflow.providers.databricks.operators.databricks import (
    DatabricksCreateJobsOperator,
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
)

default_args = {
    'owner': 'airflow',  # The owner of the DAG, typically used for monitoring and logging purposes
    'email': ['saiabhishekkatakam16@gmail.com'],  # List of email addresses to notify on task failure or retries
    'depends_on_past': False,  # Whether tasks should depend on the success of previous runs
    'catchup': False,  # Whether Airflow should backfill missed intervals (set to False to disable)
    'start_date': datetime(2024, 8, 11),  # The start date for the DAG; tasks won't run before this date
    'email_on_failure': True,  # Whether to send an email on task failure
    'email_on_retry': True,  # Whether to send an email on task retry
    'retries': 1,  # The number of times to retry a task in case of failure
    'retry_delay': timedelta(minutes=5),  # The delay between retries (in this case, 5 minutes)
}


# dag = DAG(
#     'retail-dataset-load-databricks-dag',
#     default_args=default_args,
#     description='A DAG to run ETL job',
#     schedule_interval='0 8 * * *'
# )


EMRJobFlowId = ''
dag_variable = Variable.get('retail_dataset_variable',deserialize_json=True)
topic_arn = dag_variable['topic_arn']
bucket_name = dag_variable['stage_bucket']
prefix = dag_variable['stage_prefix']

def stg_files_to_process(bucket_name, prefix):
    """
    List all S3 objects in a specified prefix.
    """
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    print(response)
    obj_list = []
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith(".parquet"):
                obj_list.append(obj['Key'])


    print(obj_list)

    if 'Contents' in response and len(obj_list) > 0 :
        return 'process_etl'
    else:
        return 'complete_etl'


def create_emr_cluster():
    # Create an EMR cluster using Boto3

    emr_client = boto3.client('emr', region_name='us-east-1')

    # Specify the configurations for your EMR cluster
    cluster_config = {
        'Name': 'retail-dataset-load-emr',
        'LogUri': 's3://aws-logs-058264291771-us-east-1/elasticmapreduce/',
        # S3 bucket where EMR logs will be stored
        'ReleaseLabel': 'emr-6.9.0',  # EMR release version
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'MasterInstanceGroup',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'CoreInstanceGroup',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,  # Keep cluster alive even after all steps are completed
            'TerminationProtected': False,  # Allow cluster termination
            'Ec2KeyName': 'emr-key-pair-26072024',  # EC2 key pair for SSH access
        },
        'Applications': [
            {'Name': 'Spark'},  # Include Spark application
            {'Name': 'Hadoop'},  # Include Hadoop application
            # Add other applications as needed
        ],
        'VisibleToAllUsers': True,  # Cluster visibility
        'JobFlowRole': 'AmazonEMR-InstanceProfile-20240726T202217',  # IAM role for EMR to access AWS services
        'ServiceRole': 'arn:aws:iam::975050215054:role/service-role/AmazonEMR-ServiceRole-20240726T202235',
        # IAM role for EMR service
    }

    response = emr_client.run_job_flow(**cluster_config)
    print("response :",response )

    cluster_id = response['JobFlowId']

    # Print the cluster ID
    print("EMR Cluster ID:", response['JobFlowId'])

    # Specify the cluster ID you want to check
    EMRJobFlowId = str(response['JobFlowId'])

    # Describe the cluster
    response = emr_client.describe_cluster(ClusterId=EMRJobFlowId)
    print("describe cluster response :",response )

    # Get the state of the cluster
    cluster_state = response['Cluster']['Status']['State']

    while cluster_state != 'WAITING':
        print("Current cluster state : {}".format(cluster_state))
        print("The cluster is not created yet. Will check cluster status after 2 mins")
        time.sleep(120)
        response = emr_client.describe_cluster(ClusterId=EMRJobFlowId)
        cluster_state = response['Cluster']['Status']['State']
        print("Cluster state after 2 mins : {}".format(cluster_state))

    return str(cluster_id)

def check_step_status(cluster_id, step_id):
    emr_client = boto3.client('emr', region_name='us-east-1')  # Replace 'your_region' with your AWS region
    while True:
        response = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = response['Step']['Status']['State']
        print("Step {} is currently in state: {}".format(step_id, state))
        if state in ['COMPLETED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(10)  # Wait for 10 seconds before checking again
        print("Wait for 10 seconds before checking again")

def run_spark_job(**kwargs):
    print("Inside spark job")
    # Retrieve the JobFlowId from the previous task's output
    job_flow_id = kwargs['ti'].xcom_pull(task_ids='create_emr_cluster')

    print("job_flow_id {}".format(job_flow_id))

    # Describe the cluster
    emr_client = boto3.client('emr', region_name='us-east-1')
    response = emr_client.describe_cluster(ClusterId=job_flow_id)

    # Get the state of the cluster
    cluster_state = response['Cluster']['Status']['State']

    print(f"response : {response}")
    print(f"cluster_state : {cluster_state}")

    spark_steps = [
        {
            'Name': 'Spark Job Step',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode', 'cluster',
                         's3://projectaws10042024/scripts/spark-submit-test-snippet.py']
            }
        }
    ]

    response = emr_client.add_job_flow_steps(JobFlowId=job_flow_id, Steps=spark_steps)

    print(response)

    step_id = response['StepIds'][0]

    print("Step added with ID:", step_id)

    # Check the status of the added step
    check_step_status(job_flow_id, step_id)

def terminate_emr_cluster(**kwargs):
    # Terminate a EMR cluster
    job_flow_id = kwargs['ti'].xcom_pull(task_ids='create_emr_cluster')
    # Terminate the EMR cluster using Boto3
    emr_client = boto3.client('emr', region_name='us-east-1')
    emr_client.terminate_job_flows(JobFlowIds=[job_flow_id])


def send_sns_message(topic_arn, message):
    # Create an SNS client
    sns_client = boto3.client('sns')

    # Send message
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message
    )

    # Print the MessageId of the published message
    print("MessageId of published message: ", response['MessageId'])

# Airflow Tasks
# specific DAG with additional parameters such as schedule_interval, start_date, and task-specific configurations like retries and retry_delay.
with DAG(
    'retail-dataset-load-databricks-dag', #Name of Dag reflected on UI
    default_args=default_args, #Refers to the default_args dictionary
    description='A simple Databricks DAG', #Description for our DAG.
    schedule_interval=None, # DAG is not automatically scheduled to run (i.e., it will not run on any fixed interval). Use corn expressions
    start_date=days_ago(1), #sets the start date of the DAG as 1 day ago from the current date. The start_date represents the logical start date for the DAG, and tasks will not run before this date.
    tags=['example'], #Adds metadata tags for organizational purposes. In the Airflow UI, these tags can help categorize and filter DAGs.
) as dag:
    start_task = DummyOperator(task_id='start_task', dag=dag)

    stg_files_check = BranchPythonOperator(task_id='stg_files_to_process',
                                           python_callable=stg_files_to_process,
                                           op_kwargs={'bucket_name': bucket_name,
                                                      'prefix': prefix}, dag=dag)

    process_etl = DummyOperator(task_id='process_etl', dag=dag)

    create_emr_task = PythonOperator(
        task_id='create_emr_cluster',
        python_callable=create_emr_cluster,
        dag=dag,
    )

    run_spark_job_task = PythonOperator(
        task_id='run_spark_job',
        python_callable=run_spark_job,
        provide_context=True,
        dag=dag,
    )

    terminate_emr_task = PythonOperator(
        task_id='terminate_emr_cluster',
        python_callable=terminate_emr_cluster,
        dag=dag,
    )

    glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name="glue_test_job",
        iam_role_name="glue_role",
        retry_limit=0,
        dag=dag
    )

    complete_etl = DummyOperator(task_id='complete_etl', dag=dag)

    notify_user_task = PythonOperator(
        task_id='notify_user_task',
        python_callable=send_sns_message,
        op_kwargs={'topic_arn': topic_arn, 'message': 'ETL DAG new_project_dag_glue completed'},
        provide_context=True,
        dag=dag,
    )

    end_task = DummyOperator(task_id='end_task', dag=dag)

    job = {
        "name": "Retail Dataset ETL Job",
        "tasks": [
            {
                "task_key": "test",
                "job_cluster_key": "job_cluster",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/prashantbuk1993@gmail.com/prashant_aws",
                },
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "job_cluster",
                "new_cluster": {
                    "spark_version": "14.3.x-scala2.12",
                    "node_type_id": "m5d.large",
                    "num_workers": 1,
                },
            },
        ],
    }

    jobs_create_json = DatabricksCreateJobsOperator(task_id="jobs_create_json", json=job)

    # Example of using the DatabricksRunNowOperator after creating a job with DatabricksCreateJobsOperator.
    run_now = DatabricksRunNowOperator(task_id="run_now", job_id="{{ ti.xcom_pull(task_ids='jobs_create_json') }}")

    start_task >> stg_files_check
    stg_files_check >> process_etl >> glue_job >> complete_etl
    stg_files_check >> process_etl >> create_emr_task >> run_spark_job_task >> terminate_emr_task >> complete_etl
    stg_files_check >> process_etl >> jobs_create_json >> run_now >> complete_etl
    stg_files_check >> complete_etl
    complete_etl >> notify_user_task >> end_task




