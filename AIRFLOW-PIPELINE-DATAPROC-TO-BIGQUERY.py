from datetime import datetime
from airflow import DAG
from os import getenv
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSSynchronizeBucketsOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator



# VARIAVEIS

PROJECT_ID = getenv('PROJECT_ID', 'yago-estudos')
REGIAO = getenv('REGION', 'southamerica-east1')
LOCAL = getenv('LOCATION', 'southamerica-east1')
RAW_BUCKET = getenv('RAW_BUCKET', 'voos-ocorrencia')
JOB_BUCKET = getenv('JOB_BUCKET', 'gcp-processing-zone')
BUCKET_TRATADO = getenv('BUCKET_TRATADO', 'bucket-tratado')
DATAPROC_CLUSTER = getenv('DATAPROC_CLUSTER', 'voos-tratamento')
PYSPARK_URI = getenv('PYSPARK_URI', "gs://yago-estudo-bucket/pyspark-voos.py.py")
BQ_DATASET_NAME = getenv('BQ_DATASET_NAME', 'Voos-gov')
BQ_TABLE_NAME = getenv('BQ_TABLE_NAME', 'VoosTratados')


default_args = {
    'owner': 'yago sousa - pipeline',
    'depends_on_past': False,
    'email': ['yago.devb@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # Em caso de erros, tente rodar novamente apenas 1 vez
    'retries': 1,
    
}

with DAG(
    dag_id='ETL-OCORRENCIA-VOO-TO-BIGQUERY',
    default_args=default_args, #de onde vai pegar o conjunto de chaves e valores
    tags=['bigquery','dataproc'], #tags, usadas para organizar melhor as tarefas, podendo ser usadas para fazer filtros de pesquisa na página de dags
    start_date=datetime(year=2022, month=11, day=15),
    schedule_interval='@daily',
    catchup=False
 ) as dag:

    #criando um backet pra passagem de processamento do arquivo
    create_processing_bucket = GCSCreateBucketOperator(
        task_id="Criate-bucket-processing",
        bucket_name= JOB_BUCKET,
        storage_class="REGIONAL",
        location=LOCAL,
        labels={"env": "dev", "team": "airflow"}
        
    )

    tsync_origem_processing = GCSSynchronizeBucketsOperator(
        task_id='gcs_sync_origem_processing',
        source_bucket=RAW_BUCKET,
        destination_bucket = JOB_BUCKET,
        source_object='files/',
        destination_object='files/',
        allow_overwrite=True
       
    )

    cluster_config_yealp={
        "master_config": {
            "numInstances": 1,
            "machineTypeUri": "n1-standard-2",
            "diskConfig":{"bootDiskType": "pd-standard", "bootDiskSizeGb":100},},
        "worker_config":{
            "numInstances":2,
            "machineTypeUri": "n1-standard-2",
            "diskConfig":{"bootDiskType": "pd-standard", "bootDiskSizeGb":100},},
    }
    
    dataproc_cluster = DataprocCreateClusterOperator(
        task_id='Create_dataproc_cluster',
        use_if_exists=True,
        cluster_name=DATAPROC_CLUSTER,
        cluster_config=cluster_config_yealp,
        region=REGIAO
    )

create_processing_bucket >> tsync_origem_processing >> dataproc_cluster

