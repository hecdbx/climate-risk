# Databricks notebook source
# MAGIC %md
# MAGIC # Climate Risk Insurance - Databricks SDK Pipeline Setup
# MAGIC
# MAGIC This notebook demonstrates how to programmatically create and manage data pipelines using the Databricks Python SDK.
# MAGIC
# MAGIC ## What This Notebook Creates:
# MAGIC 1. **Unified Delta Live Tables Pipeline** - Single pipeline for all data ingestion, staging, and processing
# MAGIC 2. **Jobs for Training Workflows** - ML model training and deployment
# MAGIC 3. **Streamlined Lakeflow** - Simplified data flow with unified schema
# MAGIC 4. **Monitoring and Alerting** - Pipeline health and performance tracking
# MAGIC
# MAGIC ## Prerequisites:
# MAGIC - Databricks workspace with appropriate permissions
# MAGIC - Unity Catalog enabled with volumes created
# MAGIC - Access to create pipelines and jobs
# MAGIC - Unity Catalog schema with volumes (run unity_catalog_schema_setup.py first)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

# Install required packages
%pip install databricks-sdk databricks-cli --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Import required libraries
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines, jobs, compute
from databricks.sdk.service.pipelines import (
    CreatePipeline, PipelineLibrary, NotebookLibrary, PipelineCluster,
    AutoScale, PipelineSettings, PipelineEdition
)
from databricks.sdk.service.jobs import (
    CreateJob, JobSettings, NotebookTask, NewCluster, JobCluster
)
import json
import time

# COMMAND ----------

# Configuration parameters
# Base configuration
catalog_name = "demo_hc"
environment = "development"  # development, staging, production

config = {
    "catalog_name": catalog_name,
    "environment": environment,
    "resource_prefix": "climate_risk",
    "workspace_url": spark.conf.get("spark.databricks.workspaceUrl"),
    "pipeline_target_schema": "climate_risk",
    "raw_data_path": f"/Volumes/{catalog_name}/climate_risk/data_volume/raw_data/",
    "checkpoint_path": f"/Volumes/{catalog_name}/climate_risk/data_volume/pipeline_checkpoints/",
    "storage_location": f"/Volumes/{catalog_name}/climate_risk/data_volume/processed_data/",
    "model_artifacts_path": f"/Volumes/{catalog_name}/climate_risk/data_volume/model_artifacts/",
    "staging_path": f"/Volumes/{catalog_name}/climate_risk/data_volume/staging/",
    "analytics_path": f"/Volumes/{catalog_name}/climate_risk/data_volume/analytics/"
}

print("Configuration:")
for key, value in config.items():
    print(f"  {key}: {value}")

# COMMAND ----------

# Initialize Databricks SDK client
w = WorkspaceClient()

# Verify connection
current_user = w.current_user.me()
print(f"Connected as: {current_user.user_name}")
print(f"Workspace: {config['workspace_url']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delta Live Tables Pipeline Creation

# COMMAND ----------

# Create Unified Climate Risk Data Pipeline
def create_unified_climate_risk_pipeline():
    """Create single DLT pipeline for all climate data ingestion, staging, and processing"""
    
    pipeline_name = f"{config['resource_prefix']}_unified_pipeline_{config['environment']}"
    
    # Pipeline configuration with multiple notebooks for different data sources
    pipeline_config = CreatePipeline(
        name=pipeline_name,
        storage=config['storage_location'],
        target=f"{config['catalog_name']}.{config['pipeline_target_schema']}",
        libraries=[
            PipelineLibrary(
                notebook=NotebookLibrary(
                    path="/Repos/climate-risk/lakeflow/01_accuweather_ingestion_pipeline"
                )
            ),
            PipelineLibrary(
                notebook=NotebookLibrary(
                    path="/Repos/climate-risk/lakeflow/02_historical_data_processing_pipeline"
                )
            ),
            PipelineLibrary(
                notebook=NotebookLibrary(
                    path="/Repos/climate-risk/lakeflow/climate_risk_workflow"
                )
            )
        ],
        clusters=[
            PipelineCluster(
                label="ingestion_cluster",
                autoscale=AutoScale(min_workers=2, max_workers=8),
                node_type_id="i3.xlarge",
                driver_node_type_id="i3.xlarge",
                spark_conf={
                    "spark.databricks.delta.preview.enabled": "true",
                    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
                    "spark.databricks.delta.autoCompact.enabled": "true",
                    "spark.databricks.delta.optimizeWrite.enabled": "true"
                },
                custom_tags={
                    "project": "climate-risk",
                    "environment": config['environment'],
                    "pipeline_type": "unified_ingestion"
                }
            ),
            PipelineCluster(
                label="processing_cluster",
                autoscale=AutoScale(min_workers=3, max_workers=12),
                node_type_id="i3.2xlarge",
                driver_node_type_id="i3.2xlarge",
                spark_conf={
                    "spark.databricks.delta.preview.enabled": "true",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                },
                custom_tags={
                    "project": "climate-risk",
                    "environment": config['environment'],
                    "pipeline_type": "unified_processing"
                }
            )
        ],
        configuration={
            "catalog_name": config['catalog_name'],
            "schema_name": config['pipeline_target_schema'],
            "raw_data_path": config['raw_data_path'],
            "checkpoint_path": config['checkpoint_path'],
            "staging_table_prefix": "staging_",
            "processed_table_prefix": "",
            "pipeline.trigger.interval": "10 minutes",
            "data_quality.quarantine_table": "data_quality_quarantine"
        },
        continuous=False,
        development=True if config['environment'] == 'development' else False,
        edition=PipelineEdition.ADVANCED,
        photon=True
    )
    
    try:
        pipeline = w.pipelines.create(pipeline_config)
        print(f"✅ Created unified climate risk pipeline: {pipeline.pipeline_id}")
        return pipeline.pipeline_id
    except Exception as e:
        print(f"❌ Failed to create unified pipeline: {str(e)}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ML Training Job Creation

# COMMAND ----------

# Create Historical Data Processing Pipeline (DEPRECATED - Now part of unified pipeline)
def create_historical_processing_pipeline_deprecated():
    """Create DLT pipeline for historical climate data processing"""
    
    pipeline_name = f"{config['resource_prefix']}_historical_processing_{config['environment']}"
    
    pipeline_config = CreatePipeline(
        name=pipeline_name,
        storage=config['storage_location'] + "historical/",
        target=f"{config['catalog_name']}.{config['pipeline_target_schema']}",
        libraries=[
            PipelineLibrary(
                notebook=NotebookLibrary(
                    path="/Repos/climate-risk/lakeflow/02_historical_data_processing_pipeline"
                )
            )
        ],
        clusters=[
            PipelineCluster(
                label="default",
                autoscale=AutoScale(min_workers=2, max_workers=10),
                node_type_id="i3.2xlarge",
                driver_node_type_id="i3.2xlarge",
                spark_conf={
                    "spark.databricks.delta.preview.enabled": "true",
                    "spark.databricks.delta.autoCompact.enabled": "true",
                    "spark.databricks.delta.optimizeWrite.enabled": "true"
                },
                custom_tags={
                    "project": "climate-risk",
                    "environment": config['environment'],
                    "pipeline_type": "processing"
                }
            )
        ],
        configuration={
            "catalog_name": config['catalog_name'],
            "raw_data_path": config['raw_data_path'] + "historical/",
            "checkpoint_path": config['checkpoint_path'] + "historical/",
            "processing.batch_size": "1000000",
            "processing.parallelism": "auto"
        },
        continuous=False,
        development=True if config['environment'] == 'development' else False,
        edition=PipelineEdition.ADVANCED,
        photon=True
    )
    
    try:
        pipeline = w.pipelines.create(pipeline_config)
        print(f"✅ Created historical processing pipeline: {pipeline.pipeline_id}")
        return pipeline.pipeline_id
    except Exception as e:
        print(f"❌ Failed to create historical processing pipeline: {str(e)}")
        return None

# COMMAND ----------

# Create Climate Risk Workflow Pipeline
def create_climate_risk_workflow():
    """Create comprehensive climate risk workflow pipeline"""
    
    pipeline_name = f"{config['resource_prefix']}_risk_workflow_{config['environment']}"
    
    pipeline_config = CreatePipeline(
        name=pipeline_name,
        storage=config['storage_location'] + "risk_models/",
        target=f"{config['catalog_name']}.risk_models",
        libraries=[
            PipelineLibrary(
                notebook=NotebookLibrary(
                    path="/Repos/climate-risk/lakeflow/climate_risk_workflow"
                )
            )
        ],
        clusters=[
            PipelineCluster(
                label="default",
                autoscale=AutoScale(min_workers=3, max_workers=15),
                node_type_id="i3.4xlarge",
                driver_node_type_id="i3.4xlarge",
                spark_conf={
                    "spark.databricks.delta.preview.enabled": "true",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                },
                custom_tags={
                    "project": "climate-risk",
                    "environment": config['environment'],
                    "pipeline_type": "risk_modeling"
                }
            )
        ],
        configuration={
            "catalog_name": config['catalog_name'],
            "risk_models_path": config['storage_location'] + "risk_models/",
            "checkpoint_path": config['checkpoint_path'] + "risk_models/",
            "model.drought_threshold": "0.3",
            "model.flood_threshold": "0.4",
            "model.confidence_threshold": "0.7"
        },
        continuous=False,
        development=True if config['environment'] == 'development' else False,
        edition=PipelineEdition.ADVANCED,
        photon=True
    )
    
    try:
        pipeline = w.pipelines.create(pipeline_config)
        print(f"✅ Created climate risk workflow pipeline: {pipeline.pipeline_id}")
        return pipeline.pipeline_id
    except Exception as e:
        print(f"❌ Failed to create climate risk workflow: {str(e)}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ML Training Job Creation

# COMMAND ----------

# Create Drought Risk Model Training Job
def create_drought_model_training_job():
    """Create job for drought risk model training"""
    
    job_name = f"{config['resource_prefix']}_drought_training_{config['environment']}"
    
    job_config = CreateJob(
        name=job_name,
        job_clusters=[
            JobCluster(
                job_cluster_key="drought_training_cluster",
                new_cluster=NewCluster(
                    spark_version="13.3.x-cpu-ml-scala2.12",
                    node_type_id="i3.2xlarge",
                    driver_node_type_id="i3.2xlarge",
                    autoscale=AutoScale(min_workers=2, max_workers=8),
                    spark_conf={
                        "spark.databricks.delta.preview.enabled": "true",
                        "spark.databricks.ml.enabled": "true"
                    },
                    custom_tags={
                        "project": "climate-risk",
                        "environment": config['environment'],
                        "job_type": "ml_training"
                    }
                )
            )
        ],
        tasks=[
            jobs.Task(
                task_key="drought_model_training",
                job_cluster_key="drought_training_cluster",
                notebook_task=NotebookTask(
                    notebook_path="/Repos/climate-risk/notebooks/01_drought_risk_model",
                    base_parameters={
                        "catalog_name": config['catalog_name'],
                        "environment": config['environment'],
                        "model_name": "drought_risk_model",
                        "training_data_path": f"{config['catalog_name']}.{config['pipeline_target_schema']}.climate_aggregations"
                    }
                ),
                timeout_seconds=7200,  # 2 hours
                max_retries=2
            ),
            jobs.Task(
                task_key="model_evaluation",
                job_cluster_key="drought_training_cluster",
                depends_on=[jobs.TaskDependency(task_key="drought_model_training")],
                notebook_task=NotebookTask(
                    notebook_path="/Repos/climate-risk/notebooks/model_evaluation",
                    base_parameters={
                        "catalog_name": config['catalog_name'],
                        "model_type": "drought",
                        "environment": config['environment']
                    }
                ),
                timeout_seconds=1800,  # 30 minutes
                max_retries=1
            )
        ],
        schedule=jobs.CronSchedule(
            quartz_cron_expression="0 0 2 * * ?",  # Daily at 2 AM
            timezone_id="UTC"
        ),
        max_concurrent_runs=1,
        tags={
            "project": "climate-risk",
            "model_type": "drought",
            "environment": config['environment']
        }
    )
    
    try:
        job = w.jobs.create(job_config)
        print(f"✅ Created drought model training job: {job.job_id}")
        return job.job_id
    except Exception as e:
        print(f"❌ Failed to create drought training job: {str(e)}")
        return None

# COMMAND ----------

# Create Flood Risk Model Training Job
def create_flood_model_training_job():
    """Create job for flood risk model training"""
    
    job_name = f"{config['resource_prefix']}_flood_training_{config['environment']}"
    
    job_config = CreateJob(
        name=job_name,
        job_clusters=[
            JobCluster(
                job_cluster_key="flood_training_cluster",
                new_cluster=NewCluster(
                    spark_version="13.3.x-cpu-ml-scala2.12",
                    node_type_id="i3.2xlarge",
                    driver_node_type_id="i3.2xlarge",
                    autoscale=AutoScale(min_workers=2, max_workers=8),
                    spark_conf={
                        "spark.databricks.delta.preview.enabled": "true",
                        "spark.databricks.ml.enabled": "true"
                    },
                    custom_tags={
                        "project": "climate-risk",
                        "environment": config['environment'],
                        "job_type": "ml_training"
                    }
                )
            )
        ],
        tasks=[
            jobs.Task(
                task_key="flood_model_training",
                job_cluster_key="flood_training_cluster",
                notebook_task=NotebookTask(
                    notebook_path="/Repos/climate-risk/notebooks/02_flood_risk_model",
                    base_parameters={
                        "catalog_name": config['catalog_name'],
                        "environment": config['environment'],
                        "model_name": "flood_risk_model",
                        "training_data_path": f"{config['catalog_name']}.{config['pipeline_target_schema']}.climate_aggregations"
                    }
                ),
                timeout_seconds=7200,  # 2 hours
                max_retries=2
            ),
            jobs.Task(
                task_key="model_evaluation",
                job_cluster_key="flood_training_cluster",
                depends_on=[jobs.TaskDependency(task_key="flood_model_training")],
                notebook_task=NotebookTask(
                    notebook_path="/Repos/climate-risk/notebooks/model_evaluation",
                    base_parameters={
                        "catalog_name": config['catalog_name'],
                        "model_type": "flood",
                        "environment": config['environment']
                    }
                ),
                timeout_seconds=1800,  # 30 minutes
                max_retries=1
            )
        ],
        schedule=jobs.CronSchedule(
            quartz_cron_expression="0 0 3 * * ?",  # Daily at 3 AM
            timezone_id="UTC"
        ),
        max_concurrent_runs=1,
        tags={
            "project": "climate-risk",
            "model_type": "flood",
            "environment": config['environment']
        }
    )
    
    try:
        job = w.jobs.create(job_config)
        print(f"✅ Created flood model training job: {job.job_id}")
        return job.job_id
    except Exception as e:
        print(f"❌ Failed to create flood training job: {str(e)}")
        return None

# COMMAND ----------

# Create Model Deployment Job
def create_model_deployment_job():
    """Create job for model deployment and serving setup"""
    
    job_name = f"{config['resource_prefix']}_model_deployment_{config['environment']}"
    
    job_config = CreateJob(
        name=job_name,
        job_clusters=[
            JobCluster(
                job_cluster_key="deployment_cluster",
                new_cluster=NewCluster(
                    spark_version="13.3.x-cpu-ml-scala2.12",
                    node_type_id="i3.xlarge",
                    driver_node_type_id="i3.xlarge",
                    num_workers=1,
                    spark_conf={
                        "spark.databricks.delta.preview.enabled": "true"
                    },
                    custom_tags={
                        "project": "climate-risk",
                        "environment": config['environment'],
                        "job_type": "deployment"
                    }
                )
            )
        ],
        tasks=[
            jobs.Task(
                task_key="deploy_models",
                job_cluster_key="deployment_cluster",
                notebook_task=NotebookTask(
                    notebook_path="/Repos/climate-risk/notebooks/04_model_deployment",
                    base_parameters={
                        "catalog_name": config['catalog_name'],
                        "environment": config['environment'],
                        "deployment_stage": "staging" if config['environment'] == 'development' else "production"
                    }
                ),
                timeout_seconds=3600,  # 1 hour
                max_retries=1
            )
        ],
        # Manual trigger for deployment
        max_concurrent_runs=1,
        tags={
            "project": "climate-risk",
            "job_type": "deployment",
            "environment": config['environment']
        }
    )
    
    try:
        job = w.jobs.create(job_config)
        print(f"✅ Created model deployment job: {job.job_id}")
        return job.job_id
    except Exception as e:
        print(f"❌ Failed to create deployment job: {str(e)}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Complete Lakeflow Orchestration

# COMMAND ----------

# Create Master Orchestration Job
def create_master_orchestration_job(pipeline_ids, training_job_ids):
    """Create master job that orchestrates the entire pipeline"""
    
    job_name = f"{config['resource_prefix']}_master_orchestration_{config['environment']}"
    
    # Create tasks for pipeline execution
    tasks = []
    
    # Add unified pipeline task
    if pipeline_ids.get('unified'):
        tasks.append(
            jobs.Task(
                task_key="run_unified_climate_pipeline",
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=pipeline_ids['unified']
                ),
                timeout_seconds=10800,  # 3 hours for complete pipeline
                max_retries=2
            )
        )
    
    # Add training job triggers
    if training_job_ids.get('drought'):
        tasks.append(
            jobs.Task(
                task_key="trigger_drought_training",
                run_job_task=jobs.RunJobTask(
                    job_id=training_job_ids['drought']
                ),
                depends_on=[
                    jobs.TaskDependency(task_key="run_unified_climate_pipeline")
                ] if pipeline_ids.get('unified') else [],
                timeout_seconds=10800,  # 3 hours
                max_retries=1
            )
        )
    
    if training_job_ids.get('flood'):
        tasks.append(
            jobs.Task(
                task_key="trigger_flood_training",
                run_job_task=jobs.RunJobTask(
                    job_id=training_job_ids['flood']
                ),
                depends_on=[
                    jobs.TaskDependency(task_key="trigger_drought_training")
                ] if training_job_ids.get('drought') else [],
                timeout_seconds=10800,  # 3 hours
                max_retries=1
            )
        )
    
    job_config = CreateJob(
        name=job_name,
        tasks=tasks,
        schedule=jobs.CronSchedule(
            quartz_cron_expression="0 0 1 * * ?",  # Daily at 1 AM
            timezone_id="UTC"
        ),
        max_concurrent_runs=1,
        tags={
            "project": "climate-risk",
            "job_type": "orchestration",
            "environment": config['environment']
        },
        email_notifications=jobs.JobEmailNotifications(
            on_failure=["data-engineering@company.com"],
            on_success=["data-engineering@company.com"]
        )
    )
    
    try:
        job = w.jobs.create(job_config)
        print(f"✅ Created master orchestration job: {job.job_id}")
        return job.job_id
    except Exception as e:
        print(f"❌ Failed to create orchestration job: {str(e)}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execute Pipeline Creation

# COMMAND ----------

# Execute all pipeline and job creation
def create_all_pipelines_and_jobs():
    """Create all pipelines and jobs for the climate risk system"""
    
    print("🚀 Starting pipeline and job creation...")
    print("=" * 60)
    
    # Track created resources
    pipeline_ids = {}
    job_ids = {}
    
    # Create Unified DLT Pipeline
    print("\n📊 Creating Unified Delta Live Tables Pipeline...")
    pipeline_ids['unified'] = create_unified_climate_risk_pipeline()
    
    # Create Training Jobs
    print("\n🤖 Creating ML Training Jobs...")
    job_ids['drought'] = create_drought_model_training_job()
    job_ids['flood'] = create_flood_model_training_job()
    job_ids['deployment'] = create_model_deployment_job()
    
    # Create Master Orchestration
    print("\n🎯 Creating Master Orchestration...")
    job_ids['orchestration'] = create_master_orchestration_job(pipeline_ids, job_ids)
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 CREATION SUMMARY")
    print("=" * 60)
    
    print("\n🔄 Unified Delta Live Tables Pipeline:")
    for name, pid in pipeline_ids.items():
        status = "✅ Created" if pid else "❌ Failed"
        print(f"  {name}: {status} ({pid})")
    
    print("\n⚙️ Training and Deployment Jobs:")
    for name, jid in job_ids.items():
        status = "✅ Created" if jid else "❌ Failed"
        print(f"  {name}: {status} ({jid})")
    
    return pipeline_ids, job_ids

# COMMAND ----------

# Execute the creation
pipeline_ids, job_ids = create_all_pipelines_and_jobs()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pipeline Management and Monitoring

# COMMAND ----------

# Pipeline monitoring functions
def check_pipeline_status(pipeline_id):
    """Check the status of a specific pipeline"""
    try:
        pipeline = w.pipelines.get(pipeline_id)
        print(f"Pipeline {pipeline_id}:")
        print(f"  Name: {pipeline.name}")
        print(f"  State: {pipeline.state}")
        print(f"  Health: {pipeline.health}")
        return pipeline.state
    except Exception as e:
        print(f"❌ Error checking pipeline {pipeline_id}: {str(e)}")
        return None

# COMMAND ----------

# Job monitoring functions
def check_job_status(job_id):
    """Check the status of a specific job"""
    try:
        job = w.jobs.get(job_id)
        print(f"Job {job_id}:")
        print(f"  Name: {job.settings.name}")
        
        # Get recent runs
        runs = w.jobs.list_runs(job_id=job_id, limit=5)
        if runs:
            print(f"  Recent runs:")
            for run in runs:
                print(f"    Run {run.run_id}: {run.state.life_cycle_state}")
        else:
            print(f"  No recent runs")
        
        return job.settings
    except Exception as e:
        print(f"❌ Error checking job {job_id}: {str(e)}")
        return None

# COMMAND ----------

# Start a pipeline manually
def start_pipeline(pipeline_id, full_refresh=False):
    """Start a pipeline manually"""
    try:
        update = w.pipelines.start_update(
            pipeline_id=pipeline_id,
            full_refresh=full_refresh
        )
        print(f"✅ Started pipeline {pipeline_id}, update ID: {update.update_id}")
        return update.update_id
    except Exception as e:
        print(f"❌ Failed to start pipeline {pipeline_id}: {str(e)}")
        return None

# COMMAND ----------

# Run a job manually
def run_job(job_id, parameters=None):
    """Run a job manually"""
    try:
        run = w.jobs.run_now(
            job_id=job_id,
            notebook_params=parameters or {}
        )
        print(f"✅ Started job {job_id}, run ID: {run.run_id}")
        return run.run_id
    except Exception as e:
        print(f"❌ Failed to run job {job_id}: {str(e)}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resource Management

# COMMAND ----------

# List all created resources
def list_climate_risk_resources():
    """List all climate risk related resources"""
    
    print("🔍 Climate Risk Resources:")
    print("=" * 50)
    
    # List pipelines
    print("\n📊 Delta Live Tables Pipelines:")
    try:
        pipelines = w.pipelines.list_pipelines()
        climate_pipelines = [p for p in pipelines if config['resource_prefix'] in p.name]
        for pipeline in climate_pipelines:
            print(f"  {pipeline.name} ({pipeline.pipeline_id})")
    except Exception as e:
        print(f"  ❌ Error listing pipelines: {str(e)}")
    
    # List jobs
    print("\n⚙️ Jobs:")
    try:
        jobs_list = w.jobs.list()
        climate_jobs = [j for j in jobs_list if config['resource_prefix'] in j.settings.name]
        for job in climate_jobs:
            print(f"  {job.settings.name} ({job.job_id})")
    except Exception as e:
        print(f"  ❌ Error listing jobs: {str(e)}")

# COMMAND ----------

# Clean up resources (use with caution)
def cleanup_climate_risk_resources(confirm=False):
    """Delete all climate risk related resources"""
    
    if not confirm:
        print("⚠️  This will delete ALL climate risk resources!")
        print("Set confirm=True to proceed")
        return
    
    print("🗑️  Cleaning up climate risk resources...")
    
    # Delete pipelines
    try:
        pipelines = w.pipelines.list_pipelines()
        climate_pipelines = [p for p in pipelines if config['resource_prefix'] in p.name]
        for pipeline in climate_pipelines:
            w.pipelines.delete(pipeline.pipeline_id)
            print(f"  ✅ Deleted pipeline: {pipeline.name}")
    except Exception as e:
        print(f"  ❌ Error deleting pipelines: {str(e)}")
    
    # Delete jobs
    try:
        jobs_list = w.jobs.list()
        climate_jobs = [j for j in jobs_list if config['resource_prefix'] in j.settings.name]
        for job in climate_jobs:
            w.jobs.delete(job.job_id)
            print(f"  ✅ Deleted job: {job.settings.name}")
    except Exception as e:
        print(f"  ❌ Error deleting jobs: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validation and Testing

# COMMAND ----------

# Validate pipeline configuration
def validate_pipeline_setup():
    """Validate that all pipelines and jobs are properly configured"""
    
    print("🔍 Validating pipeline setup...")
    print("=" * 50)
    
    validation_results = {
        'pipelines': {},
        'jobs': {},
        'overall': True
    }
    
    # Check pipelines
    for name, pipeline_id in pipeline_ids.items():
        if pipeline_id:
            try:
                pipeline = w.pipelines.get(pipeline_id)
                validation_results['pipelines'][name] = {
                    'status': 'valid',
                    'id': pipeline_id,
                    'name': pipeline.name
                }
                print(f"  ✅ Pipeline {name}: Valid")
            except Exception as e:
                validation_results['pipelines'][name] = {
                    'status': 'error',
                    'error': str(e)
                }
                validation_results['overall'] = False
                print(f"  ❌ Pipeline {name}: Error - {str(e)}")
        else:
            validation_results['pipelines'][name] = {'status': 'not_created'}
            validation_results['overall'] = False
            print(f"  ❌ Pipeline {name}: Not created")
    
    # Check jobs
    for name, job_id in job_ids.items():
        if job_id:
            try:
                job = w.jobs.get(job_id)
                validation_results['jobs'][name] = {
                    'status': 'valid',
                    'id': job_id,
                    'name': job.settings.name
                }
                print(f"  ✅ Job {name}: Valid")
            except Exception as e:
                validation_results['jobs'][name] = {
                    'status': 'error',
                    'error': str(e)
                }
                validation_results['overall'] = False
                print(f"  ❌ Job {name}: Error - {str(e)}")
        else:
            validation_results['jobs'][name] = {'status': 'not_created'}
            validation_results['overall'] = False
            print(f"  ❌ Job {name}: Not created")
    
    print(f"\n🎯 Overall validation: {'✅ PASSED' if validation_results['overall'] else '❌ FAILED'}")
    return validation_results

# COMMAND ----------

# Run validation
validation_results = validate_pipeline_setup()

# COMMAND ----------

# Display current resources
list_climate_risk_resources()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC
# MAGIC This notebook has created a comprehensive data pipeline infrastructure for the Climate Risk Insurance project using the Databricks Python SDK.
# MAGIC
# MAGIC ### ✅ What Was Created:
# MAGIC
# MAGIC #### **Delta Live Tables Pipelines:**
# MAGIC 1. **AccuWeather Ingestion Pipeline** - Real-time weather data ingestion
# MAGIC 2. **Historical Processing Pipeline** - Batch processing of historical climate data
# MAGIC 3. **Climate Risk Workflow** - End-to-end risk assessment pipeline
# MAGIC
# MAGIC #### **ML Training Jobs:**
# MAGIC 1. **Drought Risk Model Training** - Automated model training and evaluation
# MAGIC 2. **Flood Risk Model Training** - Automated model training and evaluation  
# MAGIC 3. **Model Deployment Job** - Model serving setup and deployment
# MAGIC
# MAGIC #### **Orchestration:**
# MAGIC 1. **Master Orchestration Job** - Coordinates all pipelines and training workflows
# MAGIC 2. **Scheduling** - Automated daily execution at optimal times
# MAGIC 3. **Monitoring** - Email notifications and error handling
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Configure Notebook Paths**: Update the notebook paths in the pipeline configurations to match your repository structure
# MAGIC 2. **Set Up Data Sources**: Configure the data source connections (AccuWeather API, historical data sources)
# MAGIC 3. **Test Pipelines**: Run individual pipelines to validate data flow
# MAGIC 4. **Monitor Execution**: Use the monitoring functions to track pipeline health
# MAGIC 5. **Customize Configuration**: Adjust cluster sizes, schedules, and parameters for your specific needs
# MAGIC
# MAGIC ### 🔧 Management Commands:
# MAGIC
# MAGIC ```python
# MAGIC # Check status of all resources
# MAGIC list_climate_risk_resources()
# MAGIC
# MAGIC # Validate configuration
# MAGIC validation_results = validate_pipeline_setup()
# MAGIC
# MAGIC # Start a pipeline manually
# MAGIC start_pipeline(pipeline_ids['accuweather'], full_refresh=True)
# MAGIC
# MAGIC # Run a training job manually
# MAGIC run_job(job_ids['drought'])
# MAGIC
# MAGIC # Clean up all resources (use with caution)
# MAGIC # cleanup_climate_risk_resources(confirm=True)
# MAGIC ```
# MAGIC
# MAGIC The infrastructure is now ready for production use! 🎉
