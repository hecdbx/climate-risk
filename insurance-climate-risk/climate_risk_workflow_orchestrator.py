# Databricks notebook source
# MAGIC %md
# MAGIC # Climate Risk Workflow Orchestrator
# MAGIC
# MAGIC This notebook creates and manages complete Databricks workflows for climate risk insurance data processing:
# MAGIC
# MAGIC ## 🚀 **What This Creates:**
# MAGIC 1. **Delta Live Tables (DLT) Pipeline** - Unified data ingestion and processing
# MAGIC 2. **Model Training Workflow** - ML model training, validation, and registration
# MAGIC 3. **Model Deployment Workflow** - Model serving and batch inference
# MAGIC 4. **End-to-End Orchestration** - Complete data-to-insights pipeline
# MAGIC 5. **Monitoring & Alerting** - Pipeline health and performance tracking
# MAGIC
# MAGIC ## 🏗️ **Architecture:**
# MAGIC ```
# MAGIC External APIs → DLT Pipeline → Unity Catalog → ML Training → Model Serving
# MAGIC     ↓              ↓              ↓            ↓            ↓
# MAGIC AccuWeather    Staging Tables   Silver Tables  MLflow      Real-time API
# MAGIC NOAA Data      Data Quality     Gold Tables    Models      Batch Scoring
# MAGIC ```
# MAGIC
# MAGIC ## 📋 **Prerequisites:**
# MAGIC - Unity Catalog schema created (run `unity_catalog_schema_setup.py` first)
# MAGIC - Appropriate Databricks permissions for workflow creation
# MAGIC - Access to required compute resources

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
from databricks.sdk.service import pipelines, jobs, compute, ml
from databricks.sdk.service.pipelines import (
    CreatePipeline, PipelineLibrary, NotebookLibrary, PipelineCluster,
    AutoScale, PipelineSettings, PipelineEdition
)
from databricks.sdk.service.jobs import (
    CreateJob, JobSettings, NotebookTask, NewCluster, JobCluster,
    TaskDependency, Task, JobTaskSettings, JobRunAs, CronSchedule,
    JobEmailNotifications, WebhookNotifications, EmailNotifications
)
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, RuntimeEngine
import json
import time
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration Parameters

# COMMAND ----------

# Configuration parameters - modify as needed for your environment
catalog_name = "demo_hc"
environment = "development"  # development, staging, production
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

# Unified configuration dictionary
config = {
    # Core settings
    "catalog_name": catalog_name,
    "environment": environment,
    "resource_prefix": "climate_risk",
    "workspace_url": workspace_url,
    
    # Schema and storage paths
    "target_schema": "climate_risk",
    "raw_data_path": f"/Volumes/{catalog_name}/climate_risk/data_volume/raw_data/",
    "checkpoint_path": f"/Volumes/{catalog_name}/climate_risk/data_volume/pipeline_checkpoints/",
    "storage_location": f"/Volumes/{catalog_name}/climate_risk/data_volume/processed_data/",
    "model_artifacts_path": f"/Volumes/{catalog_name}/climate_risk/data_volume/model_artifacts/",
    "staging_path": f"/Volumes/{catalog_name}/climate_risk/data_volume/staging/",
    "analytics_path": f"/Volumes/{catalog_name}/climate_risk/data_volume/analytics/",
    
    # Workflow names
    "dlt_pipeline_name": f"climate_risk_unified_pipeline_{environment}",
    "training_job_name": f"climate_risk_model_training_{environment}",
    "deployment_job_name": f"climate_risk_model_deployment_{environment}",
    "orchestration_job_name": f"climate_risk_master_workflow_{environment}",
    
    # Notebook paths
    "dlt_notebooks": [
        "./lakeflow/01_accuweather_ingestion_pipeline",
        "./lakeflow/02_historical_data_processing_pipeline",
        "./lakeflow/climate_risk_workflow"
    ],
    "training_notebooks": [
        "./notebooks/01_drought_risk_model",
        "./notebooks/02_flood_risk_model"
    ],
    "deployment_notebooks": [
        "./notebooks/03_risk_visualization",
        "./notebooks/04_model_deployment"
    ],
    
    # Cluster configuration
    "cluster_config": {
        "spark_version": "17.0.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "driver_node_type_id": "Standard_DS3_v2",
        "autotermination_minutes": 60,
        "data_security_mode": DataSecurityMode.SINGLE_USER,
        "runtime_engine": RuntimeEngine.PHOTON
    },
    
    # Notification settings
    "notifications": {
        "on_start": [],
        "on_success": ["data-engineering@company.com"],
        "on_failure": ["data-engineering@company.com", "alerts@company.com"]
    }
}

print("🔧 Configuration Overview:")
print(f"  📊 Catalog: {config['catalog_name']}")
print(f"  🏷️  Environment: {config['environment']}")
print(f"  🏗️  Schema: {config['target_schema']}")
print(f"  🌐 Workspace: {config['workspace_url']}")
print(f"  📁 Storage: {config['storage_location']}")

# COMMAND ----------

# Initialize Databricks SDK client
w = WorkspaceClient()

# Verify connection
current_user = w.current_user.me()
print(f"✅ Connected as: {current_user.user_name}")
print(f"🏢 Workspace: {config['workspace_url']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delta Live Tables Pipeline Creation

# COMMAND ----------

def create_unified_dlt_pipeline():
    """
    Create a unified Delta Live Tables pipeline for all climate data ingestion and processing
    """
    pipeline_name = config["dlt_pipeline_name"]
    
    print(f"🚀 Creating DLT Pipeline: {pipeline_name}")
    
    # Define pipeline clusters with different configurations for different workloads
    clusters = [
        PipelineCluster(
            label="ingestion_cluster",
            autoscale=AutoScale(min_workers=1, max_workers=4, mode="ENHANCED"),
            node_type_id=config["cluster_config"]["node_type_id"],
            driver_node_type_id=config["cluster_config"]["driver_node_type_id"],
            spark_conf={
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.databricks.delta.optimizeWrite.enabled": "true",
                "spark.databricks.photon.enabled": "true",
                "spark.sql.execution.arrow.pyspark.enabled": "true"
            },
            custom_tags={
                "purpose": "data_ingestion",
                "environment": config["environment"],
                "cost_center": "data_platform"
            }
        ),
        PipelineCluster(
            label="processing_cluster", 
            autoscale=AutoScale(min_workers=2, max_workers=8, mode="ENHANCED"),
            node_type_id="Standard_DS4_v2",  # Larger nodes for processing
            driver_node_type_id="Standard_DS4_v2",
            spark_conf={
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.databricks.delta.optimizeWrite.enabled": "true",
                "spark.databricks.photon.enabled": "true",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
                "spark.databricks.io.cache.enabled": "true"
            },
            custom_tags={
                "purpose": "data_processing",
                "environment": config["environment"],
                "cost_center": "data_platform"
            }
        )
    ]
    
    # Define pipeline libraries (notebooks)
    libraries = []
    for notebook_path in config["dlt_notebooks"]:
        libraries.append(
            PipelineLibrary(
                notebook=NotebookLibrary(path=notebook_path)
            )
        )
    
    # Pipeline configuration
    pipeline_config = CreatePipeline(
        name=pipeline_name,
        storage=config["storage_location"],
        target=f"{config['catalog_name']}.{config['target_schema']}",
        libraries=libraries,
        clusters=clusters,
        configuration={
            # API Configuration
            "accuweather.api.key": "{{secrets/climate-risk/accuweather-api-key}}",
            "accuweather.base.url": "http://dataservice.accuweather.com",
            "noaa.api.base.url": "https://www.ncei.noaa.gov/data/",
            
            # Pipeline Configuration
            "pipeline.target_schema": config["target_schema"],
            "pipeline.catalog_name": config["catalog_name"],
            "pipeline.environment": config["environment"],
            "staging_table_prefix": "staging_",
            "processed_table_prefix": "",
            
            # Storage Paths (Unity Catalog volumes)
            "raw_data_path": config["raw_data_path"],
            "checkpoint_path": config["checkpoint_path"],
            "staging_path": config["staging_path"],
            "analytics_path": config["analytics_path"],
            
            # Data Quality Configuration
            "pipeline.data_quality.enable_expectations": "true",
            "pipeline.data_quality.quarantine_invalid_records": "true",
            "pipeline.monitoring.enable_alerts": "true",
            "pipeline.processing.batch_size": "10000",
            
            # Unity Catalog Configuration
            "unity_catalog.enabled": "true",
            "delta.enableChangeDataFeed": "true",
            "delta.enablePredictiveOptimization": "true"
        },
        edition=PipelineEdition.ADVANCED,
        photon=True,
        serverless=False,  # Use clusters for better control
        development=True if config["environment"] == "development" else False,
        continuous=False,  # Triggered mode for better resource management
        allow_duplicate_names=False,
        channel="CURRENT"
    )
    
    try:
        # Create the pipeline
        pipeline = w.pipelines.create(pipeline_config)
        pipeline_id = pipeline.pipeline_id
        
        print(f"✅ DLT Pipeline created successfully!")
        print(f"   📋 Pipeline ID: {pipeline_id}")
        print(f"   🔗 Pipeline URL: {config['workspace_url']}/#joblist/pipelines/{pipeline_id}")
        
        # Start the pipeline for initial setup
        print("🚀 Starting initial pipeline run...")
        w.pipelines.start_update(pipeline_id)
        
        return pipeline_id
        
    except Exception as e:
        print(f"❌ Error creating DLT pipeline: {str(e)}")
        return None

# Create the DLT pipeline
dlt_pipeline_id = create_unified_dlt_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Model Training Workflow Creation

# COMMAND ----------

def create_model_training_workflow():
    """
    Create a Databricks workflow for ML model training
    """
    job_name = config["training_job_name"]
    
    print(f"🤖 Creating Model Training Workflow: {job_name}")
    
    # Define tasks for the training workflow
    tasks = []
    
    # Task 1: Data Preparation and Feature Engineering
    tasks.append(
        Task(
            task_key="data_preparation",
            description="Prepare training data and engineer features",
            notebook_task=NotebookTask(
                notebook_path="./notebooks/01_drought_risk_model",
                base_parameters={
                    "catalog_name": config["catalog_name"],
                    "schema_name": config["target_schema"],
                    "environment": config["environment"],
                    "model_stage": "data_preparation"
                }
            ),
            job_cluster_key="training_cluster",
            timeout_seconds=3600,  # 1 hour timeout
            max_retries=2,
            min_retry_interval_millis=60000  # 1 minute
        )
    )
    
    # Task 2: Drought Risk Model Training
    tasks.append(
        Task(
            task_key="drought_model_training",
            description="Train drought risk prediction model",
            notebook_task=NotebookTask(
                notebook_path="./notebooks/01_drought_risk_model",
                base_parameters={
                    "catalog_name": config["catalog_name"],
                    "schema_name": config["target_schema"],
                    "environment": config["environment"],
                    "model_stage": "training"
                }
            ),
            depends_on=[TaskDependency(task_key="data_preparation")],
            job_cluster_key="training_cluster",
            timeout_seconds=7200,  # 2 hours timeout
            max_retries=2
        )
    )
    
    # Task 3: Flood Risk Model Training
    tasks.append(
        Task(
            task_key="flood_model_training",
            description="Train flood risk prediction model",
            notebook_task=NotebookTask(
                notebook_path="./notebooks/02_flood_risk_model",
                base_parameters={
                    "catalog_name": config["catalog_name"],
                    "schema_name": config["target_schema"],
                    "environment": config["environment"],
                    "model_stage": "training"
                }
            ),
            depends_on=[TaskDependency(task_key="data_preparation")],
            job_cluster_key="training_cluster",
            timeout_seconds=7200,  # 2 hours timeout
            max_retries=2
        )
    )
    
    # Task 4: Model Validation and Registration
    tasks.append(
        Task(
            task_key="model_validation",
            description="Validate models and register to MLflow",
            notebook_task=NotebookTask(
                notebook_path="./notebooks/03_risk_visualization",
                base_parameters={
                    "catalog_name": config["catalog_name"],
                    "schema_name": config["target_schema"],
                    "environment": config["environment"],
                    "model_stage": "validation"
                }
            ),
            depends_on=[
                TaskDependency(task_key="drought_model_training"),
                TaskDependency(task_key="flood_model_training")
            ],
            job_cluster_key="training_cluster",
            timeout_seconds=3600,  # 1 hour timeout
            max_retries=1
        )
    )
    
    # Define job clusters for training
    job_clusters = [
        JobCluster(
            job_cluster_key="training_cluster",
            new_cluster=NewCluster(
                cluster_name=f"climate-risk-training-{config['environment']}",
                spark_version=config["cluster_config"]["spark_version"],
                node_type_id="Standard_DS4_v2",  # Larger nodes for ML training
                driver_node_type_id="Standard_DS4_v2",
                num_workers=4,  # Fixed size for consistent training
                autotermination_minutes=config["cluster_config"]["autotermination_minutes"],
                data_security_mode=config["cluster_config"]["data_security_mode"],
                runtime_engine=config["cluster_config"]["runtime_engine"],
                spark_conf={
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.photon.enabled": "true",
                    "spark.ml.cache.enabled": "true",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                },
                custom_tags={
                    "purpose": "ml_training",
                    "environment": config["environment"],
                    "cost_center": "data_science"
                }
            )
        )
    ]
    
    # Job configuration
    job_config = CreateJob(
        name=job_name,
        tasks=tasks,
        job_clusters=job_clusters,
        email_notifications=JobEmailNotifications(
            on_start=config["notifications"]["on_start"],
            on_success=config["notifications"]["on_success"],
            on_failure=config["notifications"]["on_failure"],
            no_alert_for_skipped_runs=False
        ),
        webhook_notifications=WebhookNotifications(),
        timeout_seconds=14400,  # 4 hours total timeout
        max_concurrent_runs=1,
        format="MULTI_TASK",
        run_as=JobRunAs(service_principal_name=None),  # Run as creator
        tags={
            "environment": config["environment"],
            "purpose": "ml_training",
            "project": "climate_risk_insurance"
        }
    )
    
    try:
        # Create the job
        job = w.jobs.create(job_config)
        job_id = job.job_id
        
        print(f"✅ Model Training Workflow created successfully!")
        print(f"   📋 Job ID: {job_id}")
        print(f"   🔗 Job URL: {config['workspace_url']}/#job/{job_id}")
        
        return job_id
        
    except Exception as e:
        print(f"❌ Error creating model training workflow: {str(e)}")
        return None

# Create the model training workflow
training_job_id = create_model_training_workflow()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Model Deployment Workflow Creation

# COMMAND ----------

def create_model_deployment_workflow():
    """
    Create a Databricks workflow for model deployment and serving
    """
    job_name = config["deployment_job_name"]
    
    print(f"🚀 Creating Model Deployment Workflow: {job_name}")
    
    # Define tasks for the deployment workflow
    tasks = []
    
    # Task 1: Model Deployment and Serving Setup
    tasks.append(
        Task(
            task_key="model_deployment",
            description="Deploy models to serving endpoints",
            notebook_task=NotebookTask(
                notebook_path="./notebooks/04_model_deployment",
                base_parameters={
                    "catalog_name": config["catalog_name"],
                    "schema_name": config["target_schema"],
                    "environment": config["environment"],
                    "deployment_stage": "serving_setup"
                }
            ),
            job_cluster_key="deployment_cluster",
            timeout_seconds=3600,  # 1 hour timeout
            max_retries=2
        )
    )
    
    # Task 2: Batch Inference Pipeline
    tasks.append(
        Task(
            task_key="batch_inference",
            description="Run batch inference on climate data",
            notebook_task=NotebookTask(
                notebook_path="./notebooks/04_model_deployment",
                base_parameters={
                    "catalog_name": config["catalog_name"],
                    "schema_name": config["target_schema"],
                    "environment": config["environment"],
                    "deployment_stage": "batch_inference"
                }
            ),
            depends_on=[TaskDependency(task_key="model_deployment")],
            job_cluster_key="deployment_cluster",
            timeout_seconds=5400,  # 1.5 hours timeout
            max_retries=1
        )
    )
    
    # Task 3: Risk Visualization and Reporting
    tasks.append(
        Task(
            task_key="risk_visualization",
            description="Generate risk visualizations and reports",
            notebook_task=NotebookTask(
                notebook_path="./notebooks/03_risk_visualization",
                base_parameters={
                    "catalog_name": config["catalog_name"],
                    "schema_name": config["target_schema"],
                    "environment": config["environment"],
                    "visualization_stage": "reporting"
                }
            ),
            depends_on=[TaskDependency(task_key="batch_inference")],
            job_cluster_key="deployment_cluster",
            timeout_seconds=1800,  # 30 minutes timeout
            max_retries=1
        )
    )
    
    # Define job clusters for deployment
    job_clusters = [
        JobCluster(
            job_cluster_key="deployment_cluster",
            new_cluster=NewCluster(
                cluster_name=f"climate-risk-deployment-{config['environment']}",
                spark_version=config["cluster_config"]["spark_version"],
                node_type_id=config["cluster_config"]["node_type_id"],
                driver_node_type_id=config["cluster_config"]["driver_node_type_id"],
                autoscale=compute.AutoScale(min_workers=2, max_workers=6),
                autotermination_minutes=config["cluster_config"]["autotermination_minutes"],
                data_security_mode=config["cluster_config"]["data_security_mode"],
                runtime_engine=config["cluster_config"]["runtime_engine"],
                spark_conf={
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.photon.enabled": "true",
                    "spark.databricks.io.cache.enabled": "true"
                },
                custom_tags={
                    "purpose": "model_deployment",
                    "environment": config["environment"],
                    "cost_center": "data_science"
                }
            )
        )
    ]
    
    # Job configuration
    job_config = CreateJob(
        name=job_name,
        tasks=tasks,
        job_clusters=job_clusters,
        email_notifications=JobEmailNotifications(
            on_start=config["notifications"]["on_start"],
            on_success=config["notifications"]["on_success"],
            on_failure=config["notifications"]["on_failure"],
            no_alert_for_skipped_runs=False
        ),
        timeout_seconds=10800,  # 3 hours total timeout
        max_concurrent_runs=1,
        format="MULTI_TASK",
        tags={
            "environment": config["environment"],
            "purpose": "model_deployment",
            "project": "climate_risk_insurance"
        }
    )
    
    try:
        # Create the job
        job = w.jobs.create(job_config)
        job_id = job.job_id
        
        print(f"✅ Model Deployment Workflow created successfully!")
        print(f"   📋 Job ID: {job_id}")
        print(f"   🔗 Job URL: {config['workspace_url']}/#job/{job_id}")
        
        return job_id
        
    except Exception as e:
        print(f"❌ Error creating model deployment workflow: {str(e)}")
        return None

# Create the model deployment workflow
deployment_job_id = create_model_deployment_workflow()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Master Orchestration Workflow

# COMMAND ----------

def create_master_orchestration_workflow(dlt_pipeline_id, training_job_id, deployment_job_id):
    """
    Create a master workflow that orchestrates the entire climate risk pipeline
    """
    job_name = config["orchestration_job_name"]
    
    print(f"🎯 Creating Master Orchestration Workflow: {job_name}")
    
    # Define tasks for the master orchestration
    tasks = []
    
    # Task 1: Trigger DLT Pipeline
    if dlt_pipeline_id:
        tasks.append(
            Task(
                task_key="dlt_data_ingestion",
                description="Trigger DLT pipeline for data ingestion and processing",
                pipeline_task=pipelines.PipelineTask(
                    pipeline_id=dlt_pipeline_id,
                    full_refresh=False
                ),
                timeout_seconds=7200,  # 2 hours timeout
                max_retries=1
            )
        )
    
    # Task 2: Data Quality Validation
    tasks.append(
        Task(
            task_key="data_quality_validation",
            description="Validate data quality after ingestion",
            notebook_task=NotebookTask(
                notebook_path="./climate_risk_demo_walkthrough",
                base_parameters={
                    "catalog_name": config["catalog_name"],
                    "schema_name": config["target_schema"],
                    "environment": config["environment"],
                    "operation": "data_validation"
                }
            ),
            depends_on=[TaskDependency(task_key="dlt_data_ingestion")] if dlt_pipeline_id else [],
            job_cluster_key="orchestration_cluster",
            timeout_seconds=1800,  # 30 minutes timeout
            max_retries=1
        )
    )
    
    # Task 3: Trigger Model Training (conditionally)
    if training_job_id:
        tasks.append(
            Task(
                task_key="trigger_model_training",
                description="Trigger model training workflow",
                run_job_task=jobs.RunJobTask(
                    job_id=training_job_id,
                    job_parameters={}
                ),
                depends_on=[TaskDependency(task_key="data_quality_validation")],
                timeout_seconds=14400,  # 4 hours timeout
                max_retries=1
            )
        )
    
    # Task 4: Trigger Model Deployment (conditionally)
    if deployment_job_id:
        deployment_depends_on = []
        if training_job_id:
            deployment_depends_on.append(TaskDependency(task_key="trigger_model_training"))
        else:
            deployment_depends_on.append(TaskDependency(task_key="data_quality_validation"))
            
        tasks.append(
            Task(
                task_key="trigger_model_deployment",
                description="Trigger model deployment workflow",
                run_job_task=jobs.RunJobTask(
                    job_id=deployment_job_id,
                    job_parameters={}
                ),
                depends_on=deployment_depends_on,
                timeout_seconds=10800,  # 3 hours timeout
                max_retries=1
            )
        )
    
    # Task 5: Generate Summary Report
    tasks.append(
        Task(
            task_key="generate_summary_report",
            description="Generate end-to-end pipeline summary report",
            notebook_task=NotebookTask(
                notebook_path="./climate_risk_demo_walkthrough",
                base_parameters={
                    "catalog_name": config["catalog_name"],
                    "schema_name": config["target_schema"],
                    "environment": config["environment"],
                    "operation": "summary_report"
                }
            ),
            depends_on=[TaskDependency(task_key="trigger_model_deployment")] if deployment_job_id else [TaskDependency(task_key="data_quality_validation")],
            job_cluster_key="orchestration_cluster",
            timeout_seconds=1800,  # 30 minutes timeout
            max_retries=1
        )
    )
    
    # Define job clusters for orchestration
    job_clusters = [
        JobCluster(
            job_cluster_key="orchestration_cluster",
            new_cluster=NewCluster(
                cluster_name=f"climate-risk-orchestration-{config['environment']}",
                spark_version=config["cluster_config"]["spark_version"],
                node_type_id=config["cluster_config"]["node_type_id"],
                driver_node_type_id=config["cluster_config"]["driver_node_type_id"],
                autoscale=compute.AutoScale(min_workers=1, max_workers=3),
                autotermination_minutes=config["cluster_config"]["autotermination_minutes"],
                data_security_mode=config["cluster_config"]["data_security_mode"],
                runtime_engine=config["cluster_config"]["runtime_engine"],
                spark_conf={
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.photon.enabled": "true"
                },
                custom_tags={
                    "purpose": "orchestration",
                    "environment": config["environment"],
                    "cost_center": "data_platform"
                }
            )
        )
    ]
    
    # Job configuration with scheduling
    job_config = CreateJob(
        name=job_name,
        tasks=tasks,
        job_clusters=job_clusters,
        schedule=CronSchedule(
            quartz_cron_expression="0 0 6 * * ?",  # Daily at 6 AM UTC
            timezone_id="UTC",
            pause_status="UNPAUSED"
        ),
        email_notifications=JobEmailNotifications(
            on_start=config["notifications"]["on_start"],
            on_success=config["notifications"]["on_success"],
            on_failure=config["notifications"]["on_failure"],
            no_alert_for_skipped_runs=False
        ),
        timeout_seconds=28800,  # 8 hours total timeout
        max_concurrent_runs=1,
        format="MULTI_TASK",
        tags={
            "environment": config["environment"],
            "purpose": "orchestration",
            "project": "climate_risk_insurance",
            "schedule": "daily"
        }
    )
    
    try:
        # Create the job
        job = w.jobs.create(job_config)
        job_id = job.job_id
        
        print(f"✅ Master Orchestration Workflow created successfully!")
        print(f"   📋 Job ID: {job_id}")
        print(f"   🔗 Job URL: {config['workspace_url']}/#job/{job_id}")
        print(f"   ⏰ Schedule: Daily at 6:00 AM UTC")
        
        return job_id
        
    except Exception as e:
        print(f"❌ Error creating master orchestration workflow: {str(e)}")
        return None

# Create the master orchestration workflow
orchestration_job_id = create_master_orchestration_workflow(
    dlt_pipeline_id, 
    training_job_id, 
    deployment_job_id
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Workflow Management and Monitoring

# COMMAND ----------

def list_created_workflows():
    """
    List all created workflows and their status
    """
    print("📊 **Climate Risk Workflows Summary**")
    print("=" * 60)
    
    workflows = [
        ("DLT Pipeline", dlt_pipeline_id, "pipelines"),
        ("Model Training", training_job_id, "jobs"),
        ("Model Deployment", deployment_job_id, "jobs"),
        ("Master Orchestration", orchestration_job_id, "jobs")
    ]
    
    for name, resource_id, resource_type in workflows:
        if resource_id:
            if resource_type == "pipelines":
                url = f"{config['workspace_url']}/#joblist/pipelines/{resource_id}"
            else:
                url = f"{config['workspace_url']}/#job/{resource_id}"
            
            print(f"✅ **{name}**")
            print(f"   📋 ID: {resource_id}")
            print(f"   🔗 URL: {url}")
        else:
            print(f"❌ **{name}**: Failed to create")
        print()
    
    return workflows

def trigger_master_workflow():
    """
    Trigger the master orchestration workflow manually
    """
    if orchestration_job_id:
        try:
            print(f"🚀 Triggering master orchestration workflow...")
            run = w.jobs.run_now(job_id=orchestration_job_id)
            run_id = run.run_id
            
            print(f"✅ Workflow triggered successfully!")
            print(f"   📋 Run ID: {run_id}")
            print(f"   🔗 Run URL: {config['workspace_url']}/#job/{orchestration_job_id}/run/{run_id}")
            
            return run_id
        except Exception as e:
            print(f"❌ Error triggering workflow: {str(e)}")
            return None
    else:
        print("❌ No orchestration workflow available to trigger")
        return None

def delete_all_workflows():
    """
    Delete all created workflows (use with caution!)
    """
    print("🗑️  **WARNING: This will delete all created workflows!**")
    
    # Uncomment the following lines to enable deletion
    # confirmation = input("Type 'DELETE' to confirm: ")
    # if confirmation != "DELETE":
    #     print("❌ Deletion cancelled")
    #     return
    
    print("⚠️  Deletion function is commented out for safety")
    print("   Uncomment the deletion code in the notebook if needed")
    
    # Delete jobs
    # for job_id in [training_job_id, deployment_job_id, orchestration_job_id]:
    #     if job_id:
    #         try:
    #             w.jobs.delete(job_id)
    #             print(f"✅ Deleted job: {job_id}")
    #         except Exception as e:
    #             print(f"❌ Error deleting job {job_id}: {str(e)}")
    
    # Delete DLT pipeline
    # if dlt_pipeline_id:
    #     try:
    #         w.pipelines.delete(dlt_pipeline_id)
    #         print(f"✅ Deleted DLT pipeline: {dlt_pipeline_id}")
    #     except Exception as e:
    #         print(f"❌ Error deleting DLT pipeline {dlt_pipeline_id}: {str(e)}")

# List all created workflows
created_workflows = list_created_workflows()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Workflow Execution and Monitoring

# COMMAND ----------

# Display workflow execution options
print("🎛️  **Workflow Execution Options**")
print("=" * 50)
print()

print("**1. Manual Trigger:**")
print("   Run the following cell to trigger the master workflow manually")
print()

print("**2. Scheduled Execution:**") 
print("   The master workflow is scheduled to run daily at 6:00 AM UTC")
print("   You can modify the schedule in the Databricks Jobs UI")
print()

print("**3. Individual Workflow Triggers:**")
if dlt_pipeline_id:
    print(f"   DLT Pipeline: {config['workspace_url']}/#joblist/pipelines/{dlt_pipeline_id}")
if training_job_id:
    print(f"   Model Training: {config['workspace_url']}/#job/{training_job_id}")
if deployment_job_id:
    print(f"   Model Deployment: {config['workspace_url']}/#job/{deployment_job_id}")
print()

print("**4. Monitoring:**")
print("   • Check workflow status in Databricks Jobs UI")
print("   • Monitor email notifications for success/failure alerts")
print("   • Review logs and metrics for each workflow run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Quick Actions

# COMMAND ----------

# Uncomment and run any of these actions as needed:

# Trigger the master workflow manually
# trigger_run_id = trigger_master_workflow()

# Get detailed information about a specific workflow
# if orchestration_job_id:
#     job_info = w.jobs.get(orchestration_job_id)
#     print(f"Job Details: {job_info}")

# List recent runs of the master workflow
# if orchestration_job_id:
#     runs = w.jobs.list_runs(job_id=orchestration_job_id, limit=5)
#     print("Recent Workflow Runs:")
#     for run in runs:
#         print(f"  Run ID: {run.run_id}, State: {run.state}, Start Time: {run.start_time}")

print("✅ **Climate Risk Workflow Orchestrator Setup Complete!**")
print()
print("🎯 **Next Steps:**")
print("1. Review the created workflows in the Databricks Jobs UI")
print("2. Test the workflows manually before relying on scheduled execution")
print("3. Monitor the first few scheduled runs to ensure everything works correctly")
print("4. Customize notification settings and schedules as needed")
print("5. Set up additional monitoring and alerting if required")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 **Summary**
# MAGIC
# MAGIC ### ✅ **Created Resources:**
# MAGIC - **Unified DLT Pipeline** - All-in-one data ingestion and processing
# MAGIC - **Model Training Workflow** - Multi-task ML training pipeline  
# MAGIC - **Model Deployment Workflow** - Model serving and batch inference
# MAGIC - **Master Orchestration** - End-to-end workflow coordination
# MAGIC
# MAGIC ### 🏗️ **Architecture Benefits:**
# MAGIC - **Scalable**: Auto-scaling clusters based on workload
# MAGIC - **Reliable**: Built-in retries, timeouts, and error handling
# MAGIC - **Monitored**: Email notifications and comprehensive logging
# MAGIC - **Scheduled**: Automated daily execution with manual override capability
# MAGIC - **Modular**: Independent workflows that can run separately or together
# MAGIC
# MAGIC ### 🎯 **Production Ready:**
# MAGIC - Unity Catalog integration for data governance
# MAGIC - Proper resource tagging for cost management
# MAGIC - Security configurations and access controls
# MAGIC - Comprehensive error handling and alerting
# MAGIC - Optimized Spark configurations for performance
