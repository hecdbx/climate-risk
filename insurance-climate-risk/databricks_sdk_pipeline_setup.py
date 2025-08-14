# Databricks notebook source
# MAGIC %md
# MAGIC # Climate Risk Insurance - Databricks SDK Pipeline Setup
# MAGIC 
# MAGIC This notebook demonstrates how to programmatically create and manage data pipelines using the Databricks Python SDK.
# MAGIC 
# MAGIC ## What This Notebook Creates:
# MAGIC 1. **Delta Live Tables (DLT) Pipelines** - For real-time data ingestion and processing
# MAGIC 2. **Jobs for Training Workflows** - ML model training and deployment
# MAGIC 3. **Lakeflow Integration** - Complete data flow orchestration
# MAGIC 4. **Monitoring and Alerting** - Pipeline health and performance tracking
# MAGIC 
# MAGIC ## Prerequisites:
# MAGIC - Databricks workspace with appropriate permissions
# MAGIC - Unity Catalog enabled
# MAGIC - Access to create pipelines and jobs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

# MAGIC %python
# MAGIC # Install required packages
# MAGIC %pip install databricks-sdk databricks-cli --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %python
# MAGIC # Import required libraries
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service import pipelines, jobs, compute
# MAGIC from databricks.sdk.service.pipelines import (
# MAGIC     CreatePipeline, PipelineLibrary, NotebookLibrary, PipelineCluster,
# MAGIC     AutoScale, PipelineSettings, PipelineEdition
# MAGIC )
# MAGIC from databricks.sdk.service.jobs import (
# MAGIC     CreateJob, JobSettings, NotebookTask, NewCluster, JobCluster
# MAGIC )
# MAGIC import json
# MAGIC import time

# COMMAND ----------

# MAGIC %python
# MAGIC # Configuration parameters
# MAGIC config = {
# MAGIC     "catalog_name": "climate_risk_demo",
# MAGIC     "environment": "development",  # development, staging, production
# MAGIC     "resource_prefix": "climate_risk",
# MAGIC     "workspace_url": spark.conf.get("spark.databricks.workspaceUrl"),
# MAGIC     "pipeline_target_schema": "processed_data",
# MAGIC     "raw_data_path": "/mnt/climate-data/raw/",
# MAGIC     "checkpoint_path": "/mnt/climate-data/checkpoints/",
# MAGIC     "storage_location": "/mnt/climate-data/tables/"
# MAGIC }
# MAGIC 
# MAGIC print("Configuration:")
# MAGIC for key, value in config.items():
# MAGIC     print(f"  {key}: {value}")

# COMMAND ----------

# MAGIC %python
# MAGIC # Initialize Databricks SDK client
# MAGIC w = WorkspaceClient()
# MAGIC 
# MAGIC # Verify connection
# MAGIC current_user = w.current_user.me()
# MAGIC print(f"Connected as: {current_user.user_name}")
# MAGIC print(f"Workspace: {config['workspace_url']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delta Live Tables Pipeline Creation

# COMMAND ----------

# MAGIC %python
# MAGIC # Create AccuWeather Data Ingestion Pipeline
# MAGIC def create_accuweather_ingestion_pipeline():
# MAGIC     """Create DLT pipeline for AccuWeather data ingestion"""
# MAGIC     
# MAGIC     pipeline_name = f"{config['resource_prefix']}_accuweather_ingestion_{config['environment']}"
# MAGIC     
# MAGIC     # Pipeline configuration
# MAGIC     pipeline_config = CreatePipeline(
# MAGIC         name=pipeline_name,
# MAGIC         storage=config['storage_location'] + "accuweather/",
# MAGIC         target=f"{config['catalog_name']}.{config['pipeline_target_schema']}",
# MAGIC         libraries=[
# MAGIC             PipelineLibrary(
# MAGIC                 notebook=NotebookLibrary(
# MAGIC                     path="/Repos/climate-risk/lakeflow/01_accuweather_ingestion_pipeline"
# MAGIC                 )
# MAGIC             )
# MAGIC         ],
# MAGIC         clusters=[
# MAGIC             PipelineCluster(
# MAGIC                 label="default",
# MAGIC                 autoscale=AutoScale(min_workers=1, max_workers=5),
# MAGIC                 node_type_id="i3.xlarge",
# MAGIC                 driver_node_type_id="i3.xlarge",
# MAGIC                 spark_conf={
# MAGIC                     "spark.databricks.delta.preview.enabled": "true",
# MAGIC                     "spark.databricks.delta.retentionDurationCheck.enabled": "false"
# MAGIC                 },
# MAGIC                 custom_tags={
# MAGIC                     "project": "climate-risk",
# MAGIC                     "environment": config['environment'],
# MAGIC                     "pipeline_type": "ingestion"
# MAGIC                 }
# MAGIC             )
# MAGIC         ],
# MAGIC         configuration={
# MAGIC             "catalog_name": config['catalog_name'],
# MAGIC             "raw_data_path": config['raw_data_path'] + "accuweather/",
# MAGIC             "checkpoint_path": config['checkpoint_path'] + "accuweather/",
# MAGIC             "pipeline.trigger.interval": "5 minutes"
# MAGIC         },
# MAGIC         continuous=False,
# MAGIC         development=True if config['environment'] == 'development' else False,
# MAGIC         edition=PipelineEdition.ADVANCED,
# MAGIC         photon=True
# MAGIC     )
# MAGIC     
# MAGIC     try:
# MAGIC         pipeline = w.pipelines.create(pipeline_config)
# MAGIC         print(f"✅ Created AccuWeather ingestion pipeline: {pipeline.pipeline_id}")
# MAGIC         return pipeline.pipeline_id
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Failed to create AccuWeather pipeline: {str(e)}")
# MAGIC         return None

# COMMAND ----------

# MAGIC %python
# MAGIC # Create Historical Data Processing Pipeline
# MAGIC def create_historical_processing_pipeline():
# MAGIC     """Create DLT pipeline for historical climate data processing"""
# MAGIC     
# MAGIC     pipeline_name = f"{config['resource_prefix']}_historical_processing_{config['environment']}"
# MAGIC     
# MAGIC     pipeline_config = CreatePipeline(
# MAGIC         name=pipeline_name,
# MAGIC         storage=config['storage_location'] + "historical/",
# MAGIC         target=f"{config['catalog_name']}.{config['pipeline_target_schema']}",
# MAGIC         libraries=[
# MAGIC             PipelineLibrary(
# MAGIC                 notebook=NotebookLibrary(
# MAGIC                     path="/Repos/climate-risk/lakeflow/02_historical_data_processing_pipeline"
# MAGIC                 )
# MAGIC             )
# MAGIC         ],
# MAGIC         clusters=[
# MAGIC             PipelineCluster(
# MAGIC                 label="default",
# MAGIC                 autoscale=AutoScale(min_workers=2, max_workers=10),
# MAGIC                 node_type_id="i3.2xlarge",
# MAGIC                 driver_node_type_id="i3.2xlarge",
# MAGIC                 spark_conf={
# MAGIC                     "spark.databricks.delta.preview.enabled": "true",
# MAGIC                     "spark.databricks.delta.autoCompact.enabled": "true",
# MAGIC                     "spark.databricks.delta.optimizeWrite.enabled": "true"
# MAGIC                 },
# MAGIC                 custom_tags={
# MAGIC                     "project": "climate-risk",
# MAGIC                     "environment": config['environment'],
# MAGIC                     "pipeline_type": "processing"
# MAGIC                 }
# MAGIC             )
# MAGIC         ],
# MAGIC         configuration={
# MAGIC             "catalog_name": config['catalog_name'],
# MAGIC             "raw_data_path": config['raw_data_path'] + "historical/",
# MAGIC             "checkpoint_path": config['checkpoint_path'] + "historical/",
# MAGIC             "processing.batch_size": "1000000",
# MAGIC             "processing.parallelism": "auto"
# MAGIC         },
# MAGIC         continuous=False,
# MAGIC         development=True if config['environment'] == 'development' else False,
# MAGIC         edition=PipelineEdition.ADVANCED,
# MAGIC         photon=True
# MAGIC     )
# MAGIC     
# MAGIC     try:
# MAGIC         pipeline = w.pipelines.create(pipeline_config)
# MAGIC         print(f"✅ Created historical processing pipeline: {pipeline.pipeline_id}")
# MAGIC         return pipeline.pipeline_id
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Failed to create historical processing pipeline: {str(e)}")
# MAGIC         return None

# COMMAND ----------

# MAGIC %python
# MAGIC # Create Climate Risk Workflow Pipeline
# MAGIC def create_climate_risk_workflow():
# MAGIC     """Create comprehensive climate risk workflow pipeline"""
# MAGIC     
# MAGIC     pipeline_name = f"{config['resource_prefix']}_risk_workflow_{config['environment']}"
# MAGIC     
# MAGIC     pipeline_config = CreatePipeline(
# MAGIC         name=pipeline_name,
# MAGIC         storage=config['storage_location'] + "risk_models/",
# MAGIC         target=f"{config['catalog_name']}.risk_models",
# MAGIC         libraries=[
# MAGIC             PipelineLibrary(
# MAGIC                 notebook=NotebookLibrary(
# MAGIC                     path="/Repos/climate-risk/lakeflow/climate_risk_workflow"
# MAGIC                 )
# MAGIC             )
# MAGIC         ],
# MAGIC         clusters=[
# MAGIC             PipelineCluster(
# MAGIC                 label="default",
# MAGIC                 autoscale=AutoScale(min_workers=3, max_workers=15),
# MAGIC                 node_type_id="i3.4xlarge",
# MAGIC                 driver_node_type_id="i3.4xlarge",
# MAGIC                 spark_conf={
# MAGIC                     "spark.databricks.delta.preview.enabled": "true",
# MAGIC                     "spark.sql.adaptive.enabled": "true",
# MAGIC                     "spark.sql.adaptive.coalescePartitions.enabled": "true"
# MAGIC                 },
# MAGIC                 custom_tags={
# MAGIC                     "project": "climate-risk",
# MAGIC                     "environment": config['environment'],
# MAGIC                     "pipeline_type": "risk_modeling"
# MAGIC                 }
# MAGIC             )
# MAGIC         ],
# MAGIC         configuration={
# MAGIC             "catalog_name": config['catalog_name'],
# MAGIC             "risk_models_path": config['storage_location'] + "risk_models/",
# MAGIC             "checkpoint_path": config['checkpoint_path'] + "risk_models/",
# MAGIC             "model.drought_threshold": "0.3",
# MAGIC             "model.flood_threshold": "0.4",
# MAGIC             "model.confidence_threshold": "0.7"
# MAGIC         },
# MAGIC         continuous=False,
# MAGIC         development=True if config['environment'] == 'development' else False,
# MAGIC         edition=PipelineEdition.ADVANCED,
# MAGIC         photon=True
# MAGIC     )
# MAGIC     
# MAGIC     try:
# MAGIC         pipeline = w.pipelines.create(pipeline_config)
# MAGIC         print(f"✅ Created climate risk workflow pipeline: {pipeline.pipeline_id}")
# MAGIC         return pipeline.pipeline_id
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Failed to create climate risk workflow: {str(e)}")
# MAGIC         return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ML Training Job Creation

# COMMAND ----------

# MAGIC %python
# MAGIC # Create Drought Risk Model Training Job
# MAGIC def create_drought_model_training_job():
# MAGIC     """Create job for drought risk model training"""
# MAGIC     
# MAGIC     job_name = f"{config['resource_prefix']}_drought_training_{config['environment']}"
# MAGIC     
# MAGIC     job_config = CreateJob(
# MAGIC         name=job_name,
# MAGIC         job_clusters=[
# MAGIC             JobCluster(
# MAGIC                 job_cluster_key="drought_training_cluster",
# MAGIC                 new_cluster=NewCluster(
# MAGIC                     spark_version="13.3.x-cpu-ml-scala2.12",
# MAGIC                     node_type_id="i3.2xlarge",
# MAGIC                     driver_node_type_id="i3.2xlarge",
# MAGIC                     autoscale=AutoScale(min_workers=2, max_workers=8),
# MAGIC                     spark_conf={
# MAGIC                         "spark.databricks.delta.preview.enabled": "true",
# MAGIC                         "spark.databricks.ml.enabled": "true"
# MAGIC                     },
# MAGIC                     custom_tags={
# MAGIC                         "project": "climate-risk",
# MAGIC                         "environment": config['environment'],
# MAGIC                         "job_type": "ml_training"
# MAGIC                     }
# MAGIC                 )
# MAGIC             )
# MAGIC         ],
# MAGIC         tasks=[
# MAGIC             jobs.Task(
# MAGIC                 task_key="drought_model_training",
# MAGIC                 job_cluster_key="drought_training_cluster",
# MAGIC                 notebook_task=NotebookTask(
# MAGIC                     notebook_path="/Repos/climate-risk/notebooks/01_drought_risk_model",
# MAGIC                     base_parameters={
# MAGIC                         "catalog_name": config['catalog_name'],
# MAGIC                         "environment": config['environment'],
# MAGIC                         "model_name": "drought_risk_model",
# MAGIC                         "training_data_path": f"{config['catalog_name']}.processed_data.climate_aggregations"
# MAGIC                     }
# MAGIC                 ),
# MAGIC                 timeout_seconds=7200,  # 2 hours
# MAGIC                 max_retries=2
# MAGIC             ),
# MAGIC             jobs.Task(
# MAGIC                 task_key="model_evaluation",
# MAGIC                 job_cluster_key="drought_training_cluster",
# MAGIC                 depends_on=[jobs.TaskDependency(task_key="drought_model_training")],
# MAGIC                 notebook_task=NotebookTask(
# MAGIC                     notebook_path="/Repos/climate-risk/notebooks/model_evaluation",
# MAGIC                     base_parameters={
# MAGIC                         "catalog_name": config['catalog_name'],
# MAGIC                         "model_type": "drought",
# MAGIC                         "environment": config['environment']
# MAGIC                     }
# MAGIC                 ),
# MAGIC                 timeout_seconds=1800,  # 30 minutes
# MAGIC                 max_retries=1
# MAGIC             )
# MAGIC         ],
# MAGIC         schedule=jobs.CronSchedule(
# MAGIC             quartz_cron_expression="0 0 2 * * ?",  # Daily at 2 AM
# MAGIC             timezone_id="UTC"
# MAGIC         ),
# MAGIC         max_concurrent_runs=1,
# MAGIC         tags={
# MAGIC             "project": "climate-risk",
# MAGIC             "model_type": "drought",
# MAGIC             "environment": config['environment']
# MAGIC         }
# MAGIC     )
# MAGIC     
# MAGIC     try:
# MAGIC         job = w.jobs.create(job_config)
# MAGIC         print(f"✅ Created drought model training job: {job.job_id}")
# MAGIC         return job.job_id
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Failed to create drought training job: {str(e)}")
# MAGIC         return None

# COMMAND ----------

# MAGIC %python
# MAGIC # Create Flood Risk Model Training Job
# MAGIC def create_flood_model_training_job():
# MAGIC     """Create job for flood risk model training"""
# MAGIC     
# MAGIC     job_name = f"{config['resource_prefix']}_flood_training_{config['environment']}"
# MAGIC     
# MAGIC     job_config = CreateJob(
# MAGIC         name=job_name,
# MAGIC         job_clusters=[
# MAGIC             JobCluster(
# MAGIC                 job_cluster_key="flood_training_cluster",
# MAGIC                 new_cluster=NewCluster(
# MAGIC                     spark_version="13.3.x-cpu-ml-scala2.12",
# MAGIC                     node_type_id="i3.2xlarge",
# MAGIC                     driver_node_type_id="i3.2xlarge",
# MAGIC                     autoscale=AutoScale(min_workers=2, max_workers=8),
# MAGIC                     spark_conf={
# MAGIC                         "spark.databricks.delta.preview.enabled": "true",
# MAGIC                         "spark.databricks.ml.enabled": "true"
# MAGIC                     },
# MAGIC                     custom_tags={
# MAGIC                         "project": "climate-risk",
# MAGIC                         "environment": config['environment'],
# MAGIC                         "job_type": "ml_training"
# MAGIC                     }
# MAGIC                 )
# MAGIC             )
# MAGIC         ],
# MAGIC         tasks=[
# MAGIC             jobs.Task(
# MAGIC                 task_key="flood_model_training",
# MAGIC                 job_cluster_key="flood_training_cluster",
# MAGIC                 notebook_task=NotebookTask(
# MAGIC                     notebook_path="/Repos/climate-risk/notebooks/02_flood_risk_model",
# MAGIC                     base_parameters={
# MAGIC                         "catalog_name": config['catalog_name'],
# MAGIC                         "environment": config['environment'],
# MAGIC                         "model_name": "flood_risk_model",
# MAGIC                         "training_data_path": f"{config['catalog_name']}.processed_data.climate_aggregations"
# MAGIC                     }
# MAGIC                 ),
# MAGIC                 timeout_seconds=7200,  # 2 hours
# MAGIC                 max_retries=2
# MAGIC             ),
# MAGIC             jobs.Task(
# MAGIC                 task_key="model_evaluation",
# MAGIC                 job_cluster_key="flood_training_cluster",
# MAGIC                 depends_on=[jobs.TaskDependency(task_key="flood_model_training")],
# MAGIC                 notebook_task=NotebookTask(
# MAGIC                     notebook_path="/Repos/climate-risk/notebooks/model_evaluation",
# MAGIC                     base_parameters={
# MAGIC                         "catalog_name": config['catalog_name'],
# MAGIC                         "model_type": "flood",
# MAGIC                         "environment": config['environment']
# MAGIC                     }
# MAGIC                 ),
# MAGIC                 timeout_seconds=1800,  # 30 minutes
# MAGIC                 max_retries=1
# MAGIC             )
# MAGIC         ],
# MAGIC         schedule=jobs.CronSchedule(
# MAGIC             quartz_cron_expression="0 0 3 * * ?",  # Daily at 3 AM
# MAGIC             timezone_id="UTC"
# MAGIC         ),
# MAGIC         max_concurrent_runs=1,
# MAGIC         tags={
# MAGIC             "project": "climate-risk",
# MAGIC             "model_type": "flood",
# MAGIC             "environment": config['environment']
# MAGIC         }
# MAGIC     )
# MAGIC     
# MAGIC     try:
# MAGIC         job = w.jobs.create(job_config)
# MAGIC         print(f"✅ Created flood model training job: {job.job_id}")
# MAGIC         return job.job_id
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Failed to create flood training job: {str(e)}")
# MAGIC         return None

# COMMAND ----------

# MAGIC %python
# MAGIC # Create Model Deployment Job
# MAGIC def create_model_deployment_job():
# MAGIC     """Create job for model deployment and serving setup"""
# MAGIC     
# MAGIC     job_name = f"{config['resource_prefix']}_model_deployment_{config['environment']}"
# MAGIC     
# MAGIC     job_config = CreateJob(
# MAGIC         name=job_name,
# MAGIC         job_clusters=[
# MAGIC             JobCluster(
# MAGIC                 job_cluster_key="deployment_cluster",
# MAGIC                 new_cluster=NewCluster(
# MAGIC                     spark_version="13.3.x-cpu-ml-scala2.12",
# MAGIC                     node_type_id="i3.xlarge",
# MAGIC                     driver_node_type_id="i3.xlarge",
# MAGIC                     num_workers=1,
# MAGIC                     spark_conf={
# MAGIC                         "spark.databricks.delta.preview.enabled": "true"
# MAGIC                     },
# MAGIC                     custom_tags={
# MAGIC                         "project": "climate-risk",
# MAGIC                         "environment": config['environment'],
# MAGIC                         "job_type": "deployment"
# MAGIC                     }
# MAGIC                 )
# MAGIC             )
# MAGIC         ],
# MAGIC         tasks=[
# MAGIC             jobs.Task(
# MAGIC                 task_key="deploy_models",
# MAGIC                 job_cluster_key="deployment_cluster",
# MAGIC                 notebook_task=NotebookTask(
# MAGIC                     notebook_path="/Repos/climate-risk/notebooks/04_model_deployment",
# MAGIC                     base_parameters={
# MAGIC                         "catalog_name": config['catalog_name'],
# MAGIC                         "environment": config['environment'],
# MAGIC                         "deployment_stage": "staging" if config['environment'] == 'development' else "production"
# MAGIC                     }
# MAGIC                 ),
# MAGIC                 timeout_seconds=3600,  # 1 hour
# MAGIC                 max_retries=1
# MAGIC             )
# MAGIC         ],
# MAGIC         # Manual trigger for deployment
# MAGIC         max_concurrent_runs=1,
# MAGIC         tags={
# MAGIC             "project": "climate-risk",
# MAGIC             "job_type": "deployment",
# MAGIC             "environment": config['environment']
# MAGIC         }
# MAGIC     )
# MAGIC     
# MAGIC     try:
# MAGIC         job = w.jobs.create(job_config)
# MAGIC         print(f"✅ Created model deployment job: {job.job_id}")
# MAGIC         return job.job_id
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Failed to create deployment job: {str(e)}")
# MAGIC         return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Complete Lakeflow Orchestration

# COMMAND ----------

# MAGIC %python
# MAGIC # Create Master Orchestration Job
# MAGIC def create_master_orchestration_job(pipeline_ids, training_job_ids):
# MAGIC     """Create master job that orchestrates the entire pipeline"""
# MAGIC     
# MAGIC     job_name = f"{config['resource_prefix']}_master_orchestration_{config['environment']}"
# MAGIC     
# MAGIC     # Create tasks for pipeline execution
# MAGIC     tasks = []
# MAGIC     
# MAGIC     # Add pipeline tasks
# MAGIC     if pipeline_ids.get('accuweather'):
# MAGIC         tasks.append(
# MAGIC             jobs.Task(
# MAGIC                 task_key="run_accuweather_ingestion",
# MAGIC                 pipeline_task=jobs.PipelineTask(
# MAGIC                     pipeline_id=pipeline_ids['accuweather']
# MAGIC                 ),
# MAGIC                 timeout_seconds=3600,
# MAGIC                 max_retries=2
# MAGIC             )
# MAGIC         )
# MAGIC     
# MAGIC     if pipeline_ids.get('historical'):
# MAGIC         tasks.append(
# MAGIC             jobs.Task(
# MAGIC                 task_key="run_historical_processing",
# MAGIC                 pipeline_task=jobs.PipelineTask(
# MAGIC                     pipeline_id=pipeline_ids['historical']
# MAGIC                 ),
# MAGIC                 depends_on=[jobs.TaskDependency(task_key="run_accuweather_ingestion")] if pipeline_ids.get('accuweather') else [],
# MAGIC                 timeout_seconds=7200,
# MAGIC                 max_retries=2
# MAGIC             )
# MAGIC         )
# MAGIC     
# MAGIC     if pipeline_ids.get('risk_workflow'):
# MAGIC         tasks.append(
# MAGIC             jobs.Task(
# MAGIC                 task_key="run_risk_workflow",
# MAGIC                 pipeline_task=jobs.PipelineTask(
# MAGIC                     pipeline_id=pipeline_ids['risk_workflow']
# MAGIC                 ),
# MAGIC                 depends_on=[
# MAGIC                     jobs.TaskDependency(task_key="run_historical_processing")
# MAGIC                 ] if pipeline_ids.get('historical') else [],
# MAGIC                 timeout_seconds=5400,
# MAGIC                 max_retries=2
# MAGIC             )
# MAGIC         )
# MAGIC     
# MAGIC     # Add training job triggers
# MAGIC     if training_job_ids.get('drought'):
# MAGIC         tasks.append(
# MAGIC             jobs.Task(
# MAGIC                 task_key="trigger_drought_training",
# MAGIC                 run_job_task=jobs.RunJobTask(
# MAGIC                     job_id=training_job_ids['drought']
# MAGIC                 ),
# MAGIC                 depends_on=[
# MAGIC                     jobs.TaskDependency(task_key="run_risk_workflow")
# MAGIC                 ] if pipeline_ids.get('risk_workflow') else [],
# MAGIC                 timeout_seconds=10800,  # 3 hours
# MAGIC                 max_retries=1
# MAGIC             )
# MAGIC         )
# MAGIC     
# MAGIC     if training_job_ids.get('flood'):
# MAGIC         tasks.append(
# MAGIC             jobs.Task(
# MAGIC                 task_key="trigger_flood_training",
# MAGIC                 run_job_task=jobs.RunJobTask(
# MAGIC                     job_id=training_job_ids['flood']
# MAGIC                 ),
# MAGIC                 depends_on=[
# MAGIC                     jobs.TaskDependency(task_key="trigger_drought_training")
# MAGIC                 ] if training_job_ids.get('drought') else [],
# MAGIC                 timeout_seconds=10800,  # 3 hours
# MAGIC                 max_retries=1
# MAGIC             )
# MAGIC         )
# MAGIC     
# MAGIC     job_config = CreateJob(
# MAGIC         name=job_name,
# MAGIC         tasks=tasks,
# MAGIC         schedule=jobs.CronSchedule(
# MAGIC             quartz_cron_expression="0 0 1 * * ?",  # Daily at 1 AM
# MAGIC             timezone_id="UTC"
# MAGIC         ),
# MAGIC         max_concurrent_runs=1,
# MAGIC         tags={
# MAGIC             "project": "climate-risk",
# MAGIC             "job_type": "orchestration",
# MAGIC             "environment": config['environment']
# MAGIC         },
# MAGIC         email_notifications=jobs.JobEmailNotifications(
# MAGIC             on_failure=["data-engineering@company.com"],
# MAGIC             on_success=["data-engineering@company.com"]
# MAGIC         )
# MAGIC     )
# MAGIC     
# MAGIC     try:
# MAGIC         job = w.jobs.create(job_config)
# MAGIC         print(f"✅ Created master orchestration job: {job.job_id}")
# MAGIC         return job.job_id
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Failed to create orchestration job: {str(e)}")
# MAGIC         return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execute Pipeline Creation

# COMMAND ----------

# MAGIC %python
# MAGIC # Execute all pipeline and job creation
# MAGIC def create_all_pipelines_and_jobs():
# MAGIC     """Create all pipelines and jobs for the climate risk system"""
# MAGIC     
# MAGIC     print("🚀 Starting pipeline and job creation...")
# MAGIC     print("=" * 60)
# MAGIC     
# MAGIC     # Track created resources
# MAGIC     pipeline_ids = {}
# MAGIC     job_ids = {}
# MAGIC     
# MAGIC     # Create DLT Pipelines
# MAGIC     print("\n📊 Creating Delta Live Tables Pipelines...")
# MAGIC     pipeline_ids['accuweather'] = create_accuweather_ingestion_pipeline()
# MAGIC     pipeline_ids['historical'] = create_historical_processing_pipeline()
# MAGIC     pipeline_ids['risk_workflow'] = create_climate_risk_workflow()
# MAGIC     
# MAGIC     # Create Training Jobs
# MAGIC     print("\n🤖 Creating ML Training Jobs...")
# MAGIC     job_ids['drought'] = create_drought_model_training_job()
# MAGIC     job_ids['flood'] = create_flood_model_training_job()
# MAGIC     job_ids['deployment'] = create_model_deployment_job()
# MAGIC     
# MAGIC     # Create Master Orchestration
# MAGIC     print("\n🎯 Creating Master Orchestration...")
# MAGIC     job_ids['orchestration'] = create_master_orchestration_job(pipeline_ids, job_ids)
# MAGIC     
# MAGIC     # Summary
# MAGIC     print("\n" + "=" * 60)
# MAGIC     print("📋 CREATION SUMMARY")
# MAGIC     print("=" * 60)
# MAGIC     
# MAGIC     print("\n🔄 Delta Live Tables Pipelines:")
# MAGIC     for name, pid in pipeline_ids.items():
# MAGIC         status = "✅ Created" if pid else "❌ Failed"
# MAGIC         print(f"  {name}: {status} ({pid})")
# MAGIC     
# MAGIC     print("\n⚙️ Training and Deployment Jobs:")
# MAGIC     for name, jid in job_ids.items():
# MAGIC         status = "✅ Created" if jid else "❌ Failed"
# MAGIC         print(f"  {name}: {status} ({jid})")
# MAGIC     
# MAGIC     return pipeline_ids, job_ids

# COMMAND ----------

# MAGIC %python
# MAGIC # Execute the creation
# MAGIC pipeline_ids, job_ids = create_all_pipelines_and_jobs()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pipeline Management and Monitoring

# COMMAND ----------

# MAGIC %python
# MAGIC # Pipeline monitoring functions
# MAGIC def check_pipeline_status(pipeline_id):
# MAGIC     """Check the status of a specific pipeline"""
# MAGIC     try:
# MAGIC         pipeline = w.pipelines.get(pipeline_id)
# MAGIC         print(f"Pipeline {pipeline_id}:")
# MAGIC         print(f"  Name: {pipeline.name}")
# MAGIC         print(f"  State: {pipeline.state}")
# MAGIC         print(f"  Health: {pipeline.health}")
# MAGIC         return pipeline.state
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Error checking pipeline {pipeline_id}: {str(e)}")
# MAGIC         return None

# COMMAND ----------

# MAGIC %python
# MAGIC # Job monitoring functions
# MAGIC def check_job_status(job_id):
# MAGIC     """Check the status of a specific job"""
# MAGIC     try:
# MAGIC         job = w.jobs.get(job_id)
# MAGIC         print(f"Job {job_id}:")
# MAGIC         print(f"  Name: {job.settings.name}")
# MAGIC         
# MAGIC         # Get recent runs
# MAGIC         runs = w.jobs.list_runs(job_id=job_id, limit=5)
# MAGIC         if runs:
# MAGIC             print(f"  Recent runs:")
# MAGIC             for run in runs:
# MAGIC                 print(f"    Run {run.run_id}: {run.state.life_cycle_state}")
# MAGIC         else:
# MAGIC             print(f"  No recent runs")
# MAGIC         
# MAGIC         return job.settings
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Error checking job {job_id}: {str(e)}")
# MAGIC         return None

# COMMAND ----------

# MAGIC %python
# MAGIC # Start a pipeline manually
# MAGIC def start_pipeline(pipeline_id, full_refresh=False):
# MAGIC     """Start a pipeline manually"""
# MAGIC     try:
# MAGIC         update = w.pipelines.start_update(
# MAGIC             pipeline_id=pipeline_id,
# MAGIC             full_refresh=full_refresh
# MAGIC         )
# MAGIC         print(f"✅ Started pipeline {pipeline_id}, update ID: {update.update_id}")
# MAGIC         return update.update_id
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Failed to start pipeline {pipeline_id}: {str(e)}")
# MAGIC         return None

# COMMAND ----------

# MAGIC %python
# MAGIC # Run a job manually
# MAGIC def run_job(job_id, parameters=None):
# MAGIC     """Run a job manually"""
# MAGIC     try:
# MAGIC         run = w.jobs.run_now(
# MAGIC             job_id=job_id,
# MAGIC             notebook_params=parameters or {}
# MAGIC         )
# MAGIC         print(f"✅ Started job {job_id}, run ID: {run.run_id}")
# MAGIC         return run.run_id
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Failed to run job {job_id}: {str(e)}")
# MAGIC         return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resource Management

# COMMAND ----------

# MAGIC %python
# MAGIC # List all created resources
# MAGIC def list_climate_risk_resources():
# MAGIC     """List all climate risk related resources"""
# MAGIC     
# MAGIC     print("🔍 Climate Risk Resources:")
# MAGIC     print("=" * 50)
# MAGIC     
# MAGIC     # List pipelines
# MAGIC     print("\n📊 Delta Live Tables Pipelines:")
# MAGIC     try:
# MAGIC         pipelines = w.pipelines.list_pipelines()
# MAGIC         climate_pipelines = [p for p in pipelines if config['resource_prefix'] in p.name]
# MAGIC         for pipeline in climate_pipelines:
# MAGIC             print(f"  {pipeline.name} ({pipeline.pipeline_id})")
# MAGIC     except Exception as e:
# MAGIC         print(f"  ❌ Error listing pipelines: {str(e)}")
# MAGIC     
# MAGIC     # List jobs
# MAGIC     print("\n⚙️ Jobs:")
# MAGIC     try:
# MAGIC         jobs_list = w.jobs.list()
# MAGIC         climate_jobs = [j for j in jobs_list if config['resource_prefix'] in j.settings.name]
# MAGIC         for job in climate_jobs:
# MAGIC             print(f"  {job.settings.name} ({job.job_id})")
# MAGIC     except Exception as e:
# MAGIC         print(f"  ❌ Error listing jobs: {str(e)}")

# COMMAND ----------

# MAGIC %python
# MAGIC # Clean up resources (use with caution)
# MAGIC def cleanup_climate_risk_resources(confirm=False):
# MAGIC     """Delete all climate risk related resources"""
# MAGIC     
# MAGIC     if not confirm:
# MAGIC         print("⚠️  This will delete ALL climate risk resources!")
# MAGIC         print("Set confirm=True to proceed")
# MAGIC         return
# MAGIC     
# MAGIC     print("🗑️  Cleaning up climate risk resources...")
# MAGIC     
# MAGIC     # Delete pipelines
# MAGIC     try:
# MAGIC         pipelines = w.pipelines.list_pipelines()
# MAGIC         climate_pipelines = [p for p in pipelines if config['resource_prefix'] in p.name]
# MAGIC         for pipeline in climate_pipelines:
# MAGIC             w.pipelines.delete(pipeline.pipeline_id)
# MAGIC             print(f"  ✅ Deleted pipeline: {pipeline.name}")
# MAGIC     except Exception as e:
# MAGIC         print(f"  ❌ Error deleting pipelines: {str(e)}")
# MAGIC     
# MAGIC     # Delete jobs
# MAGIC     try:
# MAGIC         jobs_list = w.jobs.list()
# MAGIC         climate_jobs = [j for j in jobs_list if config['resource_prefix'] in j.settings.name]
# MAGIC         for job in climate_jobs:
# MAGIC             w.jobs.delete(job.job_id)
# MAGIC             print(f"  ✅ Deleted job: {job.settings.name}")
# MAGIC     except Exception as e:
# MAGIC         print(f"  ❌ Error deleting jobs: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validation and Testing

# COMMAND ----------

# MAGIC %python
# MAGIC # Validate pipeline configuration
# MAGIC def validate_pipeline_setup():
# MAGIC     """Validate that all pipelines and jobs are properly configured"""
# MAGIC     
# MAGIC     print("🔍 Validating pipeline setup...")
# MAGIC     print("=" * 50)
# MAGIC     
# MAGIC     validation_results = {
# MAGIC         'pipelines': {},
# MAGIC         'jobs': {},
# MAGIC         'overall': True
# MAGIC     }
# MAGIC     
# MAGIC     # Check pipelines
# MAGIC     for name, pipeline_id in pipeline_ids.items():
# MAGIC         if pipeline_id:
# MAGIC             try:
# MAGIC                 pipeline = w.pipelines.get(pipeline_id)
# MAGIC                 validation_results['pipelines'][name] = {
# MAGIC                     'status': 'valid',
# MAGIC                     'id': pipeline_id,
# MAGIC                     'name': pipeline.name
# MAGIC                 }
# MAGIC                 print(f"  ✅ Pipeline {name}: Valid")
# MAGIC             except Exception as e:
# MAGIC                 validation_results['pipelines'][name] = {
# MAGIC                     'status': 'error',
# MAGIC                     'error': str(e)
# MAGIC                 }
# MAGIC                 validation_results['overall'] = False
# MAGIC                 print(f"  ❌ Pipeline {name}: Error - {str(e)}")
# MAGIC         else:
# MAGIC             validation_results['pipelines'][name] = {'status': 'not_created'}
# MAGIC             validation_results['overall'] = False
# MAGIC             print(f"  ❌ Pipeline {name}: Not created")
# MAGIC     
# MAGIC     # Check jobs
# MAGIC     for name, job_id in job_ids.items():
# MAGIC         if job_id:
# MAGIC             try:
# MAGIC                 job = w.jobs.get(job_id)
# MAGIC                 validation_results['jobs'][name] = {
# MAGIC                     'status': 'valid',
# MAGIC                     'id': job_id,
# MAGIC                     'name': job.settings.name
# MAGIC                 }
# MAGIC                 print(f"  ✅ Job {name}: Valid")
# MAGIC             except Exception as e:
# MAGIC                 validation_results['jobs'][name] = {
# MAGIC                     'status': 'error',
# MAGIC                     'error': str(e)
# MAGIC                 }
# MAGIC                 validation_results['overall'] = False
# MAGIC                 print(f"  ❌ Job {name}: Error - {str(e)}")
# MAGIC         else:
# MAGIC             validation_results['jobs'][name] = {'status': 'not_created'}
# MAGIC             validation_results['overall'] = False
# MAGIC             print(f"  ❌ Job {name}: Not created")
# MAGIC     
# MAGIC     print(f"\n🎯 Overall validation: {'✅ PASSED' if validation_results['overall'] else '❌ FAILED'}")
# MAGIC     return validation_results

# COMMAND ----------

# MAGIC %python
# MAGIC # Run validation
# MAGIC validation_results = validate_pipeline_setup()

# COMMAND ----------

# MAGIC %python
# MAGIC # Display current resources
# MAGIC list_climate_risk_resources()

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
