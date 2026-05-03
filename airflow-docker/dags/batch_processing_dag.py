

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from pipelines.batch.data_reader import DataReader
from pipelines.batch.validation_rules import ValidationRules
from pipelines.batch.backup_validator import BackupValidator
from pipelines.batch.data_processor import DataProcessor
from pipelines.batch.data_writer import DataWriter
from pipelines.batch.azure_writer import AzureWriter
logger = logging.getLogger(__name__)
DEFAULT_ARGS = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 28),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'yellow_taxi_batch_processing',
    default_args=DEFAULT_ARGS,
    description='Yellow Taxi Data Engineering Pipeline - Batch Processing',
    schedule='@monthly',  # Monthly schedule (adjust as needed)
    catchup=False,
    tags=['data-engineering', 'batch', 'yellow-taxi', 'azure'],
)


DATA_FILE = "/opt/airflow/data/yellow_tripdata_2025-01.parquet"
OUTPUT_DIR = str(PROJECT_ROOT / 'output')
AZURE_CONNECTION_STRING = os.getenv(
    'AZURE_STORAGE_CONNECTION_STRING',
    Variable.get('azure_storage_connection_string', default_var=None)
)
AZURE_CONTAINER = 'processed-data'
def read_data(**context) -> Dict[str, Any]:
    
    execution_start = datetime.now()
    logger.info("=" * 80)
    logger.info(f"[{execution_start.isoformat()}] PHASE 1: DATA READING")
    logger.info("=" * 80)
    
    try:
        reader = DataReader()
        df = reader.read_parquet(DATA_FILE)
        
        logger.info(f"  Data Profile:")
        logger.info(f"  Total Rows:    {df.shape[0]:,}")
        logger.info(f"  Total Columns: {df.shape[1]}")
        logger.info(f"  Memory Usage:  {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        logger.info(f"  Columns: {', '.join(df.columns.tolist())}")
        
        task_instance = context['task_instance']
        task_instance.xcom_push(key='dataframe_shape', 
                               value={'rows': df.shape[0], 'cols': df.shape[1]})
        task_instance.xcom_push(key='column_names', 
                               value=df.columns.tolist())
        
        execution_time = (datetime.now() - execution_start).total_seconds()
        logger.info(f"Phase 1 COMPLETE (Execution time: {execution_time:.2f}s)")
        logger.info("=" * 80)
        
        return {
            'status': 'success',
            'rows_loaded': df.shape[0],
            'columns_loaded': df.shape[1],
            'execution_time_seconds': execution_time
        }
        
    except Exception as e:
        logger.error(f"Phase 1 FAILED: {str(e)}")
        logger.error("=" * 80)
        raise


def validate_data_quality(**context) -> Dict[str, Any]:
    """
    Phase 2: Validate data quality with comprehensive checks
    
    Best Practice: Pre-processing validation with detailed reporting
    Reference: Chapter 6 - Data Quality & Validation Framework
    """
    execution_start = datetime.now()
    logger.info("=" * 80)
    logger.info(f"[{execution_start.isoformat()}] PHASE 2: DATA QUALITY VALIDATION")
    logger.info("=" * 80)
    
    try:
        reader = DataReader()
        df = reader.read_parquet(DATA_FILE)
        
        validator = BackupValidator()
        validation_report = validator.validate_before_processing(df)
        
        logger.info(f"  Validation Summary:")
        logger.info(f"  Total Rows Checked: {len(df):,}")
        logger.info(f"  Validation Status:  {validation_report.get('validation_passed', False)}")
        
        execution_time = (datetime.now() - execution_start).total_seconds()
        logger.info(f"  Phase 2 COMPLETE (Execution time: {execution_time:.2f}s)")
        logger.info("=" * 80)
        
        return {
            'status': 'success',
            'validation_passed': validation_report.get('validation_passed', False),
            'execution_time_seconds': execution_time
        }
        
    except Exception as e:
        logger.error(f" Phase 2 FAILED: {str(e)}")
        logger.error("=" * 80)
        raise


def process_data(**context) -> Dict[str, Any]:
    """
    Phase 3: Transform data according to school requirements
    
    Best Practice: Structured transformation pipeline with detailed logging
    Reference: Chapter 3 - Data Transformation Principles
                Chapter 5 - Pipeline Orchestration
    """
    execution_start = datetime.now()
    logger.info("=" * 80)
    logger.info(f"[{execution_start.isoformat()}] PHASE 3: DATA PROCESSING & TRANSFORMATION")
    logger.info("=" * 80)
    
    try:
        reader = DataReader()
        df = reader.read_parquet(DATA_FILE)
        
        logger.info(f"  Input Data:")
        logger.info(f"  Rows:    {df.shape[0]:,}")
        logger.info(f"  Columns: {df.shape[1]}")
        
        processor = DataProcessor()
        df_processed, transformation_report = processor.process_all(df)
        
        logger.info(f"  Output Data:")
        logger.info(f"  Rows:    {df_processed.shape[0]:,}")
        logger.info(f"  Columns: {df_processed.shape[1]}")
        logger.info(f"  Transformations Applied:")
        logger.info(f"  Columns Removed: {transformation_report['total_removed']}")
        logger.info(f"  Columns Added:   {transformation_report['total_added']}")
        logger.info(f"  Total Transformations: {transformation_report['transformations_applied']}")
        
        task_instance = context['task_instance']
        task_instance.xcom_push(key='processed_shape',
                               value={'rows': df_processed.shape[0], 'cols': df_processed.shape[1]})
        task_instance.xcom_push(key='processed_columns',
                               value=df_processed.columns.tolist())
        
        execution_time = (datetime.now() - execution_start).total_seconds()
        logger.info(f"  Phase 3 COMPLETE (Execution time: {execution_time:.2f}s)")
        logger.info("=" * 80)
        
        return {
            'status': 'success',
            'rows_processed': df_processed.shape[0],
            'columns_processed': df_processed.shape[1],
            'transformations_count': transformation_report['transformations_applied'],
            'execution_time_seconds': execution_time
        }
        
    except Exception as e:
        logger.error(f"  Phase 3 FAILED: {str(e)}")
        logger.error("=" * 80)
        raise


def validate_processed_data(**context) -> Dict[str, Any]:
    """
    Phase 4: Validate processed data quality
    
    Best Practice: Post-processing validation with detailed checking
    Reference: Chapter 6 - Data Quality Assurance
    """
    execution_start = datetime.now()
    logger.info("=" * 80)
    logger.info(f"[{execution_start.isoformat()}] PHASE 4: PROCESSED DATA VALIDATION")
    logger.info("=" * 80)
    
    try:
        reader = DataReader()
        df = reader.read_parquet(DATA_FILE)
        processor = DataProcessor()
        df_processed, _ = processor.process_all(df)
        
        validator = BackupValidator()
        validation_report = validator.validate_after_processing(df_processed)
        
        logger.info(f"  Post-Processing Validation:")
        logger.info(f"  Rows Validated: {len(df_processed):,}")
        logger.info(f"  Validation Status: {validation_report.get('validation_passed', False)}")
        
        execution_time = (datetime.now() - execution_start).total_seconds()
        logger.info(f"  Phase 4 COMPLETE (Execution time: {execution_time:.2f}s)")
        logger.info("=" * 80)
        
        return {
            'status': 'success',
            'validation_passed': validation_report.get('validation_passed', False),
            'execution_time_seconds': execution_time
        }
        
    except Exception as e:
        logger.error(f"  Phase 4 FAILED: {str(e)}")
        logger.error("=" * 80)
        raise


def write_local_data(**context) -> Dict[str, Any]:
    """
    Phase 5: Write processed data to local storage
    
    Best Practice: Structured output writing with integrity verification
    Reference: Chapter 5 - Output Validation & Monitoring
    """
    execution_start = datetime.now()
    logger.info("=" * 80)
    logger.info(f"[{execution_start.isoformat()}] PHASE 5: LOCAL STORAGE WRITING")
    logger.info("=" * 80)
    
    try:
        reader = DataReader()
        df = reader.read_parquet(DATA_FILE)
        processor = DataProcessor()
        df_processed, _ = processor.process_all(df)
        
        logger.info(f"  Writing Data:")
        logger.info(f"  Rows: {len(df_processed):,}")
        logger.info(f"  Columns: {len(df_processed.columns)}")
        
        writer = DataWriter()
        write_report = writer.write_both(df_processed, "yellow_taxi_processed")
        
        logger.info(f"  Local Output Files:")
        logger.info(f"  Status: {write_report.get('overall_success', False)}")
        if write_report.get('parquet'):
            logger.info(f"  Parquet: {write_report['parquet'].get('file_size_mb', 0)} MB")
        if write_report.get('csv'):
            logger.info(f"  CSV:     {write_report['csv'].get('file_size_mb', 0)} MB")
        
        execution_time = (datetime.now() - execution_start).total_seconds()
        logger.info(f"  Phase 5 COMPLETE (Execution time: {execution_time:.2f}s)")
        logger.info("=" * 80)
        
        return {
            'status': 'success',
            'files_written': 2,
            'execution_time_seconds': execution_time
        }
        
    except Exception as e:
        logger.error(f"  Phase 5 FAILED: {str(e)}")
        logger.error("=" * 80)
        raise


def upload_to_azure(**context) -> Dict[str, Any]:
    """
    Phase 6: Upload processed data to Azure Blob Storage
    
    Best Practice: Cloud upload with error handling and monitoring
    Reference: Chapter 7 - Cloud Engineering & Azure Integration
    """
    execution_start = datetime.now()
    logger.info("=" * 80)
    logger.info(f"[{execution_start.isoformat()}] PHASE 6: AZURE BLOB STORAGE UPLOAD")
    logger.info("=" * 80)
    
    try:
        if not AZURE_CONNECTION_STRING:
            logger.warning("  Azure connection string not configured")
            logger.warning("  Set AZURE_STORAGE_CONNECTION_STRING environment variable")
            logger.warning("  Skipping Azure upload (optional)")
            logger.info("=" * 80)
            return {
                'status': 'skipped',
                'reason': 'Azure credentials not configured',
                'execution_time_seconds': 0
            }
        
        reader = DataReader()
        df = reader.read_parquet(DATA_FILE)
        processor = DataProcessor()
        df_processed, _ = processor.process_all(df)
        
        logger.info(f"  Uploading to Azure:")
        logger.info(f"  Container: {AZURE_CONTAINER}")
        logger.info(f"  Rows: {len(df_processed):,}")
        
        azure_writer = AzureWriter(connection_string=AZURE_CONNECTION_STRING)
        azure_report = azure_writer.write_both_to_azure(df_processed, AZURE_CONTAINER)
        
        logger.info(f"  Azure Upload Results:")
        logger.info(f"  Status: {azure_report.get('overall_success', False)}")
        if azure_report.get('parquet'):
            logger.info(f"  Parquet Blob: {azure_report['parquet'].get('file_size_mb', 0)} MB")
        if azure_report.get('csv'):
            logger.info(f"  CSV Blob: {azure_report['csv'].get('file_size_mb', 0)} MB")
        
        execution_time = (datetime.now() - execution_start).total_seconds()
        logger.info(f" Phase 6 COMPLETE (Execution time: {execution_time:.2f}s)")
        logger.info("=" * 80)
        
        return {
            'status': 'success',
            'files_uploaded': 2,
            'execution_time_seconds': execution_time
        }
        
    except Exception as e:
        logger.error(f"  Phase 6 FAILED: {str(e)}")
        logger.warning("Continuing pipeline (Azure upload is optional)")
        logger.info("=" * 80)
        return {
            'status': 'failed',
            'error': str(e),
            'execution_time_seconds': 0
        }


def pipeline_summary(**context) -> Dict[str, Any]:
    """
    Final Pipeline Summary - Print execution statistics
    
    Best Practice: Comprehensive execution reporting
    Reference: Chapter 5 - Pipeline Monitoring & Orchestration
    """
    execution_start = datetime.now()
    logger.info("=" * 80)
    logger.info(f"[{execution_start.isoformat()}] PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 80)
    
    logger.info("")
    logger.info(" ALL PHASES COMPLETED SUCCESSFULLY!")
    logger.info("")
    logger.info(" Execution Summary:")
    logger.info(f"  Data File:        {DATA_FILE}")
    logger.info(f"  Output Directory: {OUTPUT_DIR}")
    logger.info(f"  Azure Container:  {AZURE_CONTAINER}")
    logger.info(f"  Execution Date:   {execution_start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    logger.info("=" * 80)
    
    return {
        'status': 'complete',
        'execution_timestamp': execution_start.isoformat()
    }



with dag:
    
    task_read = PythonOperator(
        task_id='read_data',
        python_callable=read_data,
        doc='Read raw Yellow Taxi data from Parquet file',
    )
    
    task_validate = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
        doc='Validate data quality with 5 validation rules',
    )
    
    task_process = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        doc='Transform data according to school requirements',
    )
    
    task_validate_processed = PythonOperator(
        task_id='validate_processed_data',
        python_callable=validate_processed_data,
        doc='Validate processed data schema',
    )
    
    task_write_local = PythonOperator(
        task_id='write_local_data',
        python_callable=write_local_data,
        doc='Write processed data to local Parquet and CSV',
    )
    
    task_upload_azure = PythonOperator(
        task_id='upload_to_azure',
        python_callable=upload_to_azure,
        doc='Upload processed data to Azure Blob Storage',
    )
    
    task_summary = PythonOperator(
        task_id='pipeline_summary',
        python_callable=pipeline_summary,
        doc='Print pipeline execution summary',
    )
    
    
    task_read >> task_validate >> task_process >> task_validate_processed >> task_write_local >> task_upload_azure >> task_summary



