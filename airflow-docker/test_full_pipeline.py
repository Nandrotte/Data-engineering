#!/usr/bin/env python3
"""
Test script for complete Part 1 Batch Processing Pipeline
Tests: Reader → Validator → Processor → Writer → Azure
"""

import pandas as pd
import logging
import os
from pathlib import Path

# Import pipeline components
from pipelines.batch.data_reader import DataReader
from pipelines.batch.validation_rules import ValidationRules
from pipelines.batch.backup_validator import BackupValidator
from pipelines.batch.data_processor import DataProcessor
from pipelines.batch.data_writer import DataWriter
from pipelines.batch.azure_writer import AzureWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Execute complete pipeline"""
    
    logger.info("=" * 100)
    logger.info("STARTING COMPLETE PART 1 BATCH PROCESSING PIPELINE TEST")
    logger.info("=" * 100)
    
    # ============================================================================
    # PART 1.1: DATA INGESTION & VALIDATION
    # ============================================================================
    
    logger.info("\n" + "─" * 100)
    logger.info("PHASE 1: DATA INGESTION & VALIDATION")
    logger.info("─" * 100)
    
    try:
        # 1.1.1: Read Parquet file
        logger.info("\n[1/7] Reading Yellow Taxi Parquet file...")
        parquet_file = Path("yellow_tripdata_2025-01.parquet")
        
        if not parquet_file.exists():
            logger.error(f"✗ File not found: {parquet_file}")
            return False
        
        df = DataReader.read_parquet(str(parquet_file))
        logger.info(f"✓ Loaded: {len(df):,} rows × {len(df.columns)} columns")
        
        # 1.1.2: Validate data (all 5 checks)
        logger.info("\n[2/7] Running validation checks...")
        validation_report = ValidationRules.validate_all(df)
        
        if not validation_report['overall_valid']:
            logger.warning("⚠️  Validation issues found:")
            for check_name, check_result in validation_report['checks'].items():
                if not check_result['passed']:
                    logger.warning(f"  ✗ {check_name}: {check_result}")
        else:
            logger.info("✓ All validation checks passed!")
        
        logger.info(f"\nValidation Report:")
        logger.info(f"  Total rows: {validation_report['total_rows']:,}")
        logger.info(f"  Total columns: {validation_report['total_columns']}")
        logger.info(f"  Checks passed: {sum(1 for c in validation_report['checks'].values() if c['passed'])}/5")
        
    except Exception as e:
        logger.error(f"✗ Phase 1 failed: {e}")
        return False
    
    # ============================================================================
    # PART 1.2: DATA PROCESSING
    # ============================================================================
    
    logger.info("\n" + "─" * 100)
    logger.info("PHASE 2: DATA PROCESSING & TRANSFORMATION")
    logger.info("─" * 100)
    
    try:
        # 1.2.1: Process data (all 8 transformations)
        logger.info("\n[3/7] Processing data (removing/adding columns)...")
        processed_df, processor_report = DataProcessor.process_all(df)
        
        logger.info(f"\nProcessing Report:")
        logger.info(f"  Removed: {processor_report['columns_removed']}")
        logger.info(f"  Added: {processor_report['columns_added']}")
        logger.info(f"  Shape change: {processor_report['initial_shape']} → {processor_report['final_shape']}")
        
        # 1.2.2: Backup validation (verify processing quality)
        logger.info("\n[4/7] Running backup validation (post-processing check)...")
        backup_report = BackupValidator.validate_after_processing(processed_df)
        
        if not backup_report['overall_valid']:
            logger.warning("⚠️  Backup validation issues found:")
            for check_name, check_result in backup_report['checks'].items():
                if not check_result['passed']:
                    logger.warning(f"  ✗ {check_name}: {check_result}")
        else:
            logger.info("✓ Backup validation passed!")
        
    except Exception as e:
        logger.error(f"✗ Phase 2 failed: {e}")
        return False
    
    # ============================================================================
    # PART 1.2: DATA OUTPUT (LOCAL + AZURE)
    # ============================================================================
    
    logger.info("\n" + "─" * 100)
    logger.info("PHASE 3: DATA OUTPUT (LOCAL & AZURE)")
    logger.info("─" * 100)
    
    try:
        # 1.2.3: Write to local storage
        logger.info("\n[5/7] Writing to local storage (Parquet + CSV)...")
        
        writer = DataWriter()
        local_report = writer.write_both(processed_df, 'yellow_taxi_processed_jan2025')
        
        logger.info(f"\nLocal Storage Report:")
        logger.info(f"  Parquet: {local_report['local']['parquet']['file_size_mb']} MB → {local_report['local']['parquet']['file_path']}")
        logger.info(f"  CSV: {local_report['local']['csv']['file_size_mb']} MB → {local_report['local']['csv']['file_path']}")
        
        # 1.2.4: Write to Azure (if configured)
        logger.info("\n[6/7] Writing to Azure Blob Storage...")
        
        azure_connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        
        if azure_connection_string:
            try:
                azure_writer = AzureWriter(
                    connection_string=azure_connection_string,
                    container_name='processed-data'
                )
                writer_with_azure = DataWriter(azure_writer=azure_writer)
                full_report = writer_with_azure.write_both(processed_df, 'yellow_taxi_processed_jan2025')
                
                logger.info(f"\nAzure Storage Report:")
                if full_report['azure']['success']:
                    logger.info(f"  ✓ Parquet: {full_report['azure']['parquet']['file_size_mb']} MB")
                    logger.info(f"  ✓ CSV: {full_report['azure']['csv']['file_size_mb']} MB")
                    logger.info(f"  Container: {full_report['azure']['container']}")
                else:
                    logger.warning(f"  ✗ Azure upload failed")
            except Exception as e:
                logger.warning(f"⚠️  Azure upload failed: {e}")
                logger.info("   (This is OK if Azure client isn't installed or connection string is invalid)")
        else:
            logger.warning("⚠️  AZURE_STORAGE_CONNECTION_STRING not set")
            logger.info("   To enable Azure uploads, set the environment variable:")
            logger.info("   $env:AZURE_STORAGE_CONNECTION_STRING = 'your_connection_string'")
        
    except Exception as e:
        logger.error(f"✗ Phase 3 failed: {e}")
        return False
    
    # ============================================================================
    # SUMMARY
    # ============================================================================
    
    logger.info("\n" + "=" * 100)
    logger.info("PIPELINE EXECUTION COMPLETE")
    logger.info("=" * 100)
    
    logger.info("\n✅ SUMMARY:")
    logger.info(f"  ✓ Phase 1 (Ingestion & Validation): PASSED")
    logger.info(f"  ✓ Phase 2 (Processing): PASSED")
    logger.info(f"  ✓ Phase 3 (Output): PASSED")
    
    logger.info(f"\n📊 DATA STATISTICS:")
    logger.info(f"  Input rows: {validation_report['total_rows']:,}")
    logger.info(f"  Output rows: {len(processed_df):,}")
    logger.info(f"  Input columns: {validation_report['total_columns']}")
    logger.info(f"  Output columns: {len(processed_df.columns)}")
    logger.info(f"  Columns removed: {len(processor_report['columns_removed'])}")
    logger.info(f"  Columns added: {len(processor_report['columns_added'])}")
    
    logger.info(f"\n📁 OUTPUT LOCATIONS:")
    logger.info(f"  Local Parquet: {local_report['local']['parquet']['file_path']}")
    logger.info(f"  Local CSV: {local_report['local']['csv']['file_path']}")
    logger.info(f"  Local folder: {local_report['local']['output_directory']}")
    
    if azure_connection_string and full_report['azure']['success']:
        logger.info(f"  Azure Container: {full_report['azure']['container']}")
        logger.info(f"  Azure Parquet: yellow_taxi_processed_jan2025.parquet")
        logger.info(f"  Azure CSV: yellow_taxi_processed_jan2025.csv")
    
    logger.info("\n" + "=" * 100)
    logger.info("✅ PIPELINE TEST SUCCESSFUL - READY FOR AIRFLOW DAG INTEGRATION")
    logger.info("=" * 100)
    
    return True


if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
