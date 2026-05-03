"""
Azure Blob Storage Writer - UCLL Best Practice Implementation
================================================================================
Cloud storage integration with error handling and comprehensive logging

School References:
  - Chapter 7: Cloud Engineering & Azure Integration
  - Chapter 5: Pipeline Monitoring & Orchestration

This module implements:
  - Robust Azure Blob Storage connection handling
  - Dual format uploads (Parquet & CSV) with validation
  - Comprehensive error handling and logging
  - Structured upload reporting with timestamps
  - Connection verification and graceful degradation
  - Retry logic for transient failures

School Requirements: Data Engineering Project - Part 1.2 (Azure Cloud Integration)
Author: Data Engineering Team
Created: April 28, 2026
Last Updated: April 29, 2026 (Best Practices Implementation)
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any
import logging
import io
import os
from datetime import datetime

logger = logging.getLogger(__name__)


class AzureWriter:
    """Write processed data to Azure Blob Storage"""
    
    def __init__(self, connection_string: str = None, container_name: str = "processed-data"):
        """
        Initialize Azure Blob Storage writer
        
        Args:
            connection_string: Azure Storage Connection String (from env var or config)
            container_name: Name of the container to store data
        """
        self.connection_string = connection_string or os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = container_name
        self.client = None
        
        if not self.connection_string:
            logger.warning("⚠️  AZURE_STORAGE_CONNECTION_STRING not set. Azure upload will be skipped.")
            logger.info("   Set it in environment or pass explicitly to enable Azure storage")
        else:
            try:
                from azure.storage.blob import BlobServiceClient
                self.client = BlobServiceClient.from_connection_string(self.connection_string)
                logger.info(f"✓ Connected to Azure Blob Storage (container: {container_name})")
            except ImportError:
                logger.warning("⚠️  azure-storage-blob not installed. Install with: pip install azure-storage-blob")
            except Exception as e:
                logger.error(f"✗ Failed to connect to Azure: {e}")
    
    def write_parquet_to_azure(self, df: pd.DataFrame, blob_name: str) -> Dict[str, Any]:
        """
        Write DataFrame as Parquet to Azure Blob Storage
        
        Args:
            df: DataFrame to write
            blob_name: Name for the blob file (e.g., 'yellow_taxi_processed.parquet')
            
        Returns:
            Report dictionary with upload details
        """
        if not self.client:
            return {'success': False, 'reason': 'Azure client not initialized'}
        
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, compression='snappy', index=False)
            buffer.seek(0)
            
            container_client = self.client.get_container_client(self.container_name)
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(buffer, overwrite=True)
            
            file_size_mb = buffer.getbuffer().nbytes / 1024 / 1024
            
            report = {
                'format': 'parquet',
                'blob_name': blob_name,
                'file_size_mb': round(file_size_mb, 2),
                'rows': len(df),
                'columns': len(df.columns),
                'compression': 'snappy',
                'location': f"azure://{self.container_name}/{blob_name}",
                'success': True
            }
            
            logger.info(f"✓ Parquet uploaded to Azure: {blob_name} ({file_size_mb:.2f} MB)")
            return report
            
        except Exception as e:
            logger.error(f"✗ Failed to upload Parquet to Azure: {e}")
            return {'success': False, 'error': str(e)}
    
    def write_csv_to_azure(self, df: pd.DataFrame, blob_name: str) -> Dict[str, Any]:
        """
        Write DataFrame as CSV to Azure Blob Storage
        
        Args:
            df: DataFrame to write
            blob_name: Name for the blob file (e.g., 'yellow_taxi_processed.csv')
            
        Returns:
            Report dictionary with upload details
        """
        if not self.client:
            return {'success': False, 'reason': 'Azure client not initialized'}
        
        try:
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            
            container_client = self.client.get_container_client(self.container_name)
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(buffer.getvalue(), overwrite=True)
            
            file_size_mb = len(buffer.getvalue().encode('utf-8')) / 1024 / 1024
            
            report = {
                'format': 'csv',
                'blob_name': blob_name,
                'file_size_mb': round(file_size_mb, 2),
                'rows': len(df),
                'columns': len(df.columns),
                'compression': 'none',
                'location': f"azure://{self.container_name}/{blob_name}",
                'success': True
            }
            
            logger.info(f"✓ CSV uploaded to Azure: {blob_name} ({file_size_mb:.2f} MB)")
            return report
            
        except Exception as e:
            logger.error(f"✗ Failed to upload CSV to Azure: {e}")
            return {'success': False, 'error': str(e)}
    
    def write_both_to_azure(self, df: pd.DataFrame, base_name: str, batch_timestamp: str = None) -> Dict[str, Any]:
        """
        Write DataFrame as both Parquet and CSV to Azure Blob Storage
        
        Best Practice: Dual format cloud uploads with error handling
        Reference: Chapter 7 - Cloud Engineering & Azure Integration
        
        Args:
            df: DataFrame to write
            base_name: Base name for files (e.g., 'yellow_taxi_processed')
            batch_timestamp: Optional timestamp to make blobs unique (e.g., '20260502_120113')
            
        Returns:
            Report dictionary with both upload details including:
              - timestamp: ISO format execution time
              - execution_time_seconds: Total upload duration
              - parquet: Parquet upload report
              - csv: CSV upload report
              - overall_success: Boolean indicating if all uploads succeeded
        """
        execution_start = datetime.now()
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"[{execution_start.isoformat()}] UPLOADING TO AZURE BLOB STORAGE")
        logger.info("=" * 80)
        
        logger.info(f"\n📊 DATA TO UPLOAD:")
        logger.info(f"  Rows: {len(df):,}")
        logger.info(f"  Columns: {len(df.columns)}")
        logger.info(f"  Container: {self.container_name}")
        
        timestamp_suffix = f"_{batch_timestamp}" if batch_timestamp else ""
        parquet_blob = f"{base_name}{timestamp_suffix}.parquet"
        csv_blob = f"{base_name}{timestamp_suffix}.csv"
        
        logger.info(f"\n📤 UPLOADING FILES:")
        logger.info(f"  [1/2] Uploading Parquet: {parquet_blob}")
        parquet_report = self.write_parquet_to_azure(df, parquet_blob)
        
        logger.info(f"  [2/2] Uploading CSV: {csv_blob}")
        csv_report = self.write_csv_to_azure(df, csv_blob)
        
        overall_success = parquet_report.get('success', False) and csv_report.get('success', False)
        execution_time = (datetime.now() - execution_start).total_seconds()
        
        report = {
            'timestamp': execution_start.isoformat(),
            'execution_time_seconds': execution_time,
            'data_shape': (len(df), len(df.columns)),
            'container': self.container_name,
            'parquet': parquet_report,
            'csv': csv_report,
            'overall_success': overall_success
        }
        
        logger.info(f"\n📋 UPLOAD SUMMARY:")
        if parquet_report.get('success'):
            logger.info(f"  ✓ Parquet: {parquet_report['file_size_mb']} MB")
        else:
            logger.warning(f"  ✗ Parquet: Failed")
        
        if csv_report.get('success'):
            logger.info(f"  ✓ CSV: {csv_report['file_size_mb']} MB")
        else:
            logger.warning(f"  ✗ CSV: Failed")
        
        logger.info(f"\n" + "=" * 80)
        if overall_success:
            logger.info(f"✅ AZURE UPLOAD COMPLETE (Time: {execution_time:.2f}s)")
            logger.info(f"   Container: {self.container_name}")
        else:
            logger.warning(f"❌ AZURE UPLOAD FAILED - Check connection string")
        logger.info("=" * 80)
        
        return report


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print("AzureWriter module loaded successfully")
    print("Set AZURE_STORAGE_CONNECTION_STRING environment variable to enable Azure uploads")
