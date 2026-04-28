"""Azure Blob Storage Writer - School Part 1.2 Requirement"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any
import logging
import io
import os

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
            # Serialize DataFrame to Parquet in memory
            buffer = io.BytesIO()
            df.to_parquet(buffer, compression='snappy', index=False)
            buffer.seek(0)
            
            # Upload to Azure
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
            # Serialize DataFrame to CSV in memory
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            
            # Upload to Azure
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
    
    def write_both_to_azure(self, df: pd.DataFrame, base_name: str) -> Dict[str, Any]:
        """
        Write DataFrame as both Parquet and CSV to Azure Blob Storage
        
        Args:
            df: DataFrame to write
            base_name: Base name for files (e.g., 'yellow_taxi_processed')
            
        Returns:
            Report dictionary with both upload details
        """
        logger.info("=" * 80)
        logger.info("UPLOADING TO AZURE BLOB STORAGE")
        logger.info("=" * 80)
        
        parquet_blob = f"{base_name}.parquet"
        csv_blob = f"{base_name}.csv"
        
        parquet_report = self.write_parquet_to_azure(df, parquet_blob)
        csv_report = self.write_csv_to_azure(df, csv_blob)
        
        report = {
            'data_shape': (len(df), len(df.columns)),
            'container': self.container_name,
            'parquet': parquet_report,
            'csv': csv_report,
            'success': parquet_report.get('success', False) and csv_report.get('success', False)
        }
        
        logger.info("=" * 80)
        if report['success']:
            logger.info(f"AZURE UPLOAD COMPLETE")
            logger.info(f"  Parquet: {parquet_report['file_size_mb']} MB")
            logger.info(f"  CSV:     {csv_report['file_size_mb']} MB")
            logger.info(f"  Container: {self.container_name}")
        else:
            logger.warning(f"AZURE UPLOAD INCOMPLETE - Check connection string")
        logger.info("=" * 80)
        
        return report


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print("AzureWriter module loaded successfully")
    print("Set AZURE_STORAGE_CONNECTION_STRING environment variable to enable Azure uploads")
