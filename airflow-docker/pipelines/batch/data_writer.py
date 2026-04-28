"""Data Writer - Local and Azure Storage Output - School Part 1.2"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
import logging
import os

logger = logging.getLogger(__name__)


class DataWriter:
    """Write processed data to local storage and optionally to Azure Blob Storage"""
    
    # Output directories for local storage
    OUTPUT_DIR = Path('output')
    PARQUET_DIR = OUTPUT_DIR / 'parquet'
    CSV_DIR = OUTPUT_DIR / 'csv'
    
    def __init__(self, azure_writer: Optional[object] = None):
        """
        Initialize DataWriter
        
        Args:
            azure_writer: Optional AzureWriter instance for cloud uploads
        """
        self.azure_writer = azure_writer
    
    @staticmethod
    def setup_directories() -> None:
        """Create output directories if they don't exist (local storage)"""
        DataWriter.PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        DataWriter.CSV_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"✓ Local output directories ready: {DataWriter.OUTPUT_DIR}")
    
    @staticmethod
    def write_parquet(df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Write DataFrame to Parquet file (local storage)"""
        DataWriter.setup_directories()
        
        file_path = DataWriter.PARQUET_DIR / filename
        
        logger.info(f"Writing to Parquet: {file_path}")
        
        try:
            df.to_parquet(file_path, compression='snappy', index=False)
            
            file_size = os.path.getsize(file_path) / 1024 / 1024  # MB
            
            report = {
                'format': 'parquet',
                'file_path': str(file_path),
                'file_size_mb': round(file_size, 2),
                'rows': len(df),
                'columns': len(df.columns),
                'compression': 'snappy',
                'location': 'local',
                'success': True
            }
            
            logger.info(f"✓ Parquet written locally: {file_size:.2f} MB")
            return report
            
        except Exception as e:
            logger.error(f"✗ Failed to write Parquet: {e}")
            return {'success': False, 'error': str(e)}
    
    @staticmethod
    def write_csv(df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Write DataFrame to CSV file (local storage)"""
        DataWriter.setup_directories()
        
        file_path = DataWriter.CSV_DIR / filename
        
        logger.info(f"Writing to CSV: {file_path}")
        
        try:
            df.to_csv(file_path, index=False)
            
            file_size = os.path.getsize(file_path) / 1024 / 1024  # MB
            
            report = {
                'format': 'csv',
                'file_path': str(file_path),
                'file_size_mb': round(file_size, 2),
                'rows': len(df),
                'columns': len(df.columns),
                'compression': 'none',
                'location': 'local',
                'success': True
            }
            
            logger.info(f"✓ CSV written locally: {file_size:.2f} MB")
            return report
            
        except Exception as e:
            logger.error(f"✗ Failed to write CSV: {e}")
            return {'success': False, 'error': str(e)}
    
    def write_both(self, df: pd.DataFrame, base_filename: str) -> Dict[str, Any]:
        """
        Write to both local storage AND Azure (if configured)
        This is the school requirement: "Writer writes to Azure blob storage AND local output folder"
        """
        logger.info("=" * 80)
        logger.info("WRITING OUTPUT - LOCAL & AZURE (School Part 1.2)")
        logger.info("=" * 80)
        
        # Remove extension if present
        base_name = Path(base_filename).stem
        
        # 1. WRITE TO LOCAL STORAGE
        logger.info("\n--- LOCAL STORAGE ---")
        parquet_report = self.write_parquet(df, f"{base_name}.parquet")
        csv_report = self.write_csv(df, f"{base_name}.csv")
        
        # 2. WRITE TO AZURE BLOB STORAGE (if available)
        azure_report = None
        if self.azure_writer:
            logger.info("\n--- AZURE BLOB STORAGE ---")
            azure_report = self.azure_writer.write_both_to_azure(df, base_name)
        else:
            logger.warning("\n⚠️  Azure writer not configured. Skipping cloud upload.")
            logger.info("    To enable Azure uploads, initialize DataWriter with AzureWriter instance")
        
        # Combine reports
        report = {
            'data_shape': (len(df), len(df.columns)),
            'local': {
                'parquet': parquet_report,
                'csv': csv_report,
                'output_directory': str(DataWriter.OUTPUT_DIR)
            },
            'azure': azure_report or {'status': 'not_configured'},
            'success': parquet_report.get('success', False) and csv_report.get('success', False)
        }
        
        logger.info("\n" + "=" * 80)
        logger.info("OUTPUT COMPLETE")
        logger.info(f"  Local Parquet: {parquet_report['file_size_mb']} MB")
        logger.info(f"  Local CSV:     {csv_report['file_size_mb']} MB")
        if self.azure_writer and azure_report and azure_report.get('success'):
            logger.info(f"  Azure Parquet: {azure_report['parquet']['file_size_mb']} MB")
            logger.info(f"  Azure CSV:     {azure_report['csv']['file_size_mb']} MB")
        logger.info("=" * 80)
        
        return report
    
    @staticmethod
    def get_output_summary() -> Dict[str, Any]:
        """Get summary of local output directory"""
        DataWriter.setup_directories()
        
        summary = {
            'output_directory': str(DataWriter.OUTPUT_DIR),
            'parquet_files': [str(f) for f in DataWriter.PARQUET_DIR.glob('*.parquet')],
            'csv_files': [str(f) for f in DataWriter.CSV_DIR.glob('*.csv')],
        }
        
        return summary


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print("DataWriter module loaded successfully")
