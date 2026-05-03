import pandas as pd
import os
from pathlib import Path
from typing import Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class RealtimeDataWriter:
    """Write processed data to local output"""
    
    OUTPUT_DIR = Path('output/realtime')
    
    def __init__(self, azure_writer=None):
        self.azure_writer = azure_writer
    
    @staticmethod
    def setup_directories() -> None:
        """Create output directories"""
        RealtimeDataWriter.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"✓ Output directory ready: {RealtimeDataWriter.OUTPUT_DIR}")
    
    @staticmethod
    def write_csv(df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Write CSV locally"""
        RealtimeDataWriter.setup_directories()
        
        file_path = RealtimeDataWriter.OUTPUT_DIR / filename
        
        logger.info(f"Writing to CSV: {file_path}")
        
        try:
            df.to_csv(file_path, index=False)
            
            file_size = os.path.getsize(file_path) / 1024 / 1024
            
            logger.info(f"✓ CSV written: {file_size:.2f} MB")
            
            return {
                'format': 'csv',
                'file_path': str(file_path),
                'file_size_mb': round(file_size, 2),
                'rows': len(df),
                'success': True
            }
            
        except Exception as e:
            logger.error(f"✗ Failed to write CSV: {e}")
            return {'success': False, 'error': str(e)}
    
    def write_both(self, df: pd.DataFrame, base_filename: str, batch_timestamp: str = None) -> Dict[str, Any]:
        """Write locally and optionally to Azure"""
        execution_start = datetime.now()
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"[{execution_start.isoformat()}] WRITING REALTIME OUTPUT")
        logger.info("=" * 80)
        
        base_name = Path(base_filename).stem
        
        # Use provided batch timestamp or generate new one
        timestamp = batch_timestamp if batch_timestamp else execution_start.strftime('%Y%m%d_%H%M%S')
        
        logger.info(f"\n LOCAL STORAGE")
        csv_report = self.write_csv(df, f"realtime_{base_name}_{timestamp}.csv")
        
        logger.info(f"\n  AZURE BLOB STORAGE")
        azure_report = None
        if self.azure_writer:
            try:
                azure_report = self.azure_writer.write_both_to_azure(df, f"realtime_{base_name}", batch_timestamp=timestamp)
            except Exception as e:
                logger.error(f"✗ Azure upload failed: {e}")
                azure_report = {'success': False, 'error': str(e)}
        else:
            logger.info(f"  Azure writer not configured")
        
        execution_time = (datetime.now() - execution_start).total_seconds()
        
        report = {
            'timestamp': execution_start.isoformat(),
            'execution_time_seconds': execution_time,
            'local': csv_report,
            'azure': azure_report or {'status': 'not_configured'},
            'overall_success': csv_report.get('success', False)
        }
        
        logger.info(f"\n" + "=" * 80)
        logger.info(f" REALTIME OUTPUT COMPLETE (Time: {execution_time:.2f}s)")
        logger.info("=" * 80)
        
        return report
