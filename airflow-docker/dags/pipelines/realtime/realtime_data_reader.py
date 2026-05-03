import pandas as pd
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class RealtimeDataReader:
    """Read CSV files from real-time input folder"""
    
    @staticmethod
    def read_csv(file_path: str) -> pd.DataFrame:
        """Read CSV file"""
        read_start = datetime.now()
        logger.info(f"\n[{read_start.isoformat()}] Reading CSV: {file_path}")
        
        if not os.path.exists(file_path):
            logger.error(f"✗ File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            
            read_time = (datetime.now() - read_start).total_seconds()
            logger.info(f"✓ Loaded: {len(df):,} rows × {len(df.columns)} cols ({df.memory_usage(deep=True).sum() / 1024**2:.2f} MB)")
            logger.info(f"  Read time: {read_time:.2f}s")
            
            return df
            
        except Exception as e:
            logger.error(f"✗ Failed to read CSV: {e}")
            raise
