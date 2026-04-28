"""Data Reader - Leest data uit Parquet, CSV, Excel"""

import pandas as pd
import os
from pathlib import Path
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


class DataReader:
    """Reads data from multiple formats (Parquet, CSV, Excel)"""
    
    @staticmethod
    def read_parquet(file_path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Reads a Parquet file"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        logger.info(f"Reading parquet: {file_path}")
        df = pd.read_parquet(file_path, columns=columns)
        logger.info(f"✓ Loaded: {len(df)} rows × {len(df.columns)} columns")
        return df
    
    
    @staticmethod
    def read_csv(file_path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Reads a CSV file"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        logger.info(f"Reading CSV: {file_path}")
        df = pd.read_csv(file_path, usecols=columns if columns else None)
        logger.info(f"✓ Loaded: {len(df)} rows × {len(df.columns)} columns")
        return df
    
    @staticmethod
    def read_excel(file_path: str, sheet_name: str = 0, 
                   columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Reads an Excel file"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        logger.info(f"Reading Excel: {file_path}")
        df = pd.read_excel(file_path, sheet_name=sheet_name, 
                          usecols=columns if columns else None)
        logger.info(f"✓ Loaded: {len(df)} rows × {len(df.columns)} columns")
        return df
    
    @staticmethod
    def auto_detect_and_read(file_path: str, 
                            columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Auto-detects file format and reads accordingly"""
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext == '.parquet':
            return DataReader.read_parquet(file_path, columns)
        elif file_ext == '.csv':
            return DataReader.read_csv(file_path, columns)
        elif file_ext in ['.xlsx', '.xls']:
            return DataReader.read_excel(file_path, columns=columns)
        else:
            raise ValueError(f"Unsupported format: {file_ext}")
    
    @staticmethod
    def get_file_info(file_path: str) -> dict:
        """Returns file information (size, type, modified date)"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        file_stat = os.stat(file_path)
        
        return {
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'file_size_mb': round(file_stat.st_size / 1024 / 1024, 2),
            'file_extension': Path(file_path).suffix,
            'last_modified': pd.Timestamp(file_stat.st_mtime, unit='s').isoformat(),
        }

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    # Example: Read parquet file
    try:
        df = DataReader.read_parquet('yellow_tripdata_2025-01.parquet')
        print(f"✓ Loaded: {len(df)} rows, {len(df.columns)} columns")
    except FileNotFoundError:
        print("⚠ File not found (this is ok for demo)")

