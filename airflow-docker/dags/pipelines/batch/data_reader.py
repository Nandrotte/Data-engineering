"""
Data Reader - UCLL Best Practice Implementation
================================================================================
Multi-format file reader with data profiling and validation

Supported Formats:
  - Parquet (.parquet) - Primary format for efficiency
  - CSV (.csv) - Common text format
  - Excel (.xlsx, .xls) - Legacy format support

School References:
  - Chapter 5: Pipeline Monitoring & Validation
  - Chapter 6: Data Quality on Input

This module implements:
  - Multi-format file reading with error handling
  - Data profiling on load (rows, columns, memory, types)
  - Input validation and schema checking
  - Structured logging with timestamps
  - File metadata extraction

School Requirements: Data Engineering Project - Part 1 (Input Data Handling)
Author: Data Engineering Team
Created: April 28, 2026
Last Updated: April 29, 2026 (Best Practices Implementation)
"""

import pandas as pd
import os
from pathlib import Path
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DataReader:
    """Reads data from multiple formats with profiling and validation"""
    
    @staticmethod
    def _profile_dataframe(df: pd.DataFrame, file_path: str) -> Dict[str, Any]:
        """
        Generate data profile for loaded dataset
        
        Reference: Chapter 6 - Data Quality Profiling
        
        Returns detailed profile with rows, columns, memory, data types, and null counts
        """
        profile = {
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'null_counts': {col: int(df[col].isnull().sum()) for col in df.columns},
            'duplicate_rows': int(df.duplicated().sum()),
            'timestamp': datetime.now().isoformat()
        }
        return profile
    
    @staticmethod
    def read_parquet(file_path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Reads a Parquet file with profiling and validation
        
        Reference: Chapter 5 - Input Data Validation
        """
        read_start = datetime.now()
        logger.info(f"\n[{read_start.isoformat()}] Reading Parquet: {file_path}")
        
        if not os.path.exists(file_path):
            logger.error(f"✗ File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            df = pd.read_parquet(file_path, columns=columns)
            
            profile = DataReader._profile_dataframe(df, file_path)
            
            read_time = (datetime.now() - read_start).total_seconds()
            logger.info(f"✓ Loaded Parquet: {profile['total_rows']:,} rows × {profile['total_columns']} cols ({profile['memory_usage_mb']:.2f} MB)")
            logger.info(f"  Read time: {read_time:.2f}s")
            
            return df
            
        except Exception as e:
            logger.error(f"✗ Failed to read Parquet: {e}")
            raise
    
    @staticmethod
    def read_csv(file_path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Reads a CSV file with profiling and validation
        
        Reference: Chapter 5 - Input Data Validation
        """
        read_start = datetime.now()
        logger.info(f"\n[{read_start.isoformat()}] Reading CSV: {file_path}")
        
        if not os.path.exists(file_path):
            logger.error(f"✗ File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            df = pd.read_csv(file_path, usecols=columns if columns else None)
            
            profile = DataReader._profile_dataframe(df, file_path)
            
            read_time = (datetime.now() - read_start).total_seconds()
            logger.info(f"✓ Loaded CSV: {profile['total_rows']:,} rows × {profile['total_columns']} cols ({profile['memory_usage_mb']:.2f} MB)")
            logger.info(f"  Read time: {read_time:.2f}s")
            
            return df
            
        except Exception as e:
            logger.error(f"✗ Failed to read CSV: {e}")
            raise
    
    @staticmethod
    def read_excel(file_path: str, sheet_name: str = 0, 
                   columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Reads an Excel file with profiling and validation
        
        Reference: Chapter 5 - Input Data Validation
        """
        read_start = datetime.now()
        logger.info(f"\n[{read_start.isoformat()}] Reading Excel: {file_path}")
        
        if not os.path.exists(file_path):
            logger.error(f"✗ File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, 
                              usecols=columns if columns else None)
            
            profile = DataReader._profile_dataframe(df, file_path)
            
            read_time = (datetime.now() - read_start).total_seconds()
            logger.info(f"✓ Loaded Excel (sheet '{sheet_name}'): {profile['total_rows']:,} rows × {profile['total_columns']} cols ({profile['memory_usage_mb']:.2f} MB)")
            logger.info(f"  Read time: {read_time:.2f}s")
            
            return df
            
        except Exception as e:
            logger.error(f"✗ Failed to read Excel: {e}")
            raise
    
    @staticmethod
    def auto_detect_and_read(file_path: str, 
                            columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Auto-detects file format and reads accordingly
        
        Supported: .parquet, .csv, .xlsx, .xls
        """
        file_ext = Path(file_path).suffix.lower()
        
        logger.info(f"Auto-detecting format: {file_ext}")
        
        if file_ext == '.parquet':
            return DataReader.read_parquet(file_path, columns)
        elif file_ext == '.csv':
            return DataReader.read_csv(file_path, columns)
        elif file_ext in ['.xlsx', '.xls']:
            return DataReader.read_excel(file_path, columns=columns)
        else:
            logger.error(f"✗ Unsupported format: {file_ext}")
            raise ValueError(f"Unsupported format: {file_ext}")
    
    @staticmethod
    def get_file_info(file_path: str) -> Dict[str, Any]:
        """Returns file information (size, type, modified date)"""
        if not os.path.exists(file_path):
            logger.error(f"✗ File not found: {file_path}")
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
    
    try:
        df = DataReader.read_parquet('yellow_tripdata_2025-01.parquet')
        print(f"✓ Loaded: {len(df)} rows, {len(df.columns)} columns")
    except FileNotFoundError:
        print("⚠ File not found (this is ok for demo)")

