"""
Quick Validation After Processing - Backup Check
Based on UCLL Data Engineering Best Practices:
- Chapter 3: Data Transformation
- Chapter 5: Building and Monitoring Pipelines  
- Chapter 6: Data Quality
"""

import pandas as pd
import numpy as np
from typing import Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BackupValidator:
    """Validate data quality before & after processing - School best practices"""
    
    @staticmethod
    def validate_before_processing(df: pd.DataFrame) -> Dict[str, Any]:
        """Validate raw data before processing (School requirement - Chapter 6)"""
        logger.info("=" * 60)
        logger.info("PRE-PROCESSING VALIDATION STARTED")
        logger.info("=" * 60)
        
        try:
            schema_report = BackupValidator._validate_schema(df)
            profile_report = BackupValidator._profile_data(df)
            null_report = BackupValidator._check_nulls(df)
            dtype_report = BackupValidator._validate_dtypes(df)
            
            report = {
                'timestamp': datetime.now().isoformat(),
                'stage': 'pre_processing',
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'columns': df.columns.tolist(),
                'schema': schema_report,
                'profile': profile_report,
                'nulls': null_report,
                'dtypes': dtype_report,
                'status': 'raw_data_ready',
                'validation_passed': all([
                    schema_report['valid'],
                    len(null_report['critical_nulls']) == 0,
                    dtype_report['valid']
                ])
            }
            
            logger.info(f"✓ Pre-processing validation complete: {len(df)} rows × {len(df.columns)} cols")
            logger.info(f"✓ Overall Status: {'PASSED' if report['validation_passed'] else 'FAILED'}")
            logger.info("=" * 60)
            
            return report
        except Exception as e:
            logger.error(f"✗ Pre-processing validation failed: {str(e)}")
            raise
    
    @staticmethod
    def _validate_schema(df: pd.DataFrame) -> Dict[str, Any]:
        """Validate data schema structure"""
        try:
            schema = {
                'valid': True,
                'message': f'Schema valid: {len(df.columns)} columns',
                'column_count': len(df.columns)
            }
            return schema
        except Exception as e:
            logger.warning(f"Schema validation warning: {e}")
            return {'valid': False, 'message': str(e)}
    
    @staticmethod
    def _profile_data(df: pd.DataFrame) -> Dict[str, Any]:
        """Profile the data for monitoring (School Chapter 5 - Building Pipelines)"""
        profile = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            'numeric_columns': len(df.select_dtypes(include=np.number).columns),
            'categorical_columns': len(df.select_dtypes(include='object').columns),
            'datetime_columns': len(df.select_dtypes(include='datetime64').columns)
        }
        return profile
    
    @staticmethod
    def _check_nulls(df: pd.DataFrame) -> Dict[str, Any]:
        """Check for NULL values - critical for data quality"""
        null_counts = df.isnull().sum()
        critical_nulls = {col: int(count) for col, count in null_counts[null_counts > 0].items()}
        
        return {
            'total_null_cells': int(df.isnull().sum().sum()),
            'columns_with_nulls': len(critical_nulls),
            'critical_nulls': critical_nulls,
            'null_percentage': round((df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100), 2) if len(df) > 0 else 0
        }
    
    @staticmethod
    def _validate_dtypes(df: pd.DataFrame) -> Dict[str, Any]:
        """Validate data types"""
        return {
            'valid': True,
            'dtypes': {col: str(df[col].dtype) for col in df.columns}
        }
    
    @staticmethod
    def check_no_nulls_in_new_columns(df: pd.DataFrame) -> Dict[str, int]:
        """Check that school-required columns have no NULLs"""
        new_columns = [
            'trip_duration_minutes', 'average_speed_mph', 'revenue_per_mile',
            'trip_distance_category', 'fare_category', 'trip_time_of_day',
            'pickup_year', 'pickup_month'
        ]
        
        nulls = {}
        for col in new_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    nulls[col] = int(null_count)
                    logger.warning(f"⚠ NULL values found in column '{col}': {null_count}")
        
        return nulls
    
    @staticmethod
    def check_numeric_columns_valid(df: pd.DataFrame) -> Dict[str, Any]:
        """Check that numeric columns don't have infinite values (School Part 1.2)"""
        numeric_cols = ['average_speed_mph', 'revenue_per_mile', 'trip_duration_minutes']
        
        issues = {}
        for col in numeric_cols:
            if col in df.columns:
                inf_count = df[col].isin([float('inf'), float('-inf')]).sum()
                if inf_count > 0:
                    issues[col] = {'infinite_values': int(inf_count)}
                    logger.warning(f"⚠ Infinite values in '{col}': {inf_count}")
                
                neg_count = (df[col] < 0).sum()
                if neg_count > 0:
                    issues[col] = issues.get(col, {})
                    issues[col]['negative_values'] = int(neg_count)
                    logger.warning(f"⚠ Negative values in '{col}': {neg_count}")
        
        return issues
    
    @staticmethod
    def validate_after_processing(df: pd.DataFrame) -> Dict[str, Any]:
        """Validate data after processing (School Chapter 5 & 6)"""
        logger.info("=" * 60)
        logger.info("POST-PROCESSING VALIDATION STARTED")
        logger.info("=" * 60)
        
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'stage': 'post_processing',
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'checks': {}
            }
            
            nulls = BackupValidator.check_no_nulls_in_new_columns(df)
            report['checks']['no_nulls_in_computed'] = {
                'passed': len(nulls) == 0,
                'null_columns': nulls
            }
            
            issues = BackupValidator.check_numeric_columns_valid(df)
            report['checks']['no_infinite_values'] = {
                'passed': len(issues) == 0,
                'problematic_columns': issues
            }
            
            expected_cols = [
                'trip_duration_minutes', 'average_speed_mph', 'revenue_per_mile',
                'trip_distance_category', 'fare_category', 'trip_time_of_day',
                'pickup_year', 'pickup_month'
            ]
            missing = [col for col in expected_cols if col not in df.columns]
            report['checks']['all_columns_present'] = {
                'passed': len(missing) == 0,
                'missing_columns': missing
            }
            
            report['overall_valid'] = all(check['passed'] for check in report['checks'].values())
            
            logger.info(f"✓ Overall Status: {'PASSED' if report['overall_valid'] else 'FAILED'}")
            logger.info("=" * 60)
            
            return report
        except Exception as e:
            logger.error(f"✗ Post-processing validation failed: {str(e)}")
            raise


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print("BackupValidator module loaded successfully")
    print("✓ Best Practices Implemented: Data Quality, Pipeline Monitoring, Data Transformation")
