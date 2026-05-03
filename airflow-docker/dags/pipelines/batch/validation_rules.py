"""
Validation Rules - UCLL Best Practice Implementation
================================================================================
5 comprehensive data quality checks for Yellow Taxi data

School References:
  - Chapter 6: Data Quality & Validation Framework
  - Chapter 3: Data Transformation principles

This module implements structured validation reporting with:
  - Detailed check descriptions
  - Quantitative impact analysis
  - Type hints and comprehensive error handling
  - Logging with timestamps and visual formatting

School Requirements: Data Engineering Project - Part 1 (Data Quality)
Author: Data Engineering Team
Created: April 28, 2026
Last Updated: April 29, 2026 (Best Practices Implementation)
"""

import pandas as pd
from typing import Tuple, List, Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ValidationRules:
    """5 comprehensive data quality checks"""
    
    MANDATORY_COLUMNS = [
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'PULocationID',
        'DOLocationID',
        'payment_type',
        'fare_amount',
        'total_amount'
    ]
    
    @staticmethod
    def check_1_mandatory_columns(df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """CHECK 1: Are all mandatory columns present?"""
        missing = [col for col in ValidationRules.MANDATORY_COLUMNS if col not in df.columns]
        return len(missing) == 0, missing
    
    @staticmethod
    def check_2_duplicates(df: pd.DataFrame) -> Tuple[bool, int]:
        """CHECK 2: Are there duplicate rows?"""
        duplicates = df.duplicated().sum()
        return duplicates == 0, duplicates
    
    @staticmethod
    def check_3_datetime_validity(df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """CHECK 3: Are datetime columns valid?"""
        issues = []
        
        if 'tpep_pickup_datetime' in df.columns and 'tpep_dropoff_datetime' in df.columns:
            invalid = (df['tpep_pickup_datetime'] > df['tpep_dropoff_datetime']).sum()
            if invalid > 0:
                issues.append(f"Pickup > dropoff: {invalid} rows")
        
        if 'tpep_pickup_datetime' in df.columns:
            year_min = df['tpep_pickup_datetime'].dt.year.min()
            year_max = df['tpep_pickup_datetime'].dt.year.max()
            if year_min < 2024 or year_max > 2026:
                issues.append(f"Year out of range: {year_min}-{year_max}")
        
        return len(issues) == 0, issues
    
    @staticmethod
    def check_4_numeric_ranges(df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """CHECK 4: Are numeric values realistic?"""
        issues = []
        
        if 'passenger_count' in df.columns:
            invalid = ((df['passenger_count'] < 1) | (df['passenger_count'] > 9)).sum()
            if invalid > 0:
                issues.append(f"passenger_count out of range: {invalid} rows")
        
        if 'trip_distance' in df.columns:
            if (df['trip_distance'] < 0).any():
                issues.append(f"Negative distance: {(df['trip_distance'] < 0).sum()} rows")
            if (df['trip_distance'] > 200).any():
                issues.append(f"Distance > 200: {(df['trip_distance'] > 200).sum()} rows")
        
        if 'fare_amount' in df.columns and (df['fare_amount'] < 0).any():
            issues.append(f"Negative fare: {(df['fare_amount'] < 0).sum()} rows")
        
        if 'total_amount' in df.columns and (df['total_amount'] < 0).any():
            issues.append(f"Negative total: {(df['total_amount'] < 0).sum()} rows")
        
        if 'total_amount' in df.columns and 'fare_amount' in df.columns:
            invalid = (df['total_amount'] < df['fare_amount']).sum()
            if invalid > 0:
                issues.append(f"Total < fare: {invalid} rows")
        
        return len(issues) == 0, issues
    
    @staticmethod
    def check_5_null_values(df: pd.DataFrame) -> Tuple[bool, Dict[str, int]]:
        """CHECK 5: Are there NULL values in mandatory columns?"""
        nulls = {}
        for col in ValidationRules.MANDATORY_COLUMNS:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    nulls[col] = null_count
        
        return len(nulls) == 0, nulls
    
    @staticmethod
    def validate_all(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Run all 5 checks and return comprehensive validation report
        
        Reference: Chapter 6 - Data Quality Framework
        
        Returns:
            Dict with detailed validation report including:
              - timestamp: ISO format execution time
              - stage: 'validation'
              - total_rows: number of rows validated
              - total_columns: number of columns validated
              - checks: detailed results for each check
              - overall_valid: boolean indicating if all checks passed
              - validation_passed: overall success flag
        """
        execution_start = datetime.now()
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"[{execution_start.isoformat()}] VALIDATION RULES - Running all 5 checks")
        logger.info("=" * 80)
        
        report = {
            'timestamp': execution_start.isoformat(),
            'stage': 'validation',
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'checks': {}
        }
        
        logger.info("\n[CHECK 1] Mandatory Columns Presence")
        passed_1, missing = ValidationRules.check_1_mandatory_columns(df)
        report['checks']['1_mandatory_columns'] = {
            'passed': passed_1,
            'missing': missing,
            'description': 'Verify all 9 mandatory columns are present'
        }
        logger.info(f"  Status: {'✓ PASS' if passed_1 else '✗ FAIL'}")
        if not passed_1:
            logger.info(f"  Missing columns: {missing}")
        
        logger.info("\n[CHECK 2] Duplicate Row Detection")
        passed_2, dup_count = ValidationRules.check_2_duplicates(df)
        report['checks']['2_duplicates'] = {
            'passed': passed_2,
            'duplicate_count': dup_count,
            'description': 'Detect duplicate rows in dataset'
        }
        logger.info(f"  Status: {'✓ PASS' if passed_2 else '✗ FAIL'}")
        logger.info(f"  Duplicate rows found: {dup_count:,}")
        
        logger.info("\n[CHECK 3] Datetime Validity & Ranges")
        passed_3, issues_3 = ValidationRules.check_3_datetime_validity(df)
        report['checks']['3_datetime_validity'] = {
            'passed': passed_3,
            'issues': issues_3,
            'description': 'Validate datetime columns and date ranges (2024-2026)'
        }
        logger.info(f"  Status: {'✓ PASS' if passed_3 else '✗ FAIL'}")
        if not passed_3:
            for issue in issues_3:
                logger.info(f"  Issue: {issue}")
        
        logger.info("\n[CHECK 4] Numeric Values & Ranges")
        passed_4, issues_4 = ValidationRules.check_4_numeric_ranges(df)
        report['checks']['4_numeric_ranges'] = {
            'passed': passed_4,
            'issues': issues_4,
            'description': 'Validate numeric columns are within realistic ranges'
        }
        logger.info(f"  Status: {'✓ PASS' if passed_4 else '✗ FAIL'}")
        if not passed_4:
            for issue in issues_4:
                logger.info(f"  Issue: {issue}")
        
        logger.info("\n[CHECK 5] NULL Value Detection")
        passed_5, nulls = ValidationRules.check_5_null_values(df)
        report['checks']['5_null_values'] = {
            'passed': passed_5,
            'null_columns': nulls,
            'description': 'Check for NULL values in mandatory columns'
        }
        logger.info(f"  Status: {'✓ PASS' if passed_5 else '✗ FAIL'}")
        if not passed_5:
            for col, count in nulls.items():
                logger.info(f"  {col}: {count:,} NULL values")
        
        all_passed = all(check['passed'] for check in report['checks'].values())
        report['overall_valid'] = all_passed
        report['validation_passed'] = all_passed
        
        logger.info("\n" + "=" * 80)
        logger.info(f"VALIDATION RESULT: {'✓ ALL CHECKS PASSED' if all_passed else '✗ SOME CHECKS FAILED'}")
        logger.info("=" * 80)
        
        return report


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print("ValidationRules module loaded successfully")
