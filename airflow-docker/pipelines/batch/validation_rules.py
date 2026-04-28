"""Validation Rules - 5 data quality checks"""

import pandas as pd
from typing import Tuple, List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class ValidationRules:
    """5 comprehensive data quality checks"""
    
    # The 11 mandatory columns that MUST be present
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
        
        # Check if pickup < dropoff
        if 'tpep_pickup_datetime' in df.columns and 'tpep_dropoff_datetime' in df.columns:
            invalid = (df['tpep_pickup_datetime'] > df['tpep_dropoff_datetime']).sum()
            if invalid > 0:
                issues.append(f"Pickup > dropoff: {invalid} rows")
        
        # Check date ranges (2024-2026)
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
        
        # passenger_count: 1-9
        if 'passenger_count' in df.columns:
            invalid = ((df['passenger_count'] < 1) | (df['passenger_count'] > 9)).sum()
            if invalid > 0:
                issues.append(f"passenger_count out of range: {invalid} rows")
        
        # trip_distance: 0-200
        if 'trip_distance' in df.columns:
            if (df['trip_distance'] < 0).any():
                issues.append(f"Negative distance: {(df['trip_distance'] < 0).sum()} rows")
            if (df['trip_distance'] > 200).any():
                issues.append(f"Distance > 200: {(df['trip_distance'] > 200).sum()} rows")
        
        # Amounts: must be positive
        if 'fare_amount' in df.columns and (df['fare_amount'] < 0).any():
            issues.append(f"Negative fare: {(df['fare_amount'] < 0).sum()} rows")
        
        if 'total_amount' in df.columns and (df['total_amount'] < 0).any():
            issues.append(f"Negative total: {(df['total_amount'] < 0).sum()} rows")
        
        # total >= fare
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
        """Run all 5 checks and return report"""
        logger.info("Running all validation checks...")
        
        report = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'checks': {}
        }
        
        # Check 1
        passed, missing = ValidationRules.check_1_mandatory_columns(df)
        report['checks']['1_mandatory_columns'] = {
            'passed': passed,
            'missing': missing
        }
        
        # Check 2
        passed, count = ValidationRules.check_2_duplicates(df)
        report['checks']['2_duplicates'] = {
            'passed': passed,
            'duplicate_count': count
        }
        
        # Check 3
        passed, issues = ValidationRules.check_3_datetime_validity(df)
        report['checks']['3_datetime_validity'] = {
            'passed': passed,
            'issues': issues
        }
        
        # Check 4
        passed, issues = ValidationRules.check_4_numeric_ranges(df)
        report['checks']['4_numeric_ranges'] = {
            'passed': passed,
            'issues': issues
        }
        
        # Check 5
        passed, nulls = ValidationRules.check_5_null_values(df)
        report['checks']['5_null_values'] = {
            'passed': passed,
            'null_columns': nulls
        }
        
        # Overall result
        report['overall_valid'] = all(check['passed'] for check in report['checks'].values())
        
        return report


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print("ValidationRules module loaded successfully")
