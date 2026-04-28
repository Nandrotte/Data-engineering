"""Quick Validation After Processing - Backup Check"""

import pandas as pd
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class BackupValidator:
    """Quick validation after processing - ensure processing didn't break data"""
    
    @staticmethod
    def check_no_nulls_in_new_columns(df: pd.DataFrame) -> Dict[str, int]:
        """Check that school-required columns have no NULLs"""
        # School Part 1.2 required columns
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
                    nulls[col] = null_count
        
        return nulls
    
    @staticmethod
    def check_numeric_columns_valid(df: pd.DataFrame) -> Dict[str, Any]:
        """Check that numeric columns don't have infinite values (School Part 1.2)"""
        # School-required numeric columns that need validation
        numeric_cols = ['average_speed_mph', 'revenue_per_mile', 'trip_duration_minutes']
        
        issues = {}
        for col in numeric_cols:
            if col in df.columns:
                inf_count = df[col].isin([float('inf'), float('-inf')]).sum()
                if inf_count > 0:
                    issues[col] = inf_count
        
        return issues
    
    @staticmethod
    def validate_after_processing(df: pd.DataFrame) -> Dict[str, Any]:
        """Quick backup validation after processing"""
        logger.info("Running backup validation...")
        
        report = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'checks': {}
        }
        
        # Check 1: No NULLs in new columns
        nulls = BackupValidator.check_no_nulls_in_new_columns(df)
        report['checks']['no_nulls_in_computed'] = {
            'passed': len(nulls) == 0,
            'null_columns': nulls
        }
        
        # Check 2: No infinite values
        issues = BackupValidator.check_numeric_columns_valid(df)
        report['checks']['no_infinite_values'] = {
            'passed': len(issues) == 0,
            'problematic_columns': issues
        }
        
        # Check 3: All expected columns present (School Part 1.2 requirements)
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
        
        # Overall result
        report['overall_valid'] = all(check['passed'] for check in report['checks'].values())
        
        return report


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print("BackupValidator module loaded successfully")
