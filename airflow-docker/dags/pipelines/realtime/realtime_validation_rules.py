import pandas as pd
from typing import Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class RealtimeValidationRules:
    """Validation rules for E-Commerce real-time data"""
    
    MANDATORY_COLUMNS = [
        'order_id', 'customer_id', 'product_name', 'quantity', 
        'unit_price', 'order_date', 'status', 'country'
    ]
    
    @staticmethod
    def validate_all(df: pd.DataFrame) -> Dict[str, Any]:
        """Run all validation checks on incoming raw data."""
        logger.info("=" * 80)
        logger.info(f"[{datetime.now().isoformat()}] REALTIME VALIDATION - E-COMMERCE DATA")
        logger.info("=" * 80)
        
        checks = {}
        
        checks['mandatory_columns'] = RealtimeValidationRules.check_mandatory_columns(df)
        checks['duplicates'] = RealtimeValidationRules.check_duplicates(df)
        checks['null_values'] = RealtimeValidationRules.check_nulls(df)
        checks['numeric_ranges'] = RealtimeValidationRules.check_numeric_ranges(df)
        checks['date_validity'] = RealtimeValidationRules.check_date_validity(df)
        
        all_passed = all(check['passed'] for check in checks.values())
        
        logger.info(f"\n VALIDATION SUMMARY:")
        for check_name, result in checks.items():
            status = "PASS" if result['passed'] else "FAIL"
            logger.info(f"  {status}: {check_name}")
        
        logger.info(f"\n{'ALL CHECKS PASSED' if all_passed else 'SOME CHECKS FAILED'}")
        logger.info("=" * 80)
        
        return {
            'timestamp': datetime.now().isoformat(),
            'total_rows': len(df),
            'checks': checks,
            'overall_valid': all_passed
        }
    
    @staticmethod
    def check_mandatory_columns(df: pd.DataFrame) -> Dict[str, Any]:
        """Check whether all required columns are present."""
        missing = [col for col in RealtimeValidationRules.MANDATORY_COLUMNS if col not in df.columns]
        return {
            'passed': bool(len(missing) == 0),
            'missing_columns': missing,
            'message': f"All {len(RealtimeValidationRules.MANDATORY_COLUMNS)} mandatory columns present"
        }
    
    @staticmethod
    def check_duplicates(df: pd.DataFrame) -> Dict[str, Any]:
        """Check for duplicate rows in raw input."""
        duplicate_count = df.duplicated().sum()
        return {
            'passed': bool(duplicate_count == 0),
            'duplicate_rows': int(duplicate_count),
            'message': f"Found {duplicate_count} duplicate rows"
        }
    
    @staticmethod
    def check_nulls(df: pd.DataFrame) -> Dict[str, Any]:
        """Check null values in mandatory columns."""
        null_counts = {}
        for col in RealtimeValidationRules.MANDATORY_COLUMNS:
            if col in df.columns:
                nulls = df[col].isnull().sum()
                if nulls > 0:
                    null_counts[col] = int(nulls)
        
        return {
            'passed': bool(len(null_counts) == 0),
            'null_columns': null_counts,
            'message': f"Found NULLs in {len(null_counts)} columns"
        }
    
    @staticmethod
    def check_numeric_ranges(df: pd.DataFrame) -> Dict[str, Any]:
        """Check numeric columns for expected value ranges."""
        issues = []
        
        if 'quantity' in df.columns:
            invalid = ((df['quantity'] < 0) | (df['quantity'] > 1000)).sum()
            if invalid > 0:
                issues.append(f"quantity out of range: {invalid} rows")
        
        if 'unit_price' in df.columns:
            if (df['unit_price'] < 0).any():
                issues.append(f"negative unit_price: {(df['unit_price'] < 0).sum()} rows")
        
        if 'order_total' in df.columns:
            if (df['order_total'] < 0).any():
                issues.append(f"negative order_total: {(df['order_total'] < 0).sum()} rows")
        
        return {
            'passed': bool(len(issues) == 0),
            'issues': issues,
            'message': f"Found {len(issues)} numeric range issues"
        }
    
    @staticmethod
    def check_date_validity(df: pd.DataFrame) -> Dict[str, Any]:
        """Check date columns can be parsed correctly."""
        issues = []
        
        if 'order_date' in df.columns:
            try:
                pd.to_datetime(df['order_date'])
            except Exception as e:
                issues.append(f"Invalid date format: {str(e)}")
        
        return {
            'passed': bool(len(issues) == 0),
            'issues': issues,
            'message': f"Date validity check complete"
        }
