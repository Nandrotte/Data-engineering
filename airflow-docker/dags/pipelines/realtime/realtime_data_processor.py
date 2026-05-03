import pandas as pd
from typing import Tuple, Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class RealtimeDataProcessor:
    """Process E-Commerce data with transformations"""
    
    @staticmethod
    def process_all(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Apply cleaning and feature transformations."""
        execution_start = datetime.now()
        logger.info("=" * 80)
        logger.info(f"[{execution_start.isoformat()}] REALTIME DATA PROCESSING")
        logger.info("=" * 80)
        
        logger.info(f"\n INPUT DATA:")
        logger.info(f"  Rows: {len(df):,}")
        logger.info(f"  Columns: {len(df.columns)}")
        
        df_processed = df.copy()
        
        # Process in a fixed order: dedupe first, then cleanup, then enrichment.
        df_processed = RealtimeDataProcessor.remove_duplicates(df_processed)
        df_processed = RealtimeDataProcessor.clean_numeric_values(df_processed)
        df_processed = RealtimeDataProcessor.add_order_profit_margin(df_processed)
        df_processed = RealtimeDataProcessor.add_customer_segment(df_processed)
        df_processed = RealtimeDataProcessor.add_shipment_status(df_processed)
        df_processed = RealtimeDataProcessor.add_order_value_category(df_processed)
        
        execution_time = (datetime.now() - execution_start).total_seconds()
        
        logger.info(f"\n OUTPUT DATA:")
        logger.info(f"  Rows: {len(df_processed):,}")
        logger.info(f"  Columns: {len(df_processed.columns)}")
        logger.info(f"\n Execution time: {execution_time:.2f}s")
        logger.info("=" * 80)
        
        report = {
            'timestamp': execution_start.isoformat(),
            'execution_time_seconds': execution_time,
            'input_rows': len(df),
            'output_rows': len(df_processed),
            'input_columns': len(df.columns),
            'output_columns': len(df_processed.columns),
            'columns_added': ['profit_margin', 'customer_segment', 'shipment_status', 'order_value_category'],
            'status': 'success'
        }
        
        return df_processed, report
    
    @staticmethod
    def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows based on order_id."""
        before = len(df)
        df = df.drop_duplicates(subset=['order_id'], keep='first')
        after = len(df)
        
        if before > after:
            logger.info(f"  Removed {before - after} duplicate rows")
        
        return df
    
    @staticmethod
    def clean_numeric_values(df: pd.DataFrame) -> pd.DataFrame:
        """Clamp invalid numeric values to safe defaults."""
        if 'quantity' in df.columns:
            df.loc[df['quantity'] < 0, 'quantity'] = 0
            df.loc[df['quantity'] > 1000, 'quantity'] = 0
        
        if 'unit_price' in df.columns:
            df.loc[df['unit_price'] < 0, 'unit_price'] = 0
        
        logger.info("Cleaned numeric values")
        return df
    
    @staticmethod
    def add_order_profit_margin(df: pd.DataFrame) -> pd.DataFrame:
        """ADD: profit_margin % based on order"""
        if 'unit_price' in df.columns and 'order_total' in df.columns:
            df['profit_margin'] = (df['order_total'] / (df['unit_price'] * df['quantity'] + 0.01) * 100).round(2)
            df['profit_margin'] = df['profit_margin'].replace([float('inf'), float('-inf')], 0)
            logger.info("Added column: profit_margin")
        
        return df
    
    @staticmethod
    def add_customer_segment(df: pd.DataFrame) -> pd.DataFrame:
        """ADD: customer_segment based on order value"""
        if 'order_total' in df.columns:
            df['customer_segment'] = pd.cut(
                df['order_total'],
                bins=[0, 50, 200, float('inf')],
                labels=['Economy', 'Standard', 'Premium']
            )
            logger.info("Added column: customer_segment (Economy/Standard/Premium)")
        
        return df
    
    @staticmethod
    def add_shipment_status(df: pd.DataFrame) -> pd.DataFrame:
        """ADD: shipment_status based on status"""
        if 'status' in df.columns:
            def map_shipment_status(status):
                if status in ['Cancelled']:
                    return 'Cancelled'
                elif status in ['Pending', 'Processing']:
                    return 'In Progress'
                elif status in ['Shipped']:
                    return 'Shipped'
                else:
                    return 'Completed'
            
            df['shipment_status'] = df['status'].apply(map_shipment_status)
            logger.info("Added column: shipment_status")
        
        return df
    
    @staticmethod
    def add_order_value_category(df: pd.DataFrame) -> pd.DataFrame:
        """ADD: order_value_category"""
        if 'order_total' in df.columns:
            df['order_value_category'] = pd.cut(
                df['order_total'],
                bins=[0, 100, 500, float('inf')],
                labels=['Small', 'Medium', 'Large']
            )
            logger.info("Added column: order_value_category (Small/Medium/Large)")
        
        return df
