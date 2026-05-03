import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import random

np.random.seed(42)
random.seed(42)

def generate_ecommerce_dataset(n_rows=500):
    """Generate unclean E-Commerce dataset"""
    
    dates = [datetime.now() - timedelta(days=x) for x in range(90)]
    
    data = {
        'order_id': [f'ORD{str(i).zfill(6)}' for i in range(1, n_rows + 1)],
        'customer_id': np.random.choice([f'CUST{str(i).zfill(5)}' for i in range(1, 200)], n_rows),
        'product_name': np.random.choice([
            'Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones', 
            'USB Cable', 'Phone', 'Tablet', 'Case', 'Screen'
        ], n_rows),
        'quantity': np.random.randint(1, 10, n_rows),
        'unit_price': np.round(np.random.uniform(5, 1500, n_rows), 2),
        'order_date': np.random.choice(dates, n_rows),
        'status': np.random.choice([
            'Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled'
        ], n_rows),
        'country': np.random.choice([
            'USA', 'UK', 'Germany', 'France', 'Netherlands', 'Belgium'
        ], n_rows),
        'customer_email': [f'cust{i}@example.com' for i in range(1, n_rows + 1)],
        'payment_method': np.random.choice([
            'Credit Card', 'PayPal', 'Bank Transfer', 'Bitcoin'
        ], n_rows),
        'shipping_cost': np.round(np.random.uniform(0, 50, n_rows), 2),
    }
    
    df = pd.DataFrame(data)
    
    # Add a derived monetary column used in validation and processing.
    df['order_total'] = df['quantity'] * df['unit_price'] + df['shipping_cost']
    
    # Introduce NULLs
    nulls_indices = np.random.choice(df.index, size=int(0.05 * n_rows), replace=False)
    df.loc[nulls_indices[:len(nulls_indices)//2], 'customer_email'] = None
    df.loc[nulls_indices[len(nulls_indices)//2:], 'shipping_cost'] = None
    
    # Add duplicates
    duplicate_indices = np.random.choice(df.index, size=20, replace=False)
    df = pd.concat([df, df.loc[duplicate_indices]], ignore_index=True)
    
    # Inject invalid numeric values to test pipeline robustness.
    inconsistent_indices = np.random.choice(df.index, size=15, replace=False)
    for idx in inconsistent_indices:
        df.loc[idx, 'quantity'] = np.random.choice([-1, 0, 100])
    
    # Shuffle dataset
    df = df.sample(frac=1).reset_index(drop=True)
    
    return df


if __name__ == '__main__':
    project_dir = Path(__file__).resolve().parent
    output_path = project_dir / 'data_input_realtime'

    if not output_path.exists():
        raise FileNotFoundError(
            f"Expected existing folder not found: {output_path}"
        )
    
    df = generate_ecommerce_dataset(500)
    
    # Use a timestamped filename so each generated file is unique.
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file = output_path / f'ecommerce_orders_{timestamp}.csv'
    
    df.to_csv(csv_file, index=False)
    
    print(f"Generated E-Commerce dataset: {len(df)} rows")
    print(f"Saved to: {csv_file}")
    print(f"Columns: {list(df.columns)}")
    print("Unclean data features:")
    print(f"  - NULLs in customer_email and shipping_cost")
    print(f"  - 20 duplicate rows")
    print(f"  - 15 inconsistent quantity values")
    print(f"  - Total rows: {len(df)}")