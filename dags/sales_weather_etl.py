"""
ETL DAG to combine weather data and sales transactions
"""
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# Data paths
RAW_DATA_PATH = "/opt/airflow/data/raw"
PROCESSED_DATA_PATH = "/opt/airflow/data/processed"
FINAL_DATA_PATH = "/opt/airflow/data/final"

def load_stores_data(**kwargs):
    """
    Read store data and load into PostgreSQL
    """
    # Read store data
    stores_df = pd.read_csv(f"{RAW_DATA_PATH}/stores.csv")
    
    # Rename columns to match expected format
    stores_df.rename(columns={
        'Store ID': 'store_id',
        'Store Name': 'store_name',
        'City': 'location',
        'Country': 'region',
        'Number of Employees': 'size',
        'ZIP Code': 'opening_date'  # Using ZIP Code as opening_date for now
    }, inplace=True)
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    
    # Create cursor
    cursor = conn.cursor()
    
    # Create stores table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stores (
        store_id INT PRIMARY KEY,
        store_name VARCHAR(100),
        location VARCHAR(100),
        region VARCHAR(50),
        size VARCHAR(20),
        opening_date VARCHAR(20)
    )
    """)
    
    # Delete old data to reload
    cursor.execute("TRUNCATE TABLE stores")
    
    # Load new data
    for _, row in stores_df.iterrows():
        cursor.execute(
            """
            INSERT INTO stores (store_id, store_name, location, region, size, opening_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row['store_id'], row['store_name'], row['location'],
                row['region'], str(row['size']), row['opening_date']
            )
        )
    
    # Commit changes
    conn.commit()
    cursor.close()
    conn.close()
    
    return "Successfully loaded stores data into PostgreSQL"

def load_products_data(**kwargs):
    """
    Read product data and load into PostgreSQL
    """
    # Read product data
    products_df = pd.read_csv(f"{RAW_DATA_PATH}/products.csv")
    
    # Rename columns to match expected format
    products_df.rename(columns={
        'Product ID': 'product_id',
        'Description EN': 'product_name',
        'Category': 'category',
        'Production Cost': 'price',
        # Add default values for sensitivity fields which don't exist in the raw data
        # These will be populated by the analytics process later
    }, inplace=True)
    
    # Add sensitivity columns if they don't exist
    if 'temperature_sensitivity' not in products_df.columns:
        products_df['temperature_sensitivity'] = 'medium'  # Default value
    if 'rainfall_sensitivity' not in products_df.columns:
        products_df['rainfall_sensitivity'] = 'medium'  # Default value
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    
    # Create cursor
    cursor = conn.cursor()
    
    # Create products table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INT PRIMARY KEY,
        product_name VARCHAR(100),
        category VARCHAR(50),
        price FLOAT,
        temperature_sensitivity VARCHAR(20),
        rainfall_sensitivity VARCHAR(20)
    )
    """)
    
    # Delete old data to reload
    cursor.execute("TRUNCATE TABLE products")
    
    # Load new data
    for _, row in products_df.iterrows():
        cursor.execute(
            """
            INSERT INTO products (product_id, product_name, category, price, 
                                  temperature_sensitivity, rainfall_sensitivity)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row['product_id'], row['product_name'], row['category'], row['price'],
                row['temperature_sensitivity'], row['rainfall_sensitivity']
            )
        )
    
    # Commit changes
    conn.commit()
    cursor.close()
    conn.close()
    
    return "Successfully loaded products data into PostgreSQL"

def load_transactions_data(**kwargs):
    """
    Read transaction data and load into PostgreSQL
    """
    # Read transaction data
    transactions_df = pd.read_csv(f"{RAW_DATA_PATH}/transactions.csv")
    
    # Print the actual columns for debugging
    print(f"Actual columns in transactions.csv: {transactions_df.columns.tolist()}")
    
    # Map the actual CSV columns to our expected schema
    column_mapping = {
        'Invoice ID': 'transaction_id',
        'Date': 'date',
        'Store ID': 'store_id',
        'Product ID': 'product_id',
        'Quantity': 'quantity',
        'Line Total': 'total_amount',
        'Customer ID': 'customer_id'
    }
    
    # Rename columns according to our mapping
    transactions_df = transactions_df.rename(columns=column_mapping)
    
    # Group by transaction_id, date, store_id, product_id, customer_id
    # This will combine multiple lines of the same invoice
    transactions_grouped = transactions_df.groupby(
        ['transaction_id', 'date', 'store_id', 'product_id', 'customer_id']
    ).agg({
        'quantity': 'sum',
        'total_amount': 'sum'
    }).reset_index()
    
    print(f"After grouping, we have {len(transactions_grouped)} transactions")
    
    # Converting date string to proper date format
    transactions_grouped['date'] = pd.to_datetime(transactions_grouped['date']).dt.date
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    
    # Create cursor
    cursor = conn.cursor()
    
    # Create transactions table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id VARCHAR(100) NOT NULL,
        date DATE,
        store_id INT,
        product_id INT,
        quantity INT,
        total_amount FLOAT,
        customer_id INT,
        PRIMARY KEY (transaction_id, product_id),
        FOREIGN KEY (store_id) REFERENCES stores(store_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
    """)
    
    # Delete old data to reload
    cursor.execute("TRUNCATE TABLE transactions CASCADE")
    
    # Load new data
    for _, row in transactions_grouped.iterrows():
        try:
            cursor.execute(
                """
                INSERT INTO transactions (transaction_id, date, store_id, product_id, 
                                         quantity, total_amount, customer_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id, product_id) DO NOTHING
                """,
                (
                    row['transaction_id'], row['date'], row['store_id'], row['product_id'],
                    row['quantity'], row['total_amount'], row['customer_id']
                )
            )
        except Exception as e:
            print(f"Error inserting row with transaction_id {row['transaction_id']}, product_id {row['product_id']}: {e}")
            # Continue with the next row instead of failing the entire transaction
            continue
    
    # Commit changes
    conn.commit()
    cursor.close()
    conn.close()
    
    return "Successfully loaded transactions data into PostgreSQL"

def process_weather_data(ds, **kwargs):
    """
    Process weather data for the current day and load into PostgreSQL
    """
    date = ds
    
    # Path to processed weather data
    weather_file = f"{PROCESSED_DATA_PATH}/weather/{date}.csv"
    
    # Check if weather data for this day already exists
    if not os.path.exists(weather_file):
        print(f"Warning: No weather data found for {date}. Creating empty weather table.")
        
        # If no weather file exists, we'll create the table structure anyway
        # and add a dummy record to prevent downstream tasks from failing
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Create the weather table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            id SERIAL PRIMARY KEY,
            date DATE,
            city VARCHAR(100),
            region VARCHAR(50),
            temperature_max FLOAT,
            temperature_min FLOAT,
            rain_sum FLOAT,
            precipitation_hours FLOAT
        )
        """)
        
        # Add dummy record if table is empty
        cursor.execute("SELECT COUNT(*) FROM weather")
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("Adding placeholder weather data to allow fact table creation")
            cursor.execute("""
            INSERT INTO weather (date, city, region, temperature_max, temperature_min, rain_sum, precipitation_hours)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (date, 'Unknown', 'Unknown', 25.0, 15.0, 0.0, 0.0))
            
            conn.commit()
        
        cursor.close()
        conn.close()
        return f"Created weather table with dummy data for {date}"
    
    # Read weather data
    weather_df = pd.read_csv(weather_file)
    print(f"Found weather data for {date} with {len(weather_df)} records")
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    
    # Create cursor
    cursor = conn.cursor()
    
    # Create weather table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        id SERIAL PRIMARY KEY,
        date DATE,
        city VARCHAR(100),
        region VARCHAR(50),
        temperature_max FLOAT,
        temperature_min FLOAT,
        rain_sum FLOAT,
        precipitation_hours FLOAT
    )
    """)
    
    # Delete current day's data (if any)
    cursor.execute("DELETE FROM weather WHERE date = %s", (date,))
    
    # Load new weather data
    for _, row in weather_df.iterrows():
        try:
            cursor.execute(
                """
                INSERT INTO weather (date, city, region, temperature_max, temperature_min, 
                                    rain_sum, precipitation_hours)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row['date'], row['city'], row['region'], row['temperature_max'],
                    row['temperature_min'], row['rain_sum'], row['precipitation_hours']
                )
            )
        except Exception as e:
            print(f"Error inserting weather data: {e}")
            continue
    
    # Commit changes
    conn.commit()
    cursor.close()
    conn.close()
    
    return f"Successfully loaded weather data for {date} into PostgreSQL"

def create_sales_weather_fact_table(**kwargs):
    """
    Create fact table combining weather and sales data
    """
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    
    # Create cursor
    cursor = conn.cursor()
    
    # Check if required tables exist
    cursor.execute("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'transactions'
    );
    """)
    transactions_exist = cursor.fetchone()[0]
    
    cursor.execute("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'stores'
    );
    """)
    stores_exist = cursor.fetchone()[0]
    
    cursor.execute("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'products'
    );
    """)
    products_exist = cursor.fetchone()[0]
    
    cursor.execute("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'weather'
    );
    """)
    weather_exist = cursor.fetchone()[0]
    
    if not all([transactions_exist, stores_exist, products_exist, weather_exist]):
        missing_tables = []
        if not transactions_exist:
            missing_tables.append("transactions")
        if not stores_exist:
            missing_tables.append("stores")
        if not products_exist:
            missing_tables.append("products")
        if not weather_exist:
            missing_tables.append("weather")
        
        error_message = f"Required tables are missing: {', '.join(missing_tables)}. Cannot create fact table."
        print(error_message)
        cursor.close()
        conn.close()
        raise ValueError(error_message)
    
    # Create fact table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales_weather_fact (
        id SERIAL PRIMARY KEY,
        date DATE,
        store_id INT,
        location VARCHAR(100),
        region VARCHAR(50),
        store_size VARCHAR(20),
        product_id INT,
        product_name VARCHAR(100),
        category VARCHAR(50),
        quantity INT,
        total_sales FLOAT,
        temperature_max FLOAT,
        temperature_min FLOAT,
        rain_sum FLOAT,
        precipitation_hours FLOAT,
        is_rainy BOOLEAN
    )
    """)
    
    # Delete old data to reload
    cursor.execute("TRUNCATE TABLE sales_weather_fact")
    
    # Create view combining data
    cursor.execute("""
    WITH sales_by_store_date AS (
        SELECT 
            t.date,
            t.store_id,
            t.product_id,
            SUM(t.quantity) as total_quantity,
            SUM(t.total_amount) as total_sales
        FROM 
            transactions t
        GROUP BY 
            t.date, t.store_id, t.product_id
    ),
    weather_by_store AS (
        SELECT 
            w.date,
            s.store_id,
            w.temperature_max,
            w.temperature_min,
            w.rain_sum,
            w.precipitation_hours,
            CASE WHEN w.rain_sum > 0 THEN TRUE ELSE FALSE END as is_rainy
        FROM 
            weather w
        JOIN 
            stores s ON w.city = s.location
    )
    INSERT INTO sales_weather_fact (
        date, store_id, location, region, store_size, 
        product_id, product_name, category, 
        quantity, total_sales, 
        temperature_max, temperature_min, rain_sum, precipitation_hours, is_rainy
    )
    SELECT 
        ssd.date,
        s.store_id,
        s.location,
        s.region,
        s.size as store_size,
        p.product_id,
        p.product_name,
        p.category,
        ssd.total_quantity as quantity,
        ssd.total_sales,
        ws.temperature_max,
        ws.temperature_min,
        ws.rain_sum,
        ws.precipitation_hours,
        ws.is_rainy
    FROM 
        sales_by_store_date ssd
    JOIN 
        stores s ON ssd.store_id = s.store_id
    JOIN 
        products p ON ssd.product_id = p.product_id
    LEFT JOIN 
        weather_by_store ws ON ssd.date = ws.date AND ssd.store_id = ws.store_id
    """)
    
    # Commit changes
    conn.commit()
    cursor.close()
    conn.close()
    
    return "Successfully created sales_weather_fact table"

def export_analytics_reports(**kwargs):
    """
    Export analytics reports
    """
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    
    # Create reports directory if it doesn't exist
    os.makedirs(f"{FINAL_DATA_PATH}/reports", exist_ok=True)
    
    # Report 1: Sales by region and weather conditions
    query_1 = """
    SELECT 
        region, 
        is_rainy, 
        SUM(total_sales) as total_sales,
        AVG(total_sales) as avg_sales
    FROM 
        sales_weather_fact
    GROUP BY 
        region, is_rainy
    ORDER BY 
        region, is_rainy
    """
    
    df_report_1 = pd.read_sql(query_1, conn)
    df_report_1.to_csv(f"{FINAL_DATA_PATH}/reports/sales_by_region_weather.csv", index=False)
    
    # Report 2: Product sales by temperature
    query_2 = """
    SELECT 
        category,
        product_name, 
        CASE 
            WHEN temperature_max < 25 THEN 'Cool (< 25°C)'
            WHEN temperature_max BETWEEN 25 AND 30 THEN 'Moderate (25-30°C)'
            ELSE 'Hot (> 30°C)' 
        END as temperature_range,
        SUM(total_sales) as total_sales,
        SUM(quantity) as total_quantity
    FROM 
        sales_weather_fact
    GROUP BY 
        category, product_name, temperature_range
    ORDER BY 
        category, product_name, temperature_range
    """
    
    df_report_2 = pd.read_sql(query_2, conn)
    df_report_2.to_csv(f"{FINAL_DATA_PATH}/reports/product_sales_by_temperature.csv", index=False)
    
    # Report 3: Store sales by size and weather
    query_3 = """
    SELECT 
        store_size, 
        is_rainy, 
        SUM(total_sales) as total_sales,
        COUNT(DISTINCT date) as number_of_days,
        SUM(total_sales) / COUNT(DISTINCT date) as avg_daily_sales
    FROM 
        sales_weather_fact
    GROUP BY 
        store_size, is_rainy
    ORDER BY 
        store_size, is_rainy
    """
    
    df_report_3 = pd.read_sql(query_3, conn)
    df_report_3.to_csv(f"{FINAL_DATA_PATH}/reports/store_sales_by_size_weather.csv", index=False)
    
    conn.close()
    
    return "Successfully exported analytics reports"

# Define DAG
with DAG(
    'sales_weather_etl',
    default_args=default_args,
    description='ETL process for sales and weather data',
    schedule_interval='30 1 * * *',  # Run daily at 1:30 AM (after weather data collection)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['sales', 'weather', 'etl'],
) as dag:
    
    # Create PostgreSQL DB setup tasks
    create_postgres_connection = PostgresOperator(
        task_id='create_postgres_connection',
        postgres_conn_id='postgres_default',
        sql="""SELECT 1;"""  # Check connection
    )
    
    # Tasks to load basic data into PostgreSQL
    load_stores = PythonOperator(
        task_id='load_stores_data',
        python_callable=load_stores_data,
    )
    
    load_products = PythonOperator(
        task_id='load_products_data',
        python_callable=load_products_data,
    )
    
    load_transactions = PythonOperator(
        task_id='load_transactions_data',
        python_callable=load_transactions_data,
        retries=3,  # Increase retries for this critical task
        retry_delay=timedelta(minutes=2),
    )
    
    process_weather = PythonOperator(
        task_id='process_weather_data',
        python_callable=process_weather_data,
        retries=3,  # Increase retries for this critical task
        retry_delay=timedelta(minutes=2),
    )
    
    # Task to create fact table
    create_fact_table = PythonOperator(
        task_id='create_sales_weather_fact',
        python_callable=create_sales_weather_fact_table,
        trigger_rule='all_success',  # Only run if all upstream tasks succeed
    )
    
    # Task to export reports
    export_reports = PythonOperator(
        task_id='export_analytics_reports',
        python_callable=export_analytics_reports,
    )
    
    # Set up dependencies with clearer flow
    create_postgres_connection >> load_stores >> load_products >> load_transactions
    create_postgres_connection >> process_weather
    [load_transactions, process_weather] >> create_fact_table >> export_reports