"""
DAG for analyzing and providing insights about the relationship between weather and sales revenue
"""
from datetime import datetime, timedelta
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Setup style for matplotlib
plt.style.use('ggplot')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Data paths
FINAL_DATA_PATH = "/opt/airflow/data/final"
INSIGHTS_PATH = f"{FINAL_DATA_PATH}/insights"

def analyze_weather_impact_by_region(**kwargs):
    """
    Analyze the impact of weather on sales by region
    """
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    
    # Query data
    query = """
    SELECT 
        region,
        date,
        SUM(total_sales) as daily_sales,
        AVG(temperature_max) as avg_max_temp,
        AVG(temperature_min) as avg_min_temp,
        AVG(rain_sum) as avg_rain,
        BOOL_OR(is_rainy) as had_rain
    FROM 
        sales_weather_fact
    GROUP BY 
        region, date
    ORDER BY 
        region, date
    """
    
    # Read data into DataFrame
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Print the data for debugging
    print(f"DataFrame columns: {df.columns.tolist()}")
    print(f"DataFrame had_rain unique values: {df['had_rain'].unique()}")
    print(f"Region unique values: {df['region'].unique()}")
    
    # Create directory for insights if it doesn't exist
    os.makedirs(INSIGHTS_PATH, exist_ok=True)
    
    # Analyze rain impact on sales by region
    rain_impact = df.groupby(['region', 'had_rain'])['daily_sales'].agg(['mean', 'sum', 'count']).reset_index()
    rain_impact.columns = ['Region', 'Had Rain', 'Average Daily Sales', 'Total Sales', 'Number of Days']
    
    # Save raw analysis before pivoting for safety
    rain_impact.to_csv(f"{INSIGHTS_PATH}/weather_impact_by_region.csv", index=False)
    
    # Check if we have both True and False values in the 'Had Rain' column
    has_true = any(rain_impact['Had Rain'] == True)
    has_false = any(rain_impact['Had Rain'] == False)
    
    # Calculate percentage change in sales on rainy days compared to non-rainy days
    rain_impact_pivot = rain_impact.pivot(index='Region', columns='Had Rain', values='Average Daily Sales')
    
    # Add column names for clarity
    if True in rain_impact_pivot.columns and False in rain_impact_pivot.columns:
        rain_impact_pivot = rain_impact_pivot.rename(columns={True: 'Rainy', False: 'Not Rainy'})
    
    # Now we safely attempt to calculate the percent change only if both columns exist
    if 'Rainy' in rain_impact_pivot.columns and 'Not Rainy' in rain_impact_pivot.columns:
        rain_impact_pivot['Sales Change (%)'] = (rain_impact_pivot['Rainy'] - rain_impact_pivot['Not Rainy']) / rain_impact_pivot['Not Rainy'] * 100
    else:
        # If we don't have both rainy and non-rainy data, create an empty column
        rain_impact_pivot['Sales Change (%)'] = float('nan')
        print("Warning: Can't calculate sales change percentage - missing either rainy or non-rainy data")
    
    # Create visualization (only if we have data to show)
    if not rain_impact.empty and 'Had Rain' in rain_impact.columns:
        plt.figure(figsize=(10, 6))
        try:
            sns.barplot(x='Region', y='Average Daily Sales', hue='Had Rain', data=rain_impact)
            plt.title('Average Daily Sales by Region and Rain Condition')
            plt.xlabel('Region')
            plt.ylabel('Average Daily Sales')
            plt.tight_layout()
            plt.savefig(f"{INSIGHTS_PATH}/sales_by_region_rain.png")
        except Exception as e:
            print(f"Error creating barplot: {e}")
    
    # Create JSON file with key insights
    insights = {
        "title": "Weather Impact on Sales by Region",
        "findings": []
    }
    
    for region in rain_impact_pivot.index:
        # Check if we have the sales change column and it has valid data for this region
        if 'Sales Change (%)' in rain_impact_pivot.columns and not pd.isna(rain_impact_pivot.loc[region, 'Sales Change (%)']):
            change_pct = rain_impact_pivot.loc[region, 'Sales Change (%)']
            impact = "increase" if change_pct > 0 else "decrease"
            insights["findings"].append({
                "region": region,
                "impact": f"Sales {impact} by {abs(change_pct):.2f}% on rainy days",
                "average_sales_no_rain": float(rain_impact_pivot.loc[region, 'Not Rainy']) if 'Not Rainy' in rain_impact_pivot.columns and not pd.isna(rain_impact_pivot.loc[region, 'Not Rainy']) else None,
                "average_sales_with_rain": float(rain_impact_pivot.loc[region, 'Rainy']) if 'Rainy' in rain_impact_pivot.columns and not pd.isna(rain_impact_pivot.loc[region, 'Rainy']) else None
            })
        else:
            # Add a basic entry if we can't calculate change
            insights["findings"].append({
                "region": region,
                "impact": "Insufficient data to determine rain impact",
                "note": "Missing data for either rainy or non-rainy conditions"
            })
    
    # Save insights as JSON
    with open(f"{INSIGHTS_PATH}/weather_impact_by_region_insights.json", 'w') as f:
        json.dump(insights, f, indent=4)
    
    return "Successfully analyzed weather impact by region"

def analyze_product_sensitivity(**kwargs):
    """
    Analyze product sensitivity to weather conditions
    """
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    
    # Query data
    query = """
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        p.temperature_sensitivity,
        p.rainfall_sensitivity,
        swf.date,
        swf.is_rainy,
        swf.temperature_max,
        SUM(swf.quantity) as total_quantity,
        SUM(swf.total_sales) as total_sales
    FROM 
        sales_weather_fact swf
    JOIN 
        products p ON swf.product_id = p.product_id
    GROUP BY 
        p.product_id, p.product_name, p.category, p.temperature_sensitivity, 
        p.rainfall_sensitivity, swf.date, swf.is_rainy, swf.temperature_max
    """
    
    # Read data into DataFrame
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Print the data for debugging
    print(f"Product sensitivity DataFrame is_rainy unique values: {df['is_rainy'].unique()}")
    print(f"Temperature max data info: {df['temperature_max'].describe()}")
    print(f"Temperature max null count: {df['temperature_max'].isnull().sum()}")
    
    # Create insights directory if it doesn't exist
    os.makedirs(INSIGHTS_PATH, exist_ok=True)

    # Filter out rows with null temperature_max values before applying pd.cut
    temperature_df = df.copy()
    temperature_df = temperature_df.dropna(subset=['temperature_max'])
    
    if len(temperature_df) > 0:
        # Categorize temperature only for non-null values
        temperature_df['temperature_range'] = pd.cut(
            temperature_df['temperature_max'], 
            bins=[0, 25, 30, 100], 
            labels=['Cool (< 25°C)', 'Moderate (25-30°C)', 'Hot (> 30°C)']
        )
        
        # Add a default category for rows with null temperature_max in the original df
        df['temperature_range'] = 'Unknown'
        
        # Update temperature ranges for rows with valid temperature data
        valid_temp_indices = df.index[df['temperature_max'].notna()]
        if len(valid_temp_indices) > 0:  # Only try to update if there are valid indices
            df.loc[valid_temp_indices, 'temperature_range'] = temperature_df['temperature_range'].values
    else:
        # If no valid temperature data exists, create an empty temperature_range column
        df['temperature_range'] = 'Unknown'
        print("Warning: No valid temperature data found. All temperature ranges set to 'Unknown'")
    
    # Analysis by temperature - Fix the groupby aggregation
    try:
        # First group by the categorical columns
        grouped = df.groupby(['product_name', 'category', 'temperature_sensitivity', 'temperature_range'])
        
        # Then apply aggregation on the columns we want to aggregate
        temp_analysis = grouped.agg({
            'total_quantity': ['mean', 'sum'],
            'total_sales': ['mean', 'sum']
        }).reset_index()
        
        # Rename the columns for clarity
        temp_analysis.columns = [
            'Product', 'Category', 'Temperature Sensitivity', 'Temperature Range',
            'Average Daily Quantity', 'Total Quantity', 'Average Daily Sales', 'Total Sales'
        ]
    except Exception as e:
        # Create a simple empty dataframe with the expected columns if aggregation fails
        print(f"Error creating temperature analysis: {e}")
        temp_analysis = pd.DataFrame(columns=[
            'Product', 'Category', 'Temperature Sensitivity', 'Temperature Range',
            'Average Daily Quantity', 'Total Quantity', 'Average Daily Sales', 'Total Sales'
        ])
    
    # Analysis by rainfall - Fix the groupby aggregation
    try:
        # First group by the categorical columns
        grouped_rain = df.groupby(['product_name', 'category', 'rainfall_sensitivity', 'is_rainy'])
        
        # Then apply aggregation on the columns we want to aggregate
        rain_analysis = grouped_rain.agg({
            'total_quantity': ['mean', 'sum'],
            'total_sales': ['mean', 'sum']
        }).reset_index()
        
        # Rename the columns for clarity
        rain_analysis.columns = [
            'Product', 'Category', 'Rainfall Sensitivity', 'Is Rainy',
            'Average Daily Quantity', 'Total Quantity', 'Average Daily Sales', 'Total Sales'
        ]
    except Exception as e:
        # Create a simple empty dataframe with the expected columns if aggregation fails
        print(f"Error creating rainfall analysis: {e}")
        rain_analysis = pd.DataFrame(columns=[
            'Product', 'Category', 'Rainfall Sensitivity', 'Is Rainy',
            'Average Daily Quantity', 'Total Quantity', 'Average Daily Sales', 'Total Sales'
        ])
    
    # Save analysis results
    temp_analysis.to_csv(f"{INSIGHTS_PATH}/product_sensitivity_temperature.csv", index=False)
    rain_analysis.to_csv(f"{INSIGHTS_PATH}/product_sensitivity_rainfall.csv", index=False)
    
    # Create JSON file containing key insights
    insights = {
        "title": "Product Weather Sensitivity Insights",
        "temperature_sensitive_products": [],
        "rainfall_sensitive_products": []
    }
    
    # Create visualization for top 5 temperature-sensitive products
    try:
        high_temp_sensitivity = df[df['temperature_sensitivity'] == 'High']
        
        # Only proceed with temperature analysis if we have valid data
        if len(temperature_df) > 0 and 'high' in df['temperature_sensitivity'].str.lower().values and not high_temp_sensitivity.empty:
            # Calculate average sales by product and temperature
            product_temp_sales = high_temp_sensitivity.groupby(['product_name', 'temperature_range'])['total_sales'].mean().reset_index()
            
            # Only create pivot table if we have sufficient data
            if len(product_temp_sales) > 0 and len(product_temp_sales['temperature_range'].unique()) > 1:
                # Pivot to compare between temperature levels
                product_temp_pivot = product_temp_sales.pivot(index='product_name', columns='temperature_range', values='total_sales')
                
                # Only calculate max_diff if we have at least two temperature ranges
                if product_temp_pivot.shape[1] > 1:
                    # Get top 5 products with highest difference
                    product_temp_pivot['max_diff'] = product_temp_pivot.max(axis=1) - product_temp_pivot.min(axis=1)
                    top_temp_sensitive = product_temp_pivot.nlargest(min(5, len(product_temp_pivot)), 'max_diff').index
                    
                    # Filter data for top sensitive products
                    top_temp_data = product_temp_sales[product_temp_sales['product_name'].isin(top_temp_sensitive)]
                    
                    if len(top_temp_data) > 0:
                        # Create chart
                        try:
                            plt.figure(figsize=(12, 8))
                            chart = sns.catplot(
                                data=top_temp_data, 
                                x='product_name', y='total_sales', 
                                hue='temperature_range', kind='bar',
                                height=6, aspect=1.5
                            )
                            chart.set_xticklabels(rotation=45, ha='right')
                            plt.title('Top Temperature Sensitive Products: Average Sales by Temperature')
                            plt.tight_layout()
                            plt.savefig(f"{INSIGHTS_PATH}/top_temperature_sensitive_products.png")
                        except Exception as e:
                            print(f"Error creating temperature sensitivity chart: {e}")
                    
                    # Find temperature-sensitive products
                    for product in top_temp_sensitive:
                        # Skip products with incomplete data
                        if product in product_temp_pivot.index:
                            product_data = product_temp_pivot.loc[product]
                            try:
                                # Skip the max_diff column if it exists
                                cols_to_consider = [col for col in product_data.index if col != 'max_diff']
                                if len(cols_to_consider) > 1:  # Need at least 2 temperature ranges to compare
                                    best_temp = product_data[cols_to_consider].idxmax()
                                    worst_temp = product_data[cols_to_consider].idxmin()
                                    
                                    if pd.notna(product_data[best_temp]) and pd.notna(product_data[worst_temp]) and product_data[worst_temp] > 0:
                                        change_pct = (product_data[best_temp] - product_data[worst_temp]) / product_data[worst_temp] * 100
                                        
                                        insights["temperature_sensitive_products"].append({
                                            "product": product,
                                            "best_temperature": best_temp,
                                            "worst_temperature": worst_temp,
                                            "sales_difference_percentage": f"{change_pct:.2f}%"
                                        })
                            except Exception as e:
                                print(f"Error analyzing temperature sensitivity for product {product}: {e}")
    except Exception as e:
        print(f"Error in temperature sensitivity analysis: {e}")
    
    # Similar analysis for rainfall
    try:
        high_rain_sensitivity = df[df['rainfall_sensitivity'] == 'High']
        if not high_rain_sensitivity.empty:
            product_rain_sales = high_rain_sensitivity.groupby(['product_name', 'is_rainy'])['total_sales'].mean().reset_index()
            product_rain_pivot = product_rain_sales.pivot(index='product_name', columns='is_rainy', values='total_sales')
            
            print(f"Rain pivot table columns: {product_rain_pivot.columns.tolist()}")
            
            # Find products sensitive to rainfall
            for product in product_rain_pivot.index:
                # First check if we have both rainy and non-rainy data for this product
                if True in product_rain_pivot.columns and False in product_rain_pivot.columns:
                    # Check if both values exist and are not NaN
                    if not pd.isna(product_rain_pivot.loc[product, True]) and not pd.isna(product_rain_pivot.loc[product, False]):
                        rainy_sales = product_rain_pivot.loc[product, True]
                        non_rainy_sales = product_rain_pivot.loc[product, False]
                        
                        # Calculate percent change
                        if non_rainy_sales > 0:
                            change_pct = (rainy_sales - non_rainy_sales) / non_rainy_sales * 100
                            
                            if abs(change_pct) > 10:  # Only consider products with significant change
                                better_condition = "rainy days" if change_pct > 0 else "non-rainy days"
                                
                                insights["rainfall_sensitive_products"].append({
                                    "product": product,
                                    "performs_better_on": better_condition,
                                    "sales_difference_percentage": f"{abs(change_pct):.2f}%"
                                })
    except Exception as e:
        print(f"Error in rainfall sensitivity analysis: {e}")
    
    # Save insights as JSON
    with open(f"{INSIGHTS_PATH}/product_sensitivity_insights.json", 'w') as f:
        json.dump(insights, f, indent=4)
    
    return "Successfully analyzed product sensitivity to weather conditions"

def analyze_store_size_performance(**kwargs):
    """
    Analyze store performance by size under different weather conditions
    """
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    
    # Query data
    query = """
    SELECT 
        store_id,
        location,
        region,
        store_size,
        date,
        is_rainy,
        temperature_max,
        SUM(total_sales) as daily_sales,
        SUM(quantity) as daily_units
    FROM 
        sales_weather_fact
    GROUP BY 
        store_id, location, region, store_size, date, is_rainy, temperature_max
    """
    
    # Read data into DataFrame
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Print the data for debugging
    print(f"Store size DataFrame is_rainy unique values: {df['is_rainy'].unique()}")
    print(f"Store size temperature_max null count: {df['temperature_max'].isnull().sum()}")
    
    # Create insights directory if it doesn't exist
    os.makedirs(INSIGHTS_PATH, exist_ok=True)
    
    # Analyze performance by store size and weather condition - Fix the groupby aggregation
    try:
        # First group by the categorical columns
        grouped = df.groupby(['store_size', 'is_rainy'])
        
        # Then apply aggregation on the columns we want to aggregate
        store_size_analysis = grouped.agg({
            'daily_sales': ['mean', 'sum', 'count'],
            'daily_units': ['mean', 'sum', 'count']
        }).reset_index()
        
        # Rename the columns for clarity
        store_size_analysis.columns = [
            'Store Size', 'Is Rainy',
            'Average Daily Sales', 'Total Sales', 'Count',
            'Average Daily Units', 'Total Units', 'Count_2'
        ]
        store_size_analysis = store_size_analysis.drop(columns=['Count_2'])
    except Exception as e:
        # Create a simple empty dataframe with the expected columns if aggregation fails
        print(f"Error creating store size analysis: {e}")
        store_size_analysis = pd.DataFrame(columns=[
            'Store Size', 'Is Rainy',
            'Average Daily Sales', 'Total Sales', 'Count',
            'Average Daily Units', 'Total Units'
        ])
    
    # Save analysis results
    store_size_analysis.to_csv(f"{INSIGHTS_PATH}/store_size_weather_performance.csv", index=False)
    
    # Create visualization
    try:
        plt.figure(figsize=(10, 6))
        if not store_size_analysis.empty and len(store_size_analysis) > 1:
            sns.barplot(x='Store Size', y='Average Daily Sales', hue='Is Rainy', data=store_size_analysis)
            plt.title('Average Daily Sales by Store Size and Weather Condition')
            plt.xlabel('Store Size')
            plt.ylabel('Average Daily Sales')
            plt.xticks(rotation=0)
            plt.tight_layout()
            plt.savefig(f"{INSIGHTS_PATH}/store_size_performance.png")
        else:
            print("Warning: Not enough data to create store size performance plot")
    except Exception as e:
        print(f"Error creating store size performance plot: {e}")
    
    # Create pivot table to analyze rain impact by store size
    try:
        if not store_size_analysis.empty:
            size_rain_pivot = store_size_analysis.pivot(index='Store Size', columns='Is Rainy', values='Average Daily Sales')
            
            # Add column names for clarity
            if True in size_rain_pivot.columns and False in size_rain_pivot.columns:
                size_rain_pivot = size_rain_pivot.rename(columns={True: 'Rainy', False: 'Not Rainy'})
            
            # Calculate change percentage only if we have both values
            if 'Rainy' in size_rain_pivot.columns and 'Not Rainy' in size_rain_pivot.columns:
                size_rain_pivot['Change (%)'] = (size_rain_pivot['Rainy'] - size_rain_pivot['Not Rainy']) / size_rain_pivot['Not Rainy'] * 100
            else:
                size_rain_pivot['Change (%)'] = float('nan')  # Add empty column if we can't calculate
                print("Warning: Missing either rainy or non-rainy data for store size analysis")
        else:
            # Create an empty pivot table if no data
            size_rain_pivot = pd.DataFrame(columns=['Rainy', 'Not Rainy', 'Change (%)'])
            print("Warning: No data for store size rain pivot analysis")
    except Exception as e:
        print(f"Error creating store size rain pivot: {e}")
        size_rain_pivot = pd.DataFrame(columns=['Rainy', 'Not Rainy', 'Change (%)'])
    
    # Create JSON file containing key insights
    insights = {
        "title": "Store Size Performance in Different Weather Conditions",
        "findings": []
    }
    
    if not size_rain_pivot.empty:
        for size in size_rain_pivot.index:
            if 'Change (%)' in size_rain_pivot.columns and not pd.isna(size_rain_pivot.loc[size, 'Change (%)']):
                change_pct = size_rain_pivot.loc[size, 'Change (%)']
                impact = "increase" if change_pct > 0 else "decrease"
                insights["findings"].append({
                    "store_size": size,
                    "rainy_days_impact": f"Sales {impact} by {abs(change_pct):.2f}% on rainy days",
                    "average_sales_no_rain": float(size_rain_pivot.loc[size, 'Not Rainy']) if 'Not Rainy' in size_rain_pivot.columns and not pd.isna(size_rain_pivot.loc[size, 'Not Rainy']) else None,
                    "average_sales_with_rain": float(size_rain_pivot.loc[size, 'Rainy']) if 'Rainy' in size_rain_pivot.columns and not pd.isna(size_rain_pivot.loc[size, 'Rainy']) else None
                })
            else:
                # Add basic entry if we can't calculate change
                insights["findings"].append({
                    "store_size": size,
                    "rainy_days_impact": "Insufficient data to determine rain impact",
                    "note": "Missing data for either rainy or non-rainy conditions"
                })
    
    # Handle temperature analysis - safely handling potential NaN values
    try:
        # Only analyze temperature if we have valid temperature data
        if df['temperature_max'].notna().any():
            df['temperature_range'] = pd.cut(
                df['temperature_max'], 
                bins=[0, 25, 30, 100], 
                labels=['Cool (< 25°C)', 'Moderate (25-30°C)', 'Hot (> 30°C)']
            )
            
            # Group only if we have valid data
            size_temp_df = df.dropna(subset=['temperature_range', 'store_size', 'daily_sales'])
            if not size_temp_df.empty:
                size_temp_analysis = size_temp_df.groupby(['store_size', 'temperature_range'])['daily_sales'].mean().reset_index()
            else:
                print("Warning: No valid temperature data for store size analysis")
                size_temp_analysis = pd.DataFrame(columns=['store_size', 'temperature_range', 'daily_sales'])
        else:
            # Create placeholder data for reporting
            print("Warning: No valid temperature data for store size analysis")
            df['temperature_range'] = 'Unknown'
            size_temp_analysis = pd.DataFrame(columns=['store_size', 'temperature_range', 'daily_sales'])
    except Exception as e:
        print(f"Error in store temperature analysis: {e}")
        size_temp_analysis = pd.DataFrame(columns=['store_size', 'temperature_range', 'daily_sales'])
    
    # Create chart
    try:
        if not size_temp_analysis.empty and len(size_temp_analysis) > 1:
            plt.figure(figsize=(12, 8))
            sns.barplot(x='store_size', y='daily_sales', hue='temperature_range', data=size_temp_analysis)
            plt.title('Average Daily Sales by Store Size and Temperature Range')
            plt.xlabel('Store Size')
            plt.ylabel('Average Daily Sales')
            plt.tight_layout()
            plt.savefig(f"{INSIGHTS_PATH}/store_size_temperature_performance.png")
        else:
            print("Warning: Not enough data to create store size temperature performance plot")
    except Exception as e:
        print(f"Error creating temperature performance plot: {e}")
    
    # Add temperature insights
    for size in df['store_size'].unique():
        size_data = size_temp_analysis[size_temp_analysis['store_size'] == size]
        if not size_data.empty and len(size_data) > 1:
            try:
                best_temp = size_data.loc[size_data['daily_sales'].idxmax(), 'temperature_range']
                worst_temp = size_data.loc[size_data['daily_sales'].idxmin(), 'temperature_range']
                
                for finding in insights["findings"]:
                    if finding["store_size"] == size:
                        finding["best_temperature_range"] = best_temp
                        finding["worst_temperature_range"] = worst_temp
                        break
            except Exception as e:
                print(f"Error analyzing temperature data for store size {size}: {e}")
    
    # Save insights as JSON
    with open(f"{INSIGHTS_PATH}/store_size_performance_insights.json", 'w') as f:
        json.dump(insights, f, indent=4)
    
    return "Successfully analyzed store size performance by weather conditions"

def generate_final_report(**kwargs):
    """
    Combine all insights and create final report
    """
    # Read insight files
    with open(f"{INSIGHTS_PATH}/weather_impact_by_region_insights.json", 'r') as f:
        region_insights = json.load(f)
    
    with open(f"{INSIGHTS_PATH}/product_sensitivity_insights.json", 'r') as f:
        product_insights = json.load(f)
    
    with open(f"{INSIGHTS_PATH}/store_size_performance_insights.json", 'r') as f:
        store_insights = json.load(f)
    
    # Combine insights into final report
    final_insights = {
        "title": "Weather and Sales Relationship Analysis",
        "date_generated": datetime.now().strftime("%Y-%m-%d"),
        "summary": {
            "regional_impact": "How weather affects sales in different regions",
            "product_sensitivity": "Which products are most sensitive to weather changes",
            "store_performance": "How different store sizes perform in various weather conditions"
        },
        "insights": {
            "regional_findings": region_insights["findings"],
            "temperature_sensitive_products": product_insights["temperature_sensitive_products"][:3],  # Top 3
            "rainfall_sensitive_products": product_insights["rainfall_sensitive_products"][:3],  # Top 3
            "store_size_findings": store_insights["findings"]
        }
    }
    
    # Add main conclusions to the report
    final_insights["conclusions"] = []
    
    # Conclusion about regional impact
    region_conclusion = "Regional analysis: "
    north_impact = next((item for item in region_insights["findings"] if item["region"] == "North"), None)
    south_impact = next((item for item in region_insights["findings"] if item["region"] == "South"), None)
    central_impact = next((item for item in region_insights["findings"] if item["region"] == "Central"), None)
    
    if north_impact and south_impact and central_impact:
        region_conclusion += f"The North region is most affected by rain with a change of {north_impact['impact'].split('by ')[1]}. "
        region_conclusion += f"Meanwhile, the South region shows a change of {south_impact['impact'].split('by ')[1]} and the Central region {central_impact['impact'].split('by ')[1]}."
    
    final_insights["conclusions"].append({
        "title": "Weather Impact by Region",
        "content": region_conclusion
    })
    
    # Conclusion about product sensitivity
    product_conclusion = "Weather-sensitive products: "
    if product_insights["temperature_sensitive_products"]:
        top_temp_product = product_insights["temperature_sensitive_products"][0]
        product_conclusion += f"{top_temp_product['product']} has the best sales in {top_temp_product['best_temperature']} conditions "
        product_conclusion += f"and the lowest in {top_temp_product['worst_temperature']} conditions, with a difference of {top_temp_product['sales_difference_percentage']}. "
    
    if product_insights["rainfall_sensitive_products"]:
        top_rain_product = product_insights["rainfall_sensitive_products"][0]
        product_conclusion += f"{top_rain_product['product']} performs better on {top_rain_product['performs_better_on']} "
        product_conclusion += f"with a difference of {top_rain_product['sales_difference_percentage']}."
    
    final_insights["conclusions"].append({
        "title": "Weather-Sensitive Products",
        "content": product_conclusion
    })
    
    # Conclusion about store performance
    store_conclusion = "Store performance by size: "
    if store_insights["findings"]:
        small_store = next((item for item in store_insights["findings"] if item["store_size"] == "Small"), None)
        large_store = next((item for item in store_insights["findings"] if item["store_size"] == "Large"), None)
        
        if small_store and large_store:
            store_conclusion += f"Small stores have {small_store['rainy_days_impact']} while large stores have {large_store['rainy_days_impact']}. "
            
            if "best_temperature_range" in small_store:
                store_conclusion += f"Small stores perform best in {small_store['best_temperature_range']} temperature conditions, "
                store_conclusion += f"while large stores perform best in {large_store['best_temperature_range']} conditions."
    
    final_insights["conclusions"].append({
        "title": "Store Performance by Size in Different Weather Conditions",
        "content": store_conclusion
    })
    
    # Save final report
    with open(f"{FINAL_DATA_PATH}/weather_sales_final_report.json", 'w') as f:
        json.dump(final_insights, f, indent=4)
    
    # Create text report for easier reading
    with open(f"{FINAL_DATA_PATH}/weather_sales_final_report.txt", 'w') as f:
        f.write(f"# {final_insights['title']}\n")
        f.write(f"Date generated: {final_insights['date_generated']}\n\n")
        
        f.write("## Summary\n")
        for key, value in final_insights['summary'].items():
            f.write(f"- {value}\n")
        
        f.write("\n## Key Conclusions\n")
        for conclusion in final_insights['conclusions']:
            f.write(f"### {conclusion['title']}\n")
            f.write(f"{conclusion['content']}\n\n")
        
        f.write("\n## Detailed Insights\n")
        
        f.write("\n### Regional Impact\n")
        for finding in final_insights['insights']['regional_findings']:
            f.write(f"- Region {finding['region']}: {finding['impact']}\n")
        
        f.write("\n### Top Temperature-Sensitive Products\n")
        for product in final_insights['insights']['temperature_sensitive_products']:
            f.write(f"- {product['product']}: Best at {product['best_temperature']}, worst at {product['worst_temperature']}, difference {product['sales_difference_percentage']}\n")
        
        f.write("\n### Top Rainfall-Sensitive Products\n")
        for product in final_insights['insights']['rainfall_sensitive_products']:
            f.write(f"- {product['product']}: Performs better on {product['performs_better_on']}, difference {product['sales_difference_percentage']}\n")
        
        f.write("\n### Store Performance by Size\n")
        for finding in final_insights['insights']['store_size_findings']:
            f.write(f"- {finding['store_size']} stores: {finding['rainy_days_impact']}\n")
            if "best_temperature_range" in finding:
                f.write(f"  + Best at: {finding['best_temperature_range']}, worst at: {finding['worst_temperature_range']}\n")
    
    return "Successfully generated final report"

# Define DAG
with DAG(
    'sales_weather_insights',
    default_args=default_args,
    description='Generate insights about the relationship between weather and sales',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM (after ETL completes)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['sales', 'weather', 'insights'],
) as dag:
    
    # Task to analyze weather impact by region
    analyze_by_region = PythonOperator(
        task_id='analyze_weather_impact_by_region',
        python_callable=analyze_weather_impact_by_region,
    )
    
    # Task to analyze product sensitivity to weather
    analyze_products = PythonOperator(
        task_id='analyze_product_sensitivity',
        python_callable=analyze_product_sensitivity,
    )
    
    # Task to analyze store performance by size
    analyze_stores = PythonOperator(
        task_id='analyze_store_size_performance',
        python_callable=analyze_store_size_performance,
    )
    
    # Task to generate final report
    final_report = PythonOperator(
        task_id='generate_final_report',
        python_callable=generate_final_report,
    )
    
    # Set up dependencies
    [analyze_by_region, analyze_products, analyze_stores] >> final_report