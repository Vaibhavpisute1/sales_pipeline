from pyspark.sql import SparkSession
from config import HDFS_PATHS

def create_hive_tables():
    """
    Create Hive external tables for the supply chain data warehouse
    """
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("create_hive_tables") \
        .master("yarn") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # Create database
        spark.sql("CREATE DATABASE IF NOT EXISTS supply_chain_db")
        spark.sql("USE supply_chain_db")
        
        print("Creating Hive external tables...")
        
        # Fact Table
        spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS supply_chain_fact (
            order_key INT,
            customer_key INT,
            product_key INT,
            supplier_key INT,
            warehouse_key INT,
            category_key INT,
            order_date DATE,
            shipment_date DATE,
            expected_delivery_date DATE,
            actual_delivery_date DATE,
            customer_name STRING,
            product_name STRING,
            category_name STRING,
            supplier_name STRING,
            order_quantity INT,
            unit_price DOUBLE,
            order_value DOUBLE,
            delivery_time_days INT,
            is_late_delivery BOOLEAN,
            shipping_cost DOUBLE,
            delivery_performance_score DOUBLE,
            quality_rating DOUBLE,
            current_stock_level INT,
            reorder_point INT,
            stock_out_risk_score DOUBLE,
            days_of_supply INT,
            movement_velocity STRING,
            stock_status STRING,
            warehouse_name STRING,
            warehouse_region STRING,
            customer_order_sequence INT,
            customer_total_orders INT,
            customer_cumulative_value DOUBLE,
            supplier_order_sequence INT,
            supplier_avg_delivery_time DOUBLE,
            supplier_late_delivery_rate DOUBLE,
            product_order_sequence INT,
            product_demand_trend DOUBLE,
            supply_chain_risk_score DOUBLE,
            inventory_turnover_category STRING,
            total_supply_chain_cost DOUBLE,
            order_year INT,
            order_month INT,
            order_day INT
        )
        STORED AS PARQUET
        LOCATION '{HDFS_PATHS["fact_table"]}'
        """)
        
        # Customer Dimension (SCD Type 2)
        spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS customer_dim (
            customer_id INT,
            customer_name STRING,
            customer_email STRING,
            effective_start_date DATE,
            effective_end_date DATE,
            is_current BOOLEAN,
            record_version INT,
            created_timestamp TIMESTAMP,
            updated_timestamp TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION '{HDFS_PATHS["customer_dim"]}'
        """)
        
        # Supplier Dimension (SCD Type 2)
        spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS supplier_dim (
            supplier_id INT,
            supplier_name STRING,
            effective_start_date DATE,
            effective_end_date DATE,
            is_current BOOLEAN,
            record_version INT,
            created_timestamp TIMESTAMP,
            updated_timestamp TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION '{HDFS_PATHS["supplier_dim"]}'
        """)
        
        # Warehouse Dimension (SCD Type 2)
        spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS warehouse_dim (
            warehouse_id INT,
            warehouse_name STRING,
            warehouse_type STRING,
            region STRING,
            city STRING,
            effective_start_date DATE,
            effective_end_date DATE,
            is_current BOOLEAN,
            record_version INT,
            created_timestamp TIMESTAMP,
            updated_timestamp TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION '{HDFS_PATHS["warehouse_dim"]}'
        """)
        
        # Business Metrics Tables
        spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS supply_chain_team_metrics (
            supplier_key INT,
            supplier_name STRING,
            warehouse_region STRING,
            order_year INT,
            order_month INT,
            avg_delivery_time DOUBLE,
            late_shipment_rate DOUBLE,
            avg_delivery_performance DOUBLE,
            avg_quality_rating DOUBLE,
            total_shipping_cost DOUBLE,
            avg_supply_chain_risk DOUBLE,
            performance_category STRING
        )
        STORED AS PARQUET
        LOCATION '{HDFS_PATHS["supply_chain_metrics"]}'
        """)
        
        spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS store_manager_metrics (
            product_key INT,
            product_name STRING,
            category_name STRING,
            warehouse_key INT,
            warehouse_name STRING,
            order_year INT,
            order_month INT,
            avg_stock_level DOUBLE,
            avg_stock_out_risk DOUBLE,
            total_demand DOUBLE,
            avg_days_of_supply DOUBLE,
            total_revenue DOUBLE,
            stock_health_status STRING,
            product_velocity_classification STRING
        )
        STORED AS PARQUET
        LOCATION '{HDFS_PATHS["store_manager_metrics"]}'
        """)
        
        spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS finance_team_metrics (
            supplier_key INT,
            supplier_name STRING,
            category_name STRING,
            order_year INT,
            order_month INT,
            total_purchase_value DOUBLE,
            avg_unit_price DOUBLE,
            total_shipping_cost DOUBLE,
            total_supply_chain_cost DOUBLE,
            avg_payment_cycle_days DOUBLE,
            total_units_purchased DOUBLE,
            supplier_spend_tier STRING,
            cost_efficiency_ratio DOUBLE
        )
        STORED AS PARQUET
        LOCATION '{HDFS_PATHS["finance_metrics"]}'
        """)
        
        spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS supplier_scorecard (
            supplier_key INT,
            supplier_name STRING,
            delivery_score DOUBLE,
            quality_score DOUBLE,
            availability_score DOUBLE,
            overall_supplier_score DOUBLE
        )
        STORED AS PARQUET
        LOCATION '{HDFS_PATHS["supplier_scorecard"]}'
        """)
        
        print("Hive tables created successfully!")
        
        # Show table information
        spark.sql("SHOW TABLES").show()
        
    except Exception as e:
        print(f"Error creating Hive tables: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    create_hive_tables()