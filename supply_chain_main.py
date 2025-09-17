#!/usr/bin/env python3
"""
Retail Supply Chain Warehouse Data Pipeline with SCD Type 2
Main execution script for comprehensive supply chain analytics

Author: Supply Chain Analytics Team
Date: 2024
Purpose: Extract supplier & shipment logs → Transform in PySpark → Load into Hive star schema
"""

from supply_chain_data_pipeline import SupplyChainDataPipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, max as spark_max
import sys
from datetime import datetime

def main():
    """
    Main execution function for the Retail Supply Chain Data Pipeline
    """
    
    print("="*100)
    print("RETAIL SUPPLY CHAIN WAREHOUSE DATA PIPELINE WITH SCD TYPE 2")
    print("Comprehensive Supply Chain Analytics & Slowly Changing Dimensions")
    print("="*100)
    print(f"Pipeline started at: {datetime.now()}")
    print()
    
    try:
        # Database connection configuration
        # PostgreSQL RDS connection details
        jdbc_url = "jdbc:postgresql://database-1.cpyaoguwudqo.ap-south-1.rds.amazonaws.com:5432/retail_supply_chain_db"
        user = "postgres"
        password = "mypostgreypassword"
        driver = "org.postgresql.Driver"
        
        print("Database Configuration:")
        print(f"  - Host: {jdbc_url.split('//')[1].split(':')[0]}")
        print(f"  - Database: retail_supply_chain_db")
        print(f"  - User: {user}")
        print("  - Connection: PostgreSQL RDS")
        print()
        
        # Initialize the pipeline
        print("Initializing Supply Chain Data Pipeline...")
        pipeline = SupplyChainDataPipeline(jdbc_url, user, password, driver)
        print("Pipeline initialized successfully!")
        print()
        
        # Execute the complete pipeline
        print("Executing complete supply chain data pipeline...")
        results = pipeline.run_complete_pipeline()
        
        # Display final results and analytics
        print_pipeline_summary(results)
        
        # Generate sample business queries for different teams
        print()
        print("="*80)
        print("GENERATING SAMPLE BUSINESS QUERIES FOR DIFFERENT TEAMS")
        print("="*80)
        
        generate_sample_queries(pipeline.spark)
        
        print()
        print("="*100)
        print("SUPPLY CHAIN PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
        print(f"Pipeline completed at: {datetime.now()}")
        print("="*100)
        
    except Exception as e:
        print(f"Pipeline execution failed with error: {str(e)}")
        print("Please check the logs for detailed error information.")
        sys.exit(1)

def print_pipeline_summary(results):
    """
    Print comprehensive pipeline execution summary
    """
    print()
    print("="*80)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*80)
    
    # Fact table statistics
    fact_df = results["fact_table"]
    total_records = fact_df.count()
    total_revenue = fact_df.agg(spark_sum("order_value")).collect()[0][0]
    avg_delivery_time = fact_df.agg(avg("delivery_time_days")).collect()[0][0]
    late_shipment_rate = fact_df.filter(col("is_late_delivery") == True).count() / total_records
    
    print(f"FACT TABLE STATISTICS:")
    print(f"  - Total Records Processed: {total_records:,}")
    print(f"  - Total Revenue: ${total_revenue:,.2f}")
    print(f"  - Average Delivery Time: {avg_delivery_time:.1f} days")
    print(f"  - Late Shipment Rate: {late_shipment_rate:.2%}")
    print()
    
    # Dimension table statistics
    dimensions = results["dimensions"]
    print(f"DIMENSION TABLES (SCD Type 2):")
    for dim_name, dim_df in dimensions.items():
        dim_count = dim_df.count()
        print(f"  - {dim_name.title()} Dimension: {dim_count:,} records")
    print()
    
    # Business metrics statistics
    business_metrics = results["business_metrics"]
    print(f"BUSINESS METRICS TABLES:")
    for metric_name, metric_df in business_metrics.items():
        metric_count = metric_df.count()
        print(f"  - {metric_name.replace('_', ' ').title()}: {metric_count:,} records")
    print()
    
    # KPI Summary
    kpis = results["kpis"]
    print(f"KEY PERFORMANCE INDICATORS:")
    print(f"  - Perfect Order Rate: {kpis['perfect_order_rate']:.2%}")
    print(f"  - Supplier Scorecard: {kpis['supplier_scorecard'].count()} suppliers evaluated")
    print(f"  - Inventory Turnover Analysis: {kpis['inventory_turnover'].count()} categories analyzed")

def generate_sample_queries(spark):
    """
    Generate and execute sample business queries for different user teams
    """
    
    try:
        # Set the database context
        spark.sql("USE supply_chain_db")
        
        print()
        print("1. SUPPLY CHAIN TEAM QUERIES")
        print("-" * 50)
        
        # Query 1: Average delivery times by supplier
        print("Query: Average delivery times and late shipments by supplier")
        delivery_query = """
        SELECT 
            supplier_name,
            warehouse_region,
            avg_delivery_time,
            late_shipment_rate,
            performance_category,
            avg_supply_chain_risk
        FROM supply_chain_team_metrics 
        WHERE order_year = 2024
        ORDER BY avg_delivery_time DESC
        LIMIT 10
        """
        
        delivery_results = spark.sql(delivery_query)
        delivery_results.show(truncate=False)
        
        print()
        print("2. STORE MANAGERS QUERIES")
        print("-" * 50)
        
        # Query 2: Inventory levels and fast vs slow moving items
        print("Query: Inventory levels and product movement analysis")
        inventory_query = """
        SELECT 
            product_name,
            warehouse_name,
            avg_stock_level,
            avg_stock_out_risk,
            product_velocity_classification,
            stock_health_status,
            total_demand,
            total_revenue
        FROM store_manager_metrics 
        WHERE order_year = 2024
        ORDER BY total_demand DESC
        LIMIT 15
        """
        
        inventory_results = spark.sql(inventory_query)
        inventory_results.show(truncate=False)
        
        print()
        print("3. FINANCE TEAM QUERIES")
        print("-" * 50)
        
        # Query 3: Supplier payment analysis
        print("Query: Supplier payment and cost analysis")
        finance_query = """
        SELECT 
            supplier_name,
            supplier_spend_tier,
            total_purchase_value,
            total_shipping_cost,
            total_supply_chain_cost,
            cost_efficiency_ratio,
            avg_payment_cycle_days
        FROM finance_team_metrics 
        WHERE order_year = 2024
        ORDER BY total_purchase_value DESC
        LIMIT 10
        """
        
        finance_results = spark.sql(finance_query)
        finance_results.show(truncate=False)
        
        print()
        print("4. EXECUTIVE DASHBOARD QUERIES")
        print("-" * 50)
        
        # Query 4: Supplier performance scorecard
        print("Query: Overall supplier performance scorecard")
        scorecard_query = """
        SELECT 
            supplier_name,
            delivery_score,
            quality_score,
            availability_score,
            overall_supplier_score,
            CASE 
                WHEN overall_supplier_score >= 80 THEN 'EXCELLENT'
                WHEN overall_supplier_score >= 70 THEN 'GOOD'
                WHEN overall_supplier_score >= 60 THEN 'FAIR'
                ELSE 'NEEDS_IMPROVEMENT'
            END as supplier_grade
        FROM supplier_scorecard 
        ORDER BY overall_supplier_score DESC
        LIMIT 10
        """
        
        scorecard_results = spark.sql(scorecard_query)
        scorecard_results.show(truncate=False)
        
        print()
        print("5. SUPPLY CHAIN RISK ANALYSIS")
        print("-" * 50)
        
        # Query 5: High-risk supply chain scenarios
        risk_query = """
        SELECT 
            product_name,
            supplier_name,
            warehouse_name,
            current_stock_level,
            stock_out_risk_score,
            supply_chain_risk_score,
            movement_velocity,
            stock_status
        FROM supply_chain_fact 
        WHERE supply_chain_risk_score > 0.7
        ORDER BY supply_chain_risk_score DESC
        LIMIT 20
        """
        
        risk_results = spark.sql(risk_query)
        risk_results.show(truncate=False)
        
        print()
        print("SAMPLE QUERIES COMPLETED SUCCESSFULLY!")
        print("All teams can now query their respective data slices from the warehouse.")
        
    except Exception as e:
        print(f"Error executing sample queries: {str(e)}")

def create_additional_business_views(spark):
    """
    Create additional business views for advanced analytics
    """
    print("Creating additional business views...")
    
    try:
        spark.sql("USE supply_chain_db")
        
        # View 1: Monthly Supply Chain Performance
        spark.sql("""
        CREATE OR REPLACE VIEW monthly_supply_chain_performance AS
        SELECT 
            order_year,
            order_month,
            COUNT(DISTINCT order_key) as total_orders,
            SUM(order_value) as total_revenue,
            AVG(delivery_time_days) as avg_delivery_time,
            SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) / COUNT(*) as late_delivery_rate,
            AVG(supply_chain_risk_score) as avg_risk_score
        FROM supply_chain_fact
        GROUP BY order_year, order_month
        ORDER BY order_year, order_month
        """)
        
        # View 2: Supplier Performance Trends
        spark.sql("""
        CREATE OR REPLACE VIEW supplier_performance_trends AS
        SELECT 
            s.supplier_name,
            f.order_year,
            f.