from pyspark.sql import SparkSession

def execute_sample_queries():
    """
    Execute sample business queries for different teams
    """
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("sample_supply_chain_queries") \
        .master("yarn") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # Set database context
        spark.sql("USE supply_chain_db")
        
        print("=" * 80)
        print("SAMPLE BUSINESS QUERIES FOR RETAIL SUPPLY CHAIN DATA WAREHOUSE")
        print("=" * 80)
        
        # 1. Supply Chain Team Queries
        print("\n1. SUPPLY CHAIN TEAM: Supplier Performance Analysis")
        print("-" * 60)
        
        supply_chain_query = """
        SELECT 
            supplier_name,
            warehouse_region,
            ROUND(avg_delivery_time, 2) as avg_delivery_days,
            ROUND(late_shipment_rate * 100, 2) as late_shipment_percent,
            performance_category,
            ROUND(avg_supply_chain_risk * 100, 2) as risk_score_percent
        FROM supply_chain_team_metrics 
        WHERE order_year = 2024 AND order_month = 1
        ORDER BY avg_delivery_time DESC
        LIMIT 10
        """
        
        supply_chain_results = spark.sql(supply_chain_query)
        supply_chain_results.show(truncate=False)
        
        # 2. Store Managers Queries
        print("\n2. STORE MANAGERS: Inventory Health & Product Velocity")
        print("-" * 60)
        
        store_manager_query = """
        SELECT 
            product_name,
            category_name,
            warehouse_name,
            ROUND(avg_stock_level, 0) as avg_stock,
            ROUND(avg_stock_out_risk * 100, 2) as stockout_risk_percent,
            product_velocity_classification,
            stock_health_status,
            ROUND(total_demand, 0) as total_demand,
            ROUND(total_revenue, 2) as total_revenue
        FROM store_manager_metrics 
        WHERE order_year = 2024 AND order_month = 1
        ORDER BY total_demand DESC
        LIMIT 15
        """
        
        store_manager_results = spark.sql(store_manager_query)
        store_manager_results.show(truncate=False)
        
        # 3. Finance Team Queries
        print("\n3. FINANCE TEAM: Supplier Spend Analysis")
        print("-" * 60)
        
        finance_query = """
        SELECT 
            supplier_name,
            category_name,
            supplier_spend_tier,
            ROUND(total_purchase_value, 2) as total_spend,
            ROUND(total_shipping_cost, 2) as shipping_cost,
            ROUND(total_supply_chain_cost, 2) as total_cost,
            ROUND(cost_efficiency_ratio, 2) as cost_efficiency,
            ROUND(avg_payment_cycle_days, 1) as avg_payment_days
        FROM finance_team_metrics 
        WHERE order_year = 2024 AND order_month = 1
        ORDER BY total_spend DESC
        LIMIT 10
        """
        
        finance_results = spark.sql(finance_query)
        finance_results.show(truncate=False)
        
        # 4. Executive Dashboard Queries
        print("\n4. EXECUTIVE DASHBOARD: Supplier Scorecard")
        print("-" * 60)
        
        executive_query = """
        SELECT 
            supplier_name,
            ROUND(delivery_score * 100, 2) as delivery_score,
            ROUND(quality_score * 20, 2) as quality_score,
            ROUND(availability_score * 100, 2) as availability_score,
            ROUND(overall_supplier_score, 2) as overall_score,
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
        
        executive_results = spark.sql(executive_query)
        executive_results.show(truncate=False)
        
        # 5. Risk Management Queries
        print("\n5. RISK MANAGEMENT: High-Risk Products")
        print("-" * 60)
        
        risk_query = """
        SELECT 
            product_name,
            supplier_name,
            warehouse_name,
            current_stock_level,
            ROUND(stock_out_risk_score * 100, 2) as stockout_risk_percent,
            ROUND(supply_chain_risk_score * 100, 2) as overall_risk_percent,
            movement_velocity,
            stock_status
        FROM supply_chain_fact 
        WHERE supply_chain_risk_score > 0.7
        ORDER BY supply_chain_risk_score DESC
        LIMIT 20
        """
        
        risk_results = spark.sql(risk_query)
        risk_results.show(truncate=False)
        
        # 6. Trend Analysis Queries
        print("\n6. TREND ANALYSIS: Monthly Performance")
        print("-" * 60)
        
        trend_query = """
        SELECT 
            order_year,
            order_month,
            COUNT(DISTINCT order_key) as total_orders,
            ROUND(SUM(order_value), 2) as total_revenue,
            ROUND(AVG(delivery_time_days), 2) as avg_delivery_days,
            ROUND(SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as late_delivery_percent
        FROM supply_chain_fact
        GROUP BY order_year, order_month
        ORDER BY order_year, order_month
        """
        
        trend_results = spark.sql(trend_query)
        trend_results.show(truncate=False)
        
        print("\n" + "=" * 80)
        print("ALL SAMPLE QUERIES EXECUTED SUCCESSFULLY!")
        print("=" * 80)
        
    except Exception as e:
        print(f"Error executing sample queries: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    execute_sample_queries()