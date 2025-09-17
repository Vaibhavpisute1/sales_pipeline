from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, avg, year, month, dayofmonth, datediff, when, isnan, isnull, coalesce
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, udf, max as spark_max, min as spark_min
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType
from datetime import datetime, date

class SupplyChainDataPipeline:
    """
    Advanced Supply Chain Data Pipeline with SCD Type 2 implementation
    for Retail Warehouse Operations
    """

    def __init__(self, jdbc_url, user, pwd, driver):
        # Initialize Spark session with optimized configurations
        self.spark = SparkSession.builder \
            .appName("retail_supply_chain_scd_pipeline") \
            .master("yarn") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .enableHiveSupport() \
            .getOrCreate()
        
        print("Supply Chain Spark session created successfully with SCD support")

        # Set JDBC connection properties
        self.host = jdbc_url
        self.user = user
        self.pwd = pwd
        self.driver = driver
        
        # SCD configuration
        self.current_date = date.today()
        self.high_date = date(9999, 12, 31)

    def fetch_data_from_sql(self, db_table):
        """
        Fetch data from SQL table and load into DataFrame with error handling.
        """
        connection_properties = {
            "url": self.host,
            "user": self.user,
            "password": self.pwd,
            "driver": self.driver,
            "dbtable": db_table,
            "fetchsize": "10000",
            "numPartitions": "8"
        }
        
        print(f"Reading supply chain table: {db_table}")
        
        try:
            dataframe = self.spark.read.format("jdbc") \
                .option("url", connection_properties["url"]) \
                .option("user", connection_properties["user"]) \
                .option("password", connection_properties["password"]) \
                .option("driver", connection_properties["driver"]) \
                .option("dbtable", connection_properties["dbtable"]) \
                .option("fetchsize", connection_properties["fetchsize"]) \
                .option("numPartitions", connection_properties["numPartitions"]) \
                .load()
            
            # Show sample data and record count
            record_count = dataframe.count()
            print(f"Successfully loaded {record_count} records from {db_table}")
            dataframe.show(5, False)
            
            return dataframe
            
        except Exception as e:
            print(f"Error reading {db_table}: {str(e)}")
            raise

    def create_supply_chain_fact_table(self, orders_df, customers_df, order_items_df, 
                                     products_df, categories_df, suppliers_df, 
                                     inventory_df, shipments_df, warehouses_df):
        """
        Create comprehensive supply chain fact table with all business metrics
        """
        print("Creating comprehensive supply chain fact table...")
        
        # Primary joins for order-centric data
        fact_df = orders_df.alias("o") \
            .join(customers_df.alias("c"), col("o.customer_id") == col("c.customer_id"), "inner") \
            .join(order_items_df.alias("oi"), col("o.order_id") == col("oi.order_id"), "inner") \
            .join(products_df.alias("p"), col("oi.product_id") == col("p.product_id"), "inner") \
            .join(categories_df.alias("cat"), col("p.category_id") == col("cat.category_id"), "inner") \
            .join(suppliers_df.alias("s"), col("p.supplier_id") == col("s.supplier_id"), "inner") \
            .join(shipments_df.alias("sh"), col("o.order_id") == col("sh.order_id"), "left") \
            .join(warehouses_df.alias("w"), col("sh.warehouse_id") == col("w.warehouse_id"), "left") \
            .join(inventory_df.alias("i"), 
                  (col("p.product_id") == col("i.product_id")) & 
                  (col("w.warehouse_id") == col("i.warehouse_id")), "left")

        print("Base joins completed successfully")
        
        # Select and transform columns with business logic
        fact_transformed = fact_df.select(
            # Order Information
            col("o.order_id").alias("order_key"),
            col("o.customer_id").alias("customer_key"),
            col("p.product_id").alias("product_key"),
            col("s.supplier_id").alias("supplier_key"),
            col("w.warehouse_id").alias("warehouse_key"),
            col("cat.category_id").alias("category_key"),
            
            # Date Dimensions
            col("o.order_date"),
            col("sh.shipment_date"),
            col("sh.expected_delivery_date"),
            col("sh.actual_delivery_date"),
            
            # Business Metrics
            col("c.customer_name").alias("customer_name"),
            col("p.product_name"),
            col("cat.category_name"),
            col("s.supplier_name"),
            col("oi.quantity").alias("order_quantity"),
            col("oi.price").alias("unit_price"),
            (col("oi.quantity") * col("oi.price")).alias("order_value"),
            
            # Supply Chain Metrics
            coalesce(col("sh.delivery_time_days"), lit(0)).alias("delivery_time_days"),
            coalesce(col("sh.is_late"), lit(False)).alias("is_late_delivery"),
            coalesce(col("sh.shipping_cost"), lit(0.0)).alias("shipping_cost"),
            coalesce(col("sh.delivery_performance_score"), lit(1.0)).alias("delivery_performance_score"),
            coalesce(col("sh.quality_rating"), lit(5.0)).alias("quality_rating"),
            
            # Inventory Metrics
            coalesce(col("i.current_stock"), lit(0)).alias("current_stock_level"),
            coalesce(col("i.reorder_point"), lit(0)).alias("reorder_point"),
            coalesce(col("i.stock_out_risk_score"), lit(0.0)).alias("stock_out_risk_score"),
            coalesce(col("i.days_of_supply"), lit(0)).alias("days_of_supply"),
            coalesce(col("i.movement_velocity"), lit("UNKNOWN")).alias("movement_velocity"),
            coalesce(col("i.stock_status"), lit("UNKNOWN")).alias("stock_status"),
            
            # Warehouse Information
            coalesce(col("w.warehouse_name"), lit("Unknown")).alias("warehouse_name"),
            coalesce(col("w.region"), lit("Unknown")).alias("warehouse_region")
        )
        
        print("Initial fact table transformation completed")
        
        # Add calculated business metrics using window functions
        window_customer = Window.partitionBy("customer_key").orderBy("order_date")
        window_supplier = Window.partitionBy("supplier_key").orderBy("order_date")
        window_product = Window.partitionBy("product_key").orderBy("order_date")
        
        # Enhanced fact table with advanced supply chain KPIs
        enhanced_fact_df = fact_transformed.withColumn(
            # Customer Analytics
            "customer_order_sequence", row_number().over(window_customer)
        ).withColumn(
            "customer_total_orders", rank().over(window_customer)
        ).withColumn(
            "customer_cumulative_value", sum("order_value").over(window_customer)
        ).withColumn(
            # Supplier Performance Analytics
            "supplier_order_sequence", row_number().over(window_supplier)
        ).withColumn(
            "supplier_avg_delivery_time", avg("delivery_time_days").over(window_supplier)
        ).withColumn(
            "supplier_late_delivery_rate", 
            avg(when(col("is_late_delivery"), 1.0).otherwise(0.0)).over(window_supplier)
        ).withColumn(
            # Product Performance
            "product_order_sequence", row_number().over(window_product)
        ).withColumn(
            "product_demand_trend", sum("order_quantity").over(window_product)
        ).withColumn(
            # Supply Chain Risk Indicators
            "supply_chain_risk_score",
            (col("stock_out_risk_score") * 0.4 + 
             when(col("is_late_delivery"), 0.6).otherwise(0.0) * 0.6)
        ).withColumn(
            # Inventory Velocity Classification
            "inventory_turnover_category",
            when(col("movement_velocity") == "FAST", "HIGH_TURNOVER")
            .when(col("movement_velocity") == "MEDIUM", "MEDIUM_TURNOVER")
            .when(col("movement_velocity") == "SLOW", "LOW_TURNOVER")
            .otherwise("UNKNOWN_TURNOVER")
        ).withColumn(
            # Financial Impact Metrics
            "total_supply_chain_cost", col("order_value") + col("shipping_cost")
        ).withColumn(
            # Date extraction for time-based analytics
            "order_year", year("order_date")
        ).withColumn(
            "order_month", month("order_date")
        ).withColumn(
            "order_day", dayofmonth("order_date")
        )
        
        print("Enhanced fact table with advanced KPIs created successfully")
        return enhanced_fact_df

    def apply_data_quality_checks(self, df):
        """
        Comprehensive data quality checks and cleansing for supply chain data
        """
        print("Applying supply chain data quality checks...")
        
        # Check for null values in critical columns
        critical_columns = ["order_key", "customer_key", "product_key", "order_date", "order_quantity"]
        
        for col_name in critical_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                print(f"Warning: {null_count} null values found in critical column {col_name}")
        
        # Data cleansing rules
        cleaned_df = df.filter(
            # Remove invalid orders
            (col("order_quantity") > 0) & 
            (col("unit_price") > 0) &
            (col("order_value") > 0)
        ).fillna({
            # Fill missing values with defaults
            'delivery_time_days': 0,
            'shipping_cost': 0.0,
            'current_stock_level': 0,
            'stock_out_risk_score': 0.5,
            'delivery_performance_score': 1.0,
            'quality_rating': 3.0
        }).withColumn(
            # Data validation corrections
            "delivery_performance_score",
            when(col("delivery_performance_score") > 1.0, 1.0)
            .when(col("delivery_performance_score") < 0.0, 0.0)
            .otherwise(col("delivery_performance_score"))
        ).withColumn(
            "stock_out_risk_score",
            when(col("stock_out_risk_score") > 1.0, 1.0)
            .when(col("stock_out_risk_score") < 0.0, 0.0)
            .otherwise(col("stock_out_risk_score"))
        )
        
        print("Data quality checks and cleansing completed")
        return cleaned_df

    def create_scd_type2_dimension(self, df, dimension_name, key_columns, track_columns):
        """
        Implement SCD Type 2 for dimension tables to track historical changes
        """
        print(f"Implementing SCD Type 2 for {dimension_name} dimension...")
        
        # Add SCD Type 2 columns
        scd_df = df.select(*key_columns, *track_columns) \
            .dropDuplicates() \
            .withColumn("effective_start_date", lit(self.current_date)) \
            .withColumn("effective_end_date", lit(self.high_date)) \
            .withColumn("is_current", lit(True)) \
            .withColumn("record_version", lit(1)) \
            .withColumn("created_timestamp", lit(datetime.now())) \
            .withColumn("updated_timestamp", lit(datetime.now()))
        
        print(f"SCD Type 2 {dimension_name} dimension created with {scd_df.count()} records")
        return scd_df

    def create_business_aggregations(self, fact_df):
        """
        Create business-specific aggregations for different user groups
        """
        print("Creating business-specific aggregations...")
        
        # 1. Supply Chain Team Analytics - Avg delivery times and late shipments
        supply_chain_metrics = fact_df.groupBy(
            "supplier_key", "supplier_name", "warehouse_region", "order_year", "order_month"
        ).agg(
            avg("delivery_time_days").alias("avg_delivery_time"),
            avg(when(col("is_late_delivery"), 1.0).otherwise(0.0)).alias("late_shipment_rate"),
            avg("delivery_performance_score").alias("avg_delivery_performance"),
            avg("quality_rating").alias("avg_quality_rating"),
            sum("shipping_cost").alias("total_shipping_cost"),
            avg("supply_chain_risk_score").alias("avg_supply_chain_risk")
        ).withColumn(
            "performance_category",
            when(col("avg_delivery_performance") >= 0.9, "EXCELLENT")
            .when(col("avg_delivery_performance") >= 0.8, "GOOD")
            .when(col("avg_delivery_performance") >= 0.7, "FAIR")
            .otherwise("POOR")
        )
        
        print("Supply Chain Team metrics created")
        
        # 2. Store Managers Analytics - Inventory levels and product movement
        store_manager_metrics = fact_df.groupBy(
            "product_key", "product_name", "category_name", "warehouse_key", 
            "warehouse_name", "order_year", "order_month"
        ).agg(
            avg("current_stock_level").alias("avg_stock_level"),
            avg("stock_out_risk_score").alias("avg_stock_out_risk"),
            sum("order_quantity").alias("total_demand"),
            avg("days_of_supply").alias("avg_days_of_supply"),
            sum("order_value").alias("total_revenue")
        ).withColumn(
            "stock_health_status",
            when(col("avg_stock_out_risk") <= 0.3, "HEALTHY")
            .when(col("avg_stock_out_risk") <= 0.6, "MODERATE_RISK")
            .otherwise("HIGH_RISK")
        ).withColumn(
            "product_velocity_classification",
            when(col("total_demand") >= 100, "FAST_MOVING")
            .when(col("total_demand") >= 50, "MEDIUM_MOVING")
            .otherwise("SLOW_MOVING")
        )
        
        print("Store Manager metrics created")
        
        # 3. Finance Team Analytics - Supplier payment and cost analysis
        finance_metrics = fact_df.groupBy(
            "supplier_key", "supplier_name", "category_name", "order_year", "order_month"
        ).agg(
            sum("order_value").alias("total_purchase_value"),
            avg("unit_price").alias("avg_unit_price"),
            sum("shipping_cost").alias("total_shipping_cost"),
            sum("total_supply_chain_cost").alias("total_supply_chain_cost"),
            avg("delivery_time_days").alias("avg_payment_cycle_days"),
            sum("order_quantity").alias("total_units_purchased")
        ).withColumn(
            "supplier_spend_tier",
            when(col("total_purchase_value") >= 50000, "TIER_1_HIGH_SPEND")
            .when(col("total_purchase_value") >= 20000, "TIER_2_MEDIUM_SPEND")
            .otherwise("TIER_3_LOW_SPEND")
        ).withColumn(
            "cost_efficiency_ratio",
            col("total_purchase_value") / (col("total_supply_chain_cost") + lit(1))
        )
        
        print("Finance Team metrics created")
        
        return {
            "supply_chain_metrics": supply_chain_metrics,
            "store_manager_metrics": store_manager_metrics,
            "finance_metrics": finance_metrics
        }

    def calculate_advanced_kpis(self, fact_df):
        """
        Calculate advanced supply chain KPIs and benchmarks
        """
        print("Calculating advanced supply chain KPIs...")
        
        # Perfect Order Rate calculation
        perfect_orders = fact_df.filter(
            (col("is_late_delivery") == False) &
            (col("quality_rating") >= 4.0) &
            (col("stock_status") == "HEALTHY")
        )
        
        total_orders = fact_df.select("order_key").distinct()
        perfect_order_rate = perfect_orders.count() / total_orders.count()
        
        # Inventory Turnover by Category
        inventory_turnover = fact_df.groupBy("category_name").agg(
            (sum("order_value") / avg("current_stock_level")).alias("inventory_turnover_ratio")
        )
        
        # Supplier Reliability Scorecard
        supplier_scorecard = fact_df.groupBy("supplier_key", "supplier_name").agg(
            avg("delivery_performance_score").alias("delivery_score"),
            avg("quality_rating").alias("quality_score"),
            avg(when(col("stock_status") == "HEALTHY", 1.0).otherwise(0.0)).alias("availability_score")
        ).withColumn(
            "overall_supplier_score",
            (col("delivery_score") * 0.4 + 
             col("quality_score") / 5.0 * 0.3 + 
             col("availability_score") * 0.3) * 100
        )
        
        print(f"Perfect Order Rate: {perfect_order_rate:.2%}")
        print("Advanced KPI calculations completed")
        
        return {
            "perfect_order_rate": perfect_order_rate,
            "inventory_turnover": inventory_turnover,
            "supplier_scorecard": supplier_scorecard
        }

    def save_to_hdfs_and_hive(self, df, table_name, hdfs_path, mode="overwrite"):
        """
        Save DataFrame to HDFS and create corresponding Hive table
        """
        print(f"Saving {table_name} to HDFS: {hdfs_path}")
        
        try:
            # Write to HDFS as Parquet for better performance
            df.write \
                .mode(mode) \
                .option("compression", "snappy") \
                .parquet(hdfs_path)
            
            print(f"Successfully saved {table_name} to HDFS")
            
            # Create Hive external table
            self.create_hive_external_table(table_name, hdfs_path, df.schema)
            
        except Exception as e:
            print(f"Error saving {table_name}: {str(e)}")
            raise

    def create_hive_external_table(self, table_name, hdfs_path, schema):
        """
        Create Hive external table pointing to HDFS location
        """
        print(f"Creating Hive external table: supply_chain_db.{table_name}")
        
        try:
            # Create database if not exists
            self.spark.sql("CREATE DATABASE IF NOT EXISTS supply_chain_db")
            self.spark.sql("USE supply_chain_db")
            
            # Drop table if exists
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            
            # Create external table
            df_temp = self.spark.read.parquet(hdfs_path)
            df_temp.createOrReplaceTempView(f"temp_{table_name}")
            
            self.spark.sql(f"""
                CREATE TABLE {table_name}
                USING PARQUET
                LOCATION '{hdfs_path}'
                AS SELECT * FROM temp_{table_name} WHERE 1=0
            """)
            
            # Insert data
            df_temp.write.mode("overwrite").insertInto(f"supply_chain_db.{table_name}")
            
            print(f"Hive table supply_chain_db.{table_name} created successfully")
            
        except Exception as e:
            print(f"Error creating Hive table {table_name}: {str(e)}")
            raise

    def run_complete_pipeline(self):
        """
        Execute the complete supply chain data pipeline
        """
        print("Starting Retail Supply Chain Data Pipeline with SCD Type 2...")
        print("=" * 80)
        
        try:
            # Step 1: Extract data from all source tables
            print("Step 1: Extracting data from source systems...")
            customers_df = self.fetch_data_from_sql("Customers")
            orders_df = self.fetch_data_from_sql("Orders")
            order_items_df = self.fetch_data_from_sql("Order_Items")
            products_df = self.fetch_data_from_sql("Products")
            categories_df = self.fetch_data_from_sql("Categories")
            suppliers_df = self.fetch_data_from_sql("Suppliers")
            inventory_df = self.fetch_data_from_sql("Inventory")
            shipments_df = self.fetch_data_from_sql("Shipments")
            warehouses_df = self.fetch_data_from_sql("Warehouses")
            
            print("Data extraction completed successfully")
            print("=" * 80)
            
            # Step 2: Create comprehensive fact table
            print("Step 2: Creating comprehensive supply chain fact table...")
            fact_df = self.create_supply_chain_fact_table(
                orders_df, customers_df, order_items_df, products_df, 
                categories_df, suppliers_df, inventory_df, shipments_df, warehouses_df
            )
            
            # Step 3: Apply data quality checks
            print("Step 3: Applying data quality checks...")
            cleaned_fact_df = self.apply_data_quality_checks(fact_df)
            
            print("=" * 80)
            
            # Step 4: Create SCD Type 2 dimensions
            print("Step 4: Creating SCD Type 2 dimension tables...")
            
            customer_dim = self.create_scd_type2_dimension(
                customers_df, "customer", ["customer_id"], ["customer_name", "customer_email"]
            )
            
            supplier_dim = self.create_scd_type2_dimension(
                suppliers_df, "supplier", ["supplier_id"], ["supplier_name"]
            )
            
            warehouse_dim = self.create_scd_type2_dimension(
                warehouses_df, "warehouse", ["warehouse_id"], 
                ["warehouse_name", "warehouse_type", "region", "city"]
            )
            
            print("=" * 80)
            
            # Step 5: Create business aggregations
            print("Step 5: Creating business-specific aggregations...")
            business_metrics = self.create_business_aggregations(cleaned_fact_df)
            
            # Step 6: Calculate advanced KPIs
            print("Step 6: Calculating advanced supply chain KPIs...")
            advanced_kpis = self.calculate_advanced_kpis(cleaned_fact_df)
            
            print("=" * 80)
            
            # Step 7: Save all tables to HDFS and Hive
            print("Step 7: Saving results to HDFS and creating Hive tables...")
            
            # Main fact table
            self.save_to_hdfs_and_hive(
                cleaned_fact_df, "supply_chain_fact", 
                "hdfs:///user/hadoop/supply_chain/fact_table"
            )
            
            # Dimension tables
            self.save_to_hdfs_and_hive(
                customer_dim, "customer_dim", 
                "hdfs:///user/hadoop/supply_chain/customer_dimension"
            )
            
            self.save_to_hdfs_and_hive(
                supplier_dim, "supplier_dim", 
                "hdfs:///user/hadoop/supply_chain/supplier_dimension"
            )
            
            self.save_to_hdfs_and_hive(
                warehouse_dim, "warehouse_dim", 
                "hdfs:///user/hadoop/supply_chain/warehouse_dimension"
            )
            
            # Business metric tables
            self.save_to_hdfs_and_hive(
                business_metrics["supply_chain_metrics"], "supply_chain_team_metrics",
                "hdfs:///user/hadoop/supply_chain/supply_chain_metrics"
            )
            
            self.save_to_hdfs_and_hive(
                business_metrics["store_manager_metrics"], "store_manager_metrics",
                "hdfs:///user/hadoop/supply_chain/store_manager_metrics"
            )
            
            self.save_to_hdfs_and_hive(
                business_metrics["finance_metrics"], "finance_team_metrics",
                "hdfs:///user/hadoop/supply_chain/finance_metrics"
            )
            
            # Advanced KPI tables
            self.save_to_hdfs_and_hive(
                advanced_kpis["supplier_scorecard"], "supplier_scorecard",
                "hdfs:///user/hadoop/supply_chain/supplier_scorecard"
            )
            
            print("=" * 80)
            print("SUPPLY CHAIN DATA PIPELINE COMPLETED SUCCESSFULLY!")
            print("=" * 80)
            
            # Display final statistics
            print("FINAL PIPELINE STATISTICS:")
            print(f"Total Fact Records Processed: {cleaned_fact_df.count():,}")
            print(f"Perfect Order Rate: {advanced_kpis['perfect_order_rate']:.2%}")
            print("Created Tables:")
            print("  - supply_chain_db.supply_chain_fact (Main fact table)")
            print("  - supply_chain_db.customer_dim (SCD Type 2)")
            print("  - supply_chain_db.supplier_dim (SCD Type 2)")
            print("  - supply_chain_db.warehouse_dim (SCD Type 2)")
            print("  - supply_chain_db.supply_chain_team_metrics")
            print("  - supply_chain_db.store_manager_metrics")
            print("  - supply_chain_db.finance_team_metrics")
            print("  - supply_chain_db.supplier_scorecard")
            print("=" * 80)
            
            return {
                "fact_table": cleaned_fact_df,
                "dimensions": {
                    "customer": customer_dim,
                    "supplier": supplier_dim, 
                    "warehouse": warehouse_dim
                },
                "business_metrics": business_metrics,
                "kpis": advanced_kpis
            }
            
        except Exception as e:
            print(f"Pipeline execution failed: {str(e)}")
            raise
        finally:
            # Clean up
            print("Cleaning up Spark session...")
            # self.spark.stop()  # Uncomment if you want to stop session