"""
Configuration file for Retail Supply Chain Warehouse SCDs Project
"""

# Database Configuration
DB_CONFIG = {
    "host": "database-1.cpyaoguwudqo.ap-south-1.rds.amazonaws.com",
    "port": "5432",
    "database": "retail_supply_chain_db",
    "user": "postgres",
    "password": "mypostgreypassword",
    "driver": "org.postgresql.Driver"
}

# JDBC URL
JDBC_URL = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# HDFS Paths
HDFS_PATHS = {
    "fact_table": "hdfs:///user/hadoop/supply_chain/fact_table",
    "customer_dim": "hdfs:///user/hadoop/supply_chain/customer_dimension",
    "supplier_dim": "hdfs:///user/hadoop/supply_chain/supplier_dimension",
    "warehouse_dim": "hdfs:///user/hadoop/supply_chain/warehouse_dimension",
    "supply_chain_metrics": "hdfs:///user/hadoop/supply_chain/supply_chain_metrics",
    "store_manager_metrics": "hdfs:///user/hadoop/supply_chain/store_manager_metrics",
    "finance_metrics": "hdfs:///user/hadoop/supply_chain/finance_metrics",
    "supplier_scorecard": "hdfs:///user/hadoop/supply_chain/supplier_scorecard"
}

# Spark Configuration
SPARK_CONFIG = {
    "app_name": "retail_supply_chain_scd_pipeline",
    "master": "yarn",
    "config": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.hive.convertMetastoreParquet": "true"
    }
}

# SCD Configuration
SCD_CONFIG = {
    "high_date": "9999-12-31",
    "current_flag": True,
    "record_version": 1
}

# Business Rules
BUSINESS_RULES = {
    "performance_categories": {
        "EXCELLENT": 0.9,
        "GOOD": 0.8,
        "FAIR": 0.7,
        "POOR": 0.0
    },
    "stock_health_status": {
        "HEALTHY": 0.3,
        "MODERATE_RISK": 0.6,
        "HIGH_RISK": 1.0
    },
    "product_velocity": {
        "FAST_MOVING": 100,
        "MEDIUM_MOVING": 50,
        "SLOW_MOVING": 0
    },
    "supplier_spend_tiers": {
        "TIER_1_HIGH_SPEND": 50000,
        "TIER_2_MEDIUM_SPEND": 20000,
        "TIER_3_LOW_SPEND": 0
    }
}