"""
Gold Layer - Dimension Date Configuration
"""

# PostgreSQL source connection
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "date_db"  # PostgreSQL converts to lowercase
POSTGRES_USER = "lakehouse_user"
POSTGRES_PASSWORD = "lakehouse_pass"
POSTGRES_DRIVER = "org.postgresql.Driver"

# Source table
SOURCE_TABLE = "dim_date"
SOURCE_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"

# Target table (catalog.database.table)
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "dim_date"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Business key
BUSINESS_KEY = ["date_sk"]

