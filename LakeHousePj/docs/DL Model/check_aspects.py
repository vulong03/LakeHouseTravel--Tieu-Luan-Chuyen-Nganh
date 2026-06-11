import sys
sys.path.append("/opt/spark/jobs")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.spark_session import get_spark_session

def main():
    print("=" * 70)
    print("Checking Aspect Net Sentiment Values in Gold Table")
    print("=" * 70)

    spark = get_spark_session("Check_Aspect_Net_Sentiment")

    try:
        table_name = "gold.gold.fact_province_month_dl_features"
        print(f"Reading table: {table_name}")
        df = spark.table(table_name)
        
        # Show schema of aspect columns
        aspect_cols = [
            "avg_aspect_scenery", "avg_aspect_food", "avg_aspect_price",
            "avg_aspect_service", "avg_aspect_transport", "avg_aspect_accommodation"
        ]
        
        print("\n--- Summary Statistics of Aspect Net Sentiment Columns ---")
        df.select(aspect_cols).summary().show()
        
        print("\n--- Non-zero Sample Records (Province, YearMonth, Aspects) ---")
        # Filter for rows where at least one aspect has a non-zero net sentiment
        filter_expr = " OR ".join([f"{c} != 0.0" for c in aspect_cols])
        df_nz = df.filter(filter_expr).select(
            "province_name", "year_month", *aspect_cols
        ).orderBy(F.desc("year_month"))
        
        df_nz.show(30, truncate=False)
        
        # Count total rows and non-zero rows
        total_rows = df.count()
        nz_rows = df_nz.count()
        print(f"Total rows in table: {total_rows}")
        print(f"Rows with non-zero aspect net sentiment: {nz_rows}")

    except Exception as e:
        print(f"Error checking table: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
