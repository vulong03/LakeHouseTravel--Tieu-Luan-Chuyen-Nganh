import sys
sys.path.append("/opt/spark/jobs")

from pyspark.sql import SparkSession
from utils.spark_session import get_spark_session

def main():
    print("=" * 70)
    print("Checking Source Table fact_comment_nlp_v2 Columns")
    print("=" * 70)

    spark = get_spark_session("Check_Silver_ABSA")

    try:
        table_name = "gold.gold.fact_comment_nlp_v2"
        print(f"Reading table: {table_name}")
        df = spark.table(table_name)
        
        cols = [
            "aspect_scenery_pos", "aspect_scenery_neg",
            "aspect_food_pos", "aspect_food_neg",
            "aspect_price_pos", "aspect_price_neg",
            "aspect_service_pos", "aspect_service_neg",
            "aspect_transport_pos", "aspect_transport_neg",
            "aspect_accommodation_pos", "aspect_accommodation_neg"
        ]
        
        # Check count of non-zero rows for each column
        print("\n--- Non-zero Count and Sum for ABSA columns ---")
        for col in cols:
            nz_count = df.filter(f"{col} > 0.0").count()
            col_sum = df.select(col).groupBy().sum().collect()[0][0]
            print(f"{col}: Non-zero Count = {nz_count}, Sum = {col_sum}")

    except Exception as e:
        print(f"Error checking: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
