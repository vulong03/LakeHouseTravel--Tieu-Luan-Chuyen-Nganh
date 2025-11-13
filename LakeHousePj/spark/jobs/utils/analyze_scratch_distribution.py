"""
Analyze review_date distribution in Scratch to determine batch processing strategy
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, count, to_date, regexp_extract

# Create Spark session
spark = SparkSession.builder \
    .appName("Analyze_Scratch_Distribution") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("="*80)
print("🔍 ANALYZING REVIEW_DATE DISTRIBUTION IN SCRATCH")
print("="*80)

# Read Scratch data
scratch_path = "s3a://scratch/pipeline/silver/hotels_reviews/run_20251110_162818"
print(f"\n📖 Reading from: {scratch_path}")

df = spark.read.parquet(scratch_path)
total_records = df.count()
print(f"   Total records: {total_records:,}")

# Parse review_date using simpler regex
print("\n🔧 Parsing review_date...")
from pyspark.sql.functions import when, concat_ws, lpad

# Extract day, month, year using regexp_extract
df_parsed = df.withColumn("day", regexp_extract(col("review_date"), r"ngày (\d+)", 1)) \
    .withColumn("month", regexp_extract(col("review_date"), r"tháng (\d+)", 1)) \
    .withColumn("year", regexp_extract(col("review_date"), r"năm (\d{4})", 1))

# Construct date string and parse
df_parsed = df_parsed.withColumn(
    "review_date_parsed",
    when(
        (col("day") != "") & (col("month") != "") & (col("year") != ""),
        to_date(concat_ws("-", col("year"), lpad(col("month"), 2, "0"), lpad(col("day"), 2, "0")), "yyyy-MM-dd")
    ).otherwise(None)
)

# Filter valid dates only
df_valid = df_parsed.filter(col("review_date_parsed").isNotNull())
valid_count = df_valid.count()
print(f"   Valid dates: {valid_count:,} ({valid_count/total_records*100:.1f}%)")

# Extract year and month
df_with_period = df_valid \
    .withColumn("year", year(col("review_date_parsed"))) \
    .withColumn("month", month(col("review_date_parsed")))

# Year distribution
print("\n📊 YEAR DISTRIBUTION:")
year_dist = df_with_period.groupBy("year") \
    .agg(count("*").alias("count")) \
    .orderBy("year")

year_dist.show(100, False)

# Year-Month distribution
print("\n📅 YEAR-MONTH DISTRIBUTION:")
year_month_dist = df_with_period.groupBy("year", "month") \
    .agg(count("*").alias("count")) \
    .orderBy("year", "month")

print("\nAll year-month combinations:")
year_month_dist.show(200, False)

# Find largest batches
print("\n⚠️  LARGEST MONTHLY BATCHES:")
year_month_dist.orderBy(col("count").desc()).show(20, False)

# Summary statistics
print("\n📈 SUMMARY STATISTICS:")
stats = year_month_dist.select("count").summary("count", "mean", "min", "max", "50%")
stats.show(False)

print("\n" + "="*80)
print("✅ ANALYSIS COMPLETE")
print("="*80)

spark.stop()
