"""
Compare Bronze CSV source data vs Silver table data quality
"""
import pandas as pd
import pyarrow.parquet as pq
import os
from pathlib import Path

# Path to Bronze CSV
bronze_csv = "D:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/data/raw/booking/vietnam_hotels_reviews.csv"

print("="*80)
print("🔍 DATA QUALITY COMPARISON: BRONZE CSV vs SILVER TABLE")
print("="*80)

# Read Bronze CSV
print("\n📂 Reading Bronze CSV source...")
print(f"   Path: {bronze_csv}")

df = pd.read_csv(bronze_csv)

print(f"   Total records in Bronze CSV: {len(df):,}")
print(f"   Columns: {list(df.columns)}")

# Show column data types
print(f"\n📊 BRONZE CSV - DATA TYPES:")
for col in df.columns:
    print(f"   {col}: {df[col].dtype}")

# Check NULL values in all columns
print(f"\n❌ BRONZE CSV - NULL VALUES BY COLUMN:")
for col in df.columns:
    null_count = df[col].isna().sum()
    null_pct = null_count/len(df)*100
    print(f"   {col}: {null_count:,} ({null_pct:.2f}%)")

# Parse review_date (Vietnamese format)
print("\n� Parsing Vietnamese date format...")
import re

def parse_vietnamese_date(date_str):
    """Parse: 'Ngày đánh giá: ngày DD tháng MM năm YYYY' -> YYYY-MM-DD"""
    if pd.isna(date_str):
        return None
    
    pattern = r'ngày (\d+) tháng (\d+) năm (\d{4})'
    match = re.search(pattern, str(date_str))
    
    if match:
        day, month, year = match.groups()
        try:
            return pd.Timestamp(f"{year}-{month}-{day}")
        except:
            return None
    return None

# Apply parsing
df['parsed_date'] = df['review_date'].apply(parse_vietnamese_date)

parsed_count = df['parsed_date'].notna().sum()
print(f"   Successfully parsed: {parsed_count:,} ({parsed_count/len(df)*100:.2f}%)")

# Filter to valid dates only
valid_df = df[df['parsed_date'].notna()].copy()
print(f"   Working with {len(valid_df):,} valid dates")

# Extract year and month
valid_df['year'] = valid_df['parsed_date'].dt.year
valid_df['month'] = valid_df['parsed_date'].dt.month
valid_df['year_month'] = valid_df['parsed_date'].dt.to_period('M')

print(f"\n📅 DATE RANGE:")
print(f"   Min date: {valid_df['parsed_date'].min()}")
print(f"   Max date: {valid_df['parsed_date'].max()}")

# Year distribution
print(f"\n📊 YEAR DISTRIBUTION:")
year_dist = valid_df['year'].value_counts().sort_index()
for year, count in year_dist.items():
    print(f"   {int(year)}: {count:,} records")

# Year-Month distribution
print(f"\n� YEAR-MONTH DISTRIBUTION (Top 50):")
year_month_dist = valid_df['year_month'].value_counts().sort_index()
print("\n   Year-Month │ Count")
print("   " + "─"*30)
for ym, count in year_month_dist.head(50).items():
    print(f"   {ym}    │ {count:>8,}")

# Summary statistics by year
print(f"\n📈 MONTHLY STATISTICS BY YEAR:")
for year in sorted(valid_df['year'].unique()):
    year_data = valid_df[valid_df['year'] == year]
    monthly = year_data.groupby('month').size()
    
    print(f"\n   Year {int(year)}: {len(year_data):,} total records")
    print(f"      Months with data: {len(monthly)} months")
    print(f"      Avg per month: {len(year_data)/len(monthly):,.0f}")
    print(f"      Min: {monthly.min():,} │ Max: {monthly.max():,}")
    
    # Show monthly breakdown
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    print(f"      Monthly breakdown:")
    for month in sorted(monthly.index):
        count = monthly[month]
        print(f"         {month_names[int(month)-1]} ({int(month):02d}): {count:>7,}")

print("\n" + "="*80)
print("✅ ANALYSIS COMPLETE")
print("="*80)

# Generate batch processing recommendations
print(f"\n💡 BATCH PROCESSING RECOMMENDATIONS:")
print(f"\n   Strategy 1: Process by YEAR (3 batches)")
for year, count in year_dist.items():
    print(f"      {int(year)}: {count:,} records")

print(f"\n   Strategy 2: Process by YEAR-MONTH (monthly batches)")
print(f"      Total batches needed: {len(year_month_dist)}")
print(f"      Average batch size: {len(valid_df)/len(year_month_dist):,.0f} records")

# Find largest batches
print(f"\n   ⚠️  LARGEST MONTHLY BATCHES (Top 10):")
for ym, count in year_month_dist.nlargest(10).items():
    print(f"      {ym}: {count:,} records")

print(f"\n   ✅ RECOMMENDED APPROACH:")
print(f"      Use YEAR-MONTH batches to keep each batch under 100K records")
print(f"      This ensures stable processing without memory issues")
