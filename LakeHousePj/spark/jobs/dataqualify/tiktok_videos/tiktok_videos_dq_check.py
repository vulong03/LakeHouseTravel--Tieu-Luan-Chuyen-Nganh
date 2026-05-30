"""
Data Quality Checks — TikTok Videos (Silver Layer)
===================================================
Kiểm tra chất lượng dữ liệu cho: lakehouse.silver.tiktok_videos

Các nhóm kiểm tra:
  1. Completeness  : NULL rate các cột quan trọng (url, keyword, region).
  2. Validity      : url đúng format TikTok, posted_date không ở tương lai.
  3. Consistency   : region phải thuộc danh sách vùng miền Việt Nam.
  4. Uniqueness    : Trùng lặp dựa trên url.

Exit code: 0 = PASS, 1 = FAIL
"""

import sys
import os
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from pyspark.sql import functions as F
import psycopg2

# Import shared DQ utilities (tránh lặp code)
sys.path.append(os.path.dirname(os.path.dirname(__file__)))  # spark/jobs/dataqualify/
from dq_utils import ensure_dq_table, write_dq_result

# ============================================================================
# CONFIG
# ============================================================================

POSTGRES_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'metastore_db',
    'user': 'lakehouse_user',
    'password': 'lakehouse_pass'
}

TARGET_TABLE = "lakehouse.silver.tiktok_videos"

# Cột bắt buộc — không được NULL
REQUIRED_COLUMNS = [
    "url", "keyword", "region", "row_checksum",
    "ingestion_timestamp", "source_file"
]

# Danh sách 8 vùng miền hợp lệ của Việt Nam
VALID_REGIONS = [
    "South_Central_Coast", "Mekong_Delta", "Central_Highlands",
    "Northeast", "North_Central_Coast", "Northwest",
    "Red_River_Delta", "Southeast"
]

THRESHOLDS = {
    "min_records":       100,
    "max_duplicate_pct": 1.0,
}

RUN_TIMESTAMP = datetime.now().isoformat()

# ============================================================================
# CHECKS
# ============================================================================

def run_dq_checks(spark, conn):
    df = spark.table(TARGET_TABLE)
    total = df.count()
    failures = []

    print(f"\nTotal records: {total:,}")

    # --- 1. Min Records ---
    status = "PASS" if total >= THRESHOLDS["min_records"] else "FAIL"
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "min_records", "Completeness",
                    status, True, total, THRESHOLDS["min_records"])
    if status == "FAIL": failures.append("min_records")

    # --- 2. Null Checks ---
    for col in REQUIRED_COLUMNS:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / total * 100) if total > 0 else 0
        status = "PASS" if null_count == 0 else "FAIL"
        write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, f"null_{col}", "Completeness",
                        status, True, null_pct, 0.0)
        if status == "FAIL": failures.append(f"null_{col}")

    # --- 3. URL Format ---
    invalid_url = df.filter(~F.col("url").contains("tiktok.com")).count()
    status = "PASS" if invalid_url == 0 else "FAIL"
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "url_format", "Validity",
                    status, True, float(invalid_url), 0.0)
    if status == "FAIL": failures.append("url_format")

    # --- 4. Region Consistency ---
    invalid_region = df.filter(~F.col("region").isin(VALID_REGIONS)).count()
    status = "PASS" if invalid_region == 0 else "FAIL"
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "region_consistency", "Consistency",
                    status, True, float(invalid_region), 0.0)
    if status == "FAIL": failures.append("region_consistency")

    # --- 5. Uniqueness (url) ---
    distinct_urls = df.select("url").distinct().count()
    dup_count = total - distinct_urls
    dup_pct = (dup_count / total * 100) if total > 0 else 0
    status = "PASS" if dup_pct <= THRESHOLDS["max_duplicate_pct"] else "FAIL"
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "duplicate_url", "Uniqueness",
                    status, True, dup_pct, THRESHOLDS["max_duplicate_pct"])
    if status == "FAIL": failures.append("duplicate_url")

    # --- 6. Date Validity (posted_date không ở tương lai) ---
    future_date = df.filter(F.col("posted_date") > F.current_date()).count()
    status = "PASS" if future_date == 0 else "WARN"
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "future_posted_date", "Validity",
                    status, False, float(future_date), 0.0)

    # --- Commit tất cả kết quả 1 lần ---
    conn.commit()

    return failures

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 65)
    print("  DATA QUALITY — TikTok Videos")
    print("=" * 65)
    print(f"  Table     : {TARGET_TABLE}")
    print(f"  Run time  : {RUN_TIMESTAMP}")
    print("=" * 65)

    spark = get_spark_session(app_name="DQ_TikTok_Videos")

    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        ensure_dq_table(conn)

        failures = run_dq_checks(spark, conn)

        print("\n" + "=" * 65)
        print("  SUMMARY")
        print("=" * 65)
        if failures:
            print(f"  ❌ DQ FAILED — {len(failures)} critical check(s) violated:")
            for f in failures:
                print(f"     • {f}")
            sys.exit(1)
        else:
            print("  ✅ ALL CRITICAL CHECKS PASSED")
            sys.exit(0)

    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals(): conn.close()
        spark.stop()

if __name__ == "__main__":
    main()
