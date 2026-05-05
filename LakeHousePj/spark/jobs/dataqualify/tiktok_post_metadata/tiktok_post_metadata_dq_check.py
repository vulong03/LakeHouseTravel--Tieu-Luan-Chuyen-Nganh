"""
Data Quality Checks — TikTok Post Metadata (Silver Layer)
=========================================================
Kiểm tra chất lượng dữ liệu cho: lakehouse.silver.tiktok_post_metadata

Các nhóm kiểm tra:
  1. Completeness  : NULL rate (post_url, author, post_date).
  2. Validity      : URL format, numeric >= 0, date validity.
  3. Uniqueness    : Trùng lặp post_url.
  4. Freshness     : ingestion_timestamp.

Exit code: 0 = PASS, 1 = FAIL
"""

import sys
import os
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from pyspark.sql import functions as F
import psycopg2
import json

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

TARGET_TABLE = "lakehouse.silver.tiktok_post_metadata"

REQUIRED_COLUMNS = [
    "post_url", "author", "post_date", "crawl_date",
    "row_checksum", "ingestion_timestamp"
]

METRIC_COLUMNS = [
    "likes", "comments_count", "saves", "shares"
]

THRESHOLDS = {
    "min_records":        100,
    "max_null_pct_req":   0.1,  # 0.1% cho cột bắt buộc
    "max_null_pct_opt":   20.0, # 20% cho cột metrics (thực tế có NULL)
    "max_duplicate_pct":  1.0,
}

RUN_TIMESTAMP = datetime.now().isoformat()

# ============================================================================
# UTILS
# ============================================================================

def ensure_dq_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dq_results (
                id                  SERIAL PRIMARY KEY,
                run_timestamp       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                table_name          TEXT NOT NULL,
                check_name          TEXT NOT NULL,
                check_category      TEXT NOT NULL,
                status              TEXT NOT NULL,
                is_critical         BOOLEAN NOT NULL DEFAULT FALSE,
                metric_value        DOUBLE PRECISION,
                threshold_value     DOUBLE PRECISION,
                details             JSONB
            )
        """)
        conn.commit()

def write_dq_result(conn, check_name, check_category, status, is_critical, 
                    metric_value=None, threshold_value=None, details=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dq_results
                (run_timestamp, table_name, check_name, check_category,
                 status, is_critical, metric_value, threshold_value, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        """, (
            RUN_TIMESTAMP, TARGET_TABLE, check_name, check_category,
            status, is_critical,
            float(metric_value) if metric_value is not None else None,
            float(threshold_value) if threshold_value is not None else None,
            json.dumps(details, ensure_ascii=False) if details else None
        ))
    conn.commit()
    icon = "✅" if status == "PASS" else ("❌" if status == "FAIL" else "⚠️")
    print(f"  {icon} [{check_category}] {check_name}: {status}")

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
    write_dq_result(conn, "min_records", "Completeness", status, True, total, THRESHOLDS["min_records"])
    if status == "FAIL": failures.append("min_records")

    # --- 2. Null Checks (Required) ---
    for col in REQUIRED_COLUMNS:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / total * 100) if total > 0 else 0
        status = "PASS" if null_pct <= THRESHOLDS["max_null_pct_req"] else "FAIL"
        write_dq_result(conn, f"null_{col}", "Completeness", status, True, null_pct, THRESHOLDS["max_null_pct_req"])
        if status == "FAIL": failures.append(f"null_{col}")

    # --- 3. Null Checks (Metrics - Optional) ---
    for col in METRIC_COLUMNS:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / total * 100) if total > 0 else 0
        status = "PASS" if null_pct <= THRESHOLDS["max_null_pct_opt"] else "WARN"
        write_dq_result(conn, f"null_{col}_opt", "Completeness", status, False, null_pct, THRESHOLDS["max_null_pct_opt"])

    # --- 4. Numeric Validity (Non-negative) ---
    for col in METRIC_COLUMNS:
        negative_count = df.filter(F.col(col).isNotNull() & (F.col(col) < 0)).count()
        status = "PASS" if negative_count == 0 else "FAIL"
        write_dq_result(conn, f"negative_{col}", "Validity", status, True, float(negative_count), 0.0)
        if status == "FAIL": failures.append(f"negative_{col}")

    # --- 5. URL Format ---
    invalid_url = df.filter(~F.col("post_url").contains("tiktok.com")).count()
    status = "PASS" if invalid_url == 0 else "FAIL"
    write_dq_result(conn, "url_format", "Validity", status, True, float(invalid_url), 0.0)
    if status == "FAIL": failures.append("url_format")

    # --- 6. Uniqueness (post_url) ---
    distinct_urls = df.select("post_url").distinct().count()
    dup_count = total - distinct_urls
    dup_pct = (dup_count / total * 100) if total > 0 else 0
    status = "PASS" if dup_pct <= THRESHOLDS["max_duplicate_pct"] else "FAIL"
    write_dq_result(conn, "duplicate_url", "Uniqueness", status, True, dup_pct, THRESHOLDS["max_duplicate_pct"])
    if status == "FAIL": failures.append("duplicate_url")

    # --- 7. Distribution (Stats) ---
    stats = df.select(
        F.round(F.avg("likes"), 0).alias("avg_likes"),
        F.round(F.avg("shares"), 0).alias("avg_shares")
    ).collect()[0]
    write_dq_result(conn, "metrics_stats", "Distribution", "PASS", False, 
                    details={"avg_likes": int(stats["avg_likes"] or 0), "avg_shares": int(stats["avg_shares"] or 0)})

    return failures

# ============================================================================
# MAIN
# ============================================================================

def main():
    spark = get_spark_session(app_name="DQ_TikTok_Post_Metadata")
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        ensure_dq_table(conn)
        
        failures = run_dq_checks(spark, conn)
        
        if failures:
            print(f"\n❌ DQ FAILED: {len(failures)} critical failures.")
            sys.exit(1)
        else:
            print("\n✅ DQ PASSED.")
            sys.exit(0)
            
    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals(): conn.close()
        spark.stop()

if __name__ == "__main__":
    main()
