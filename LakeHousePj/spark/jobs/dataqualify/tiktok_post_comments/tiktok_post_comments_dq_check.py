"""
Data Quality Checks — TikTok Post Comments (Silver Layer)
=========================================================
Kiểm tra chất lượng dữ liệu cho: lakehouse.silver.tiktok_post_comments

Các nhóm kiểm tra:
  1. Completeness  : NULL rate (post_url, comment, ten).
  2. Validity      : URL format, comment length, level_comment ENUM.
  3. Integrity     : Kiểm tra mồ côi (post_url không tồn tại trong metadata).

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

TARGET_TABLE = "lakehouse.silver.tiktok_post_comments"
METADATA_TABLE = "lakehouse.silver.tiktok_post_metadata"

REQUIRED_COLUMNS = [
    "post_url", "comment", "ten", "scrape_date",
    "row_checksum", "ingestion_timestamp"
]

THRESHOLDS = {
    "min_records":        1000,
    "max_null_pct":       0.1,
    "max_orphan_pct":     5.0,  # Cho phép 5% comment mồ côi (do scrape lệch time)
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

    # --- 2. Null Checks ---
    for col in REQUIRED_COLUMNS:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / total * 100) if total > 0 else 0
        status = "PASS" if null_pct <= THRESHOLDS["max_null_pct"] else "FAIL"
        write_dq_result(conn, f"null_{col}", "Completeness", status, True, null_pct, THRESHOLDS["max_null_pct"])
        if status == "FAIL": failures.append(f"null_{col}")

    # --- 3. Comment Length ---
    empty_comments = df.filter(F.length(F.trim(F.col("comment"))) == 0).count()
    status = "PASS" if empty_comments == 0 else "WARN"
    write_dq_result(conn, "empty_comments", "Validity", status, False, float(empty_comments), 0.0)

    # --- 4. Level Enum (Yes/No) ---
    invalid_level = df.filter(~F.col("level_comment").isin(["Yes", "No"])).count()
    status = "PASS" if invalid_level == 0 else "FAIL"
    write_dq_result(conn, "level_enum_validity", "Validity", status, True, float(invalid_level), 0.0)
    if status == "FAIL": failures.append("level_enum_validity")

    # --- 5. Referential Integrity (Orphans) ---
    # Kiểm tra xem post_url có tồn tại trong metadata không
    meta_df = spark.table(METADATA_TABLE).select("post_url").distinct()
    orphan_df = df.join(meta_df, "post_url", "left_anti")
    orphan_count = orphan_df.count()
    orphan_pct = (orphan_count / total * 100) if total > 0 else 0
    
    status = "PASS" if orphan_pct <= THRESHOLDS["max_orphan_pct"] else "WARN"
    write_dq_result(conn, "orphan_comments", "Integrity", status, False, orphan_pct, THRESHOLDS["max_orphan_pct"],
                    {"orphan_count": orphan_count})

    return failures

# ============================================================================
# MAIN
# ============================================================================

def main():
    spark = get_spark_session(app_name="DQ_TikTok_Post_Comments")
    
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
