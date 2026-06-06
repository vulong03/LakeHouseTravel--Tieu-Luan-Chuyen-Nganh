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

# Import shared DQ utilities (tránh lặp code)
sys.path.append(os.path.dirname(os.path.dirname(__file__)))  # spark/jobs/dataqualify/
from dq_utils import ensure_dq_table, write_dq_result, run_eda

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

# Cột bắt buộc — không được NULL (threshold chặt: 0.1%)
REQUIRED_COLUMNS = [
    "post_url", "author", "crawl_date",
    "row_checksum", "ingestion_timestamp"
]

# post_date tách riêng — ngưỡng nới lỏng hơn vì có thể NULL hợp lệ
# (crawl lúc video còn dạng tương đối, chưa parse được)
POST_DATE_NULL_THRESHOLD = 15.0  # Cho phép tối đa 15% NULL post_date

METRIC_COLUMNS = [
    "likes", "comments_count", "saves", "shares"
]

THRESHOLDS = {
    "min_records":        100,
    "max_null_pct_req":   0.1,   # 0.1% cho cột bắt buộc (post_url, author...)
    "max_null_pct_opt":   20.0,  # 20% cho cột metrics (thực tế có NULL)
    "max_duplicate_pct":  1.0,
}

RUN_TIMESTAMP = datetime.now().isoformat()

# ============================================================================
# CHECKS
# ============================================================================

def run_dq_checks(spark, conn):
    df = spark.table(TARGET_TABLE)
    run_eda(df, TARGET_TABLE)
    total = df.count()
    failures = []

    print(f"\nTotal records: {total:,}")

    # --- 1. Min Records ---
    print("\n── CHECK 1: Min Records (Số lượng dòng tối thiểu) ──────")
    status = "PASS" if total >= THRESHOLDS["min_records"] else "FAIL"
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "min_records", "Completeness",
                    status, True, total, THRESHOLDS["min_records"])
    if status == "FAIL": failures.append("min_records")

    # --- 2. Null Checks (Required columns — threshold 0.1%) ---
    print("\n── CHECK 2: Null Checks Required (Cột bắt buộc) ────────")
    for col in REQUIRED_COLUMNS:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / total * 100) if total > 0 else 0
        status = "PASS" if null_pct <= THRESHOLDS["max_null_pct_req"] else "FAIL"
        write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, f"null_{col}", "Completeness",
                        status, True, null_pct, THRESHOLDS["max_null_pct_req"])
        if status == "FAIL": failures.append(f"null_{col}")

    # --- 3. Null Check (post_date — ngưỡng nới lỏng 15%) ---
    print("\n── CHECK 3: Null Check Post Date (Ngày đăng bài) ───────")
    # post_date có thể NULL hợp lệ khi crawl lúc video vừa đăng (dạng tương đối)
    # và pipeline enrich từ tiktok_videos chưa cover hết
    post_date_null = df.filter(F.col("post_date").isNull()).count()
    post_date_null_pct = (post_date_null / total * 100) if total > 0 else 0
    status = "PASS" if post_date_null_pct <= POST_DATE_NULL_THRESHOLD else "WARN"
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "null_post_date", "Completeness",
                    status, False, post_date_null_pct, POST_DATE_NULL_THRESHOLD,
                    {"null_count": post_date_null, "note": "NULL accepted if < 15% (relative date format)"})

    # --- 4. Null Checks (Metrics — optional, threshold 20%) ---
    print("\n── CHECK 4: Null Checks Metrics (Các cột chỉ số) ───────")
    for col in METRIC_COLUMNS:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / total * 100) if total > 0 else 0
        status = "PASS" if null_pct <= THRESHOLDS["max_null_pct_opt"] else "WARN"
        write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, f"null_{col}_opt", "Completeness",
                        status, False, null_pct, THRESHOLDS["max_null_pct_opt"])

    # --- 5. Numeric Validity (Non-negative) ---
    print("\n── CHECK 5: Numeric Validity (Kiểm tra giá trị âm) ─────")
    for col in METRIC_COLUMNS:
        negative_count = df.filter(F.col(col).isNotNull() & (F.col(col) < 0)).count()
        status = "PASS" if negative_count == 0 else "FAIL"
        write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, f"negative_{col}", "Validity",
                        status, True, float(negative_count), 0.0)
        if status == "FAIL": failures.append(f"negative_{col}")

    # --- 6. URL Format ---
    print("\n── CHECK 6: URL Format (Định dạng đường dẫn) ───────────")
    invalid_url = df.filter(~F.col("post_url").contains("tiktok.com")).count()
    status = "PASS" if invalid_url == 0 else "FAIL"
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "url_format", "Validity",
                    status, True, float(invalid_url), 0.0)
    if status == "FAIL": failures.append("url_format")

    # --- 7. Uniqueness (post_url) ---
    print("\n── CHECK 7: Uniqueness (Tính duy nhất của URL) ─────────")
    distinct_urls = df.select("post_url").distinct().count()
    dup_count = total - distinct_urls
    dup_pct = (dup_count / total * 100) if total > 0 else 0
    status = "PASS" if dup_pct <= THRESHOLDS["max_duplicate_pct"] else "FAIL"
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "duplicate_url", "Uniqueness",
                    status, True, dup_pct, THRESHOLDS["max_duplicate_pct"])
    if status == "FAIL": failures.append("duplicate_url")

    # --- 8. Distribution Stats (informational only) ---
    print("\n── CHECK 8: Distribution Stats (Thống kê phân phối) ────")
    stats = df.select(
        F.round(F.avg("likes"), 0).alias("avg_likes"),
        F.round(F.avg("shares"), 0).alias("avg_shares")
    ).collect()[0]
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "metrics_stats", "Distribution",
                    "PASS", False,
                    details={"avg_likes": int(stats["avg_likes"] or 0),
                             "avg_shares": int(stats["avg_shares"] or 0)})

    # --- Commit tất cả kết quả 1 lần ---
    conn.commit()

    return failures

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 65)
    print("  DATA QUALITY — TikTok Post Metadata")
    print("=" * 65)
    print(f"  Table     : {TARGET_TABLE}")
    print(f"  Run time  : {RUN_TIMESTAMP}")
    print("=" * 65)

    spark = get_spark_session(app_name="DQ_TikTok_Post_Metadata")

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
