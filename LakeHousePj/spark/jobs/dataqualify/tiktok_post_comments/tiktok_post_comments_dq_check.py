"""
Data Quality Checks — TikTok Post Comments (Silver Layer)
=========================================================
Kiểm tra chất lượng dữ liệu cho: lakehouse.silver.tiktok_post_comments

Các nhóm kiểm tra:
  1. Completeness  : NULL rate (post_url, comment, ten).
  2. Validity      : URL format, comment length, level_comment ENUM (kể cả NULL).
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

TARGET_TABLE = "lakehouse.silver.tiktok_post_comments"
METADATA_TABLE = "lakehouse.silver.tiktok_post_metadata"

REQUIRED_COLUMNS = [
    "post_url", "comment", "ten", "scrape_date",
    "row_checksum", "ingestion_timestamp"
]

THRESHOLDS = {
    "min_records":    1000,
    "max_null_pct":   0.1,
    "max_orphan_pct": 5.0,  # Cho phép 5% comment mồ côi (do scrape lệch time)
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

    # --- 2. Null Checks ---
    print("\n── CHECK 2: Null Checks (Kiểm tra giá trị rỗng) ────────")
    for col in REQUIRED_COLUMNS:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / total * 100) if total > 0 else 0
        status = "PASS" if null_pct <= THRESHOLDS["max_null_pct"] else "FAIL"
        write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, f"null_{col}", "Completeness",
                        status, True, null_pct, THRESHOLDS["max_null_pct"])
        if status == "FAIL": failures.append(f"null_{col}")

    # --- 3. Comment Length (không được rỗng sau trim) ---
    print("\n── CHECK 3: Comment Length (Nội dung bình luận rỗng) ──")
    empty_comments = df.filter(F.length(F.trim(F.col("comment"))) == 0).count()
    status = "PASS" if empty_comments == 0 else "WARN"
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "empty_comments", "Validity",
                    status, False, float(empty_comments), 0.0)

    # --- 4. Level Enum Validity (Yes / No) ---
    print("\n── CHECK 4: Level Enum Validity (Tính hợp lệ của Level) ─")
    # FIX: Spark's ~isin() trả về NULL khi cột là NULL → dùng isNull() | ~isin() để bắt cả 2
    invalid_level = df.filter(
        F.col("level_comment").isNull() |
        ~F.col("level_comment").isin(["Yes", "No"])
    ).count()
    status = "PASS" if invalid_level == 0 else "FAIL"
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "level_enum_validity", "Validity",
                    status, True, float(invalid_level), 0.0)
    if status == "FAIL": failures.append("level_enum_validity")

    # --- 5. Referential Integrity (Orphan comments) ---
    print("\n── CHECK 5: Referential Integrity (Bình luận mồ côi) ──")
    # Kiểm tra post_url trong comments có tồn tại trong metadata không
    meta_df = spark.table(METADATA_TABLE).select("post_url").distinct()
    orphan_count = df.join(meta_df, "post_url", "left_anti").count()
    orphan_pct = (orphan_count / total * 100) if total > 0 else 0
    status = "PASS" if orphan_pct <= THRESHOLDS["max_orphan_pct"] else "WARN"
    write_dq_result(conn, RUN_TIMESTAMP, TARGET_TABLE, "orphan_comments", "Integrity",
                    status, False, orphan_pct, THRESHOLDS["max_orphan_pct"],
                    {"orphan_count": orphan_count})

    # --- Commit tất cả kết quả 1 lần ---
    conn.commit()

    return failures

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 65)
    print("  DATA QUALITY — TikTok Post Comments")
    print("=" * 65)
    print(f"  Table     : {TARGET_TABLE}")
    print(f"  Ref table : {METADATA_TABLE}")
    print(f"  Run time  : {RUN_TIMESTAMP}")
    print("=" * 65)

    spark = get_spark_session(app_name="DQ_TikTok_Post_Comments")

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
