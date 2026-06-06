"""
Data Quality Checks — Hotels Reviews (Silver Layer)
===================================================
Kiểm tra chất lượng dữ liệu cho: lakehouse.silver.hotels_reviews

Bảng này có dữ liệu lớn (~1.5 triệu dòng), cần tối ưu hóa:
  - Sử dụng 1 pass aggregation (agg) để tính toán nhiều metrics cùng lúc.
  - Tránh dùng collect() trên dataframe lớn.
  - Sử dụng Spark SQL functions để xử lý nhanh.

Các nhóm kiểm tra:
  1. Completeness  : NULL rate các cột quan trọng.
  2. Validity      : review_score ∈ [1, 10], ngày tháng hợp lệ.
  3. Uniqueness    : Kiểm tra trùng lặp (row_checksum).
  4. Freshness     : ingestion_timestamp không quá cũ.

Exit code: 0 = PASS, 1 = FAIL
"""

import sys
import os
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from dataqualify.dq_utils import run_eda, get_check_description
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

TARGET_TABLE = "lakehouse.silver.hotels_reviews"

REQUIRED_COLUMNS = [
    "hotel_url", "reviewer_name", "review_date", "review_score",
    "row_checksum", "ingestion_timestamp", "source_file"
]

THRESHOLDS = {
    "min_records":        100000, # Bảng này lớn, nên có ít nhất 100k records
    "max_null_pct":       1.0,
    "max_duplicate_pct":  1.0,
    "rating_min":         1.0,
    "rating_max":        10.0,
    "max_freshness_hours": 168,
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
    icon = "✅" if status == "PASS" else ("❌" if status == "FAIL" else "⚠️ ")
    crit = "[CRITICAL]" if is_critical else "[optional]"
    if metric_value is not None and threshold_value is not None:
        context = f"  →  {metric_value:.2f} vs threshold {threshold_value}"
    elif metric_value is not None:
        context = f"  →  value={metric_value:.2f}"
    else:
        context = ""
    desc = get_check_description(check_name)
    desc_str = f" ({desc})" if desc else ""
    print(f"  {icon} {crit} [{check_category}] {check_name}{desc_str}: {status}{context}")

# ============================================================================
# DQ LOGIC
# ============================================================================

def run_dq_checks(spark, conn):
    print(f"\n🚀 Đang chạy DQ checks cho {TARGET_TABLE}...")
    
    df = spark.table(TARGET_TABLE)
    run_eda(df, TARGET_TABLE)
    failures = []
    
    # 1. Tối ưu: Tính toán metrics cơ bản trong 1 pass
    #    Dùng agg để tránh quét bảng nhiều lần
    agg_funcs = [
        F.count("*").alias("total_count"),
        F.min("review_score").alias("min_score"),
        F.max("review_score").alias("max_score"),
        F.avg("review_score").alias("avg_score"),
        F.max("ingestion_timestamp").alias("max_ingestion_ts"),
        F.min("review_date").alias("min_review_date"),
        F.max("review_date").alias("max_review_date")
    ]
    
    # Thêm null counts cho các cột bắt buộc
    for col in REQUIRED_COLUMNS:
        agg_funcs.append(F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias(f"null_count_{col}"))
    
    metrics = df.agg(*agg_funcs).collect()[0].asDict()
    total = metrics["total_count"]
    print(f"  Tổng số bản ghi: {total:,}")
    
    if total == 0:
        write_dq_result(conn, "table_empty", "Completeness", "FAIL", True, 0, THRESHOLDS["min_records"])
        return ["table_empty"]

    # --- Check 1: Min Records ---
    print("\n── CHECK 1: Min Records (Số lượng dòng tối thiểu) ──────")
    status = "PASS" if total >= THRESHOLDS["min_records"] else "FAIL"
    write_dq_result(conn, "min_records", "Completeness", status, True, total, THRESHOLDS["min_records"])
    if status == "FAIL": failures.append("min_records")

    # --- Check 2: Null Checks ---
    print("\n── CHECK 2: Null Checks (Kiểm tra giá trị rỗng) ────────")
    for col in REQUIRED_COLUMNS:
        null_count = metrics[f"null_count_{col}"]
        null_pct = (null_count / total) * 100
        status = "PASS" if null_pct <= THRESHOLDS["max_null_pct"] else "FAIL"
        write_dq_result(conn, f"null_{col}", "Completeness", status, True, null_pct, THRESHOLDS["max_null_pct"], {"count": null_count})
        if status == "FAIL": failures.append(f"null_{col}")

    # --- Check 3: Score Range ---
    print("\n── CHECK 3: Score Range (Khoảng điểm đánh giá hợp lệ) ──")
    status = "PASS" if (metrics["min_score"] >= THRESHOLDS["rating_min"] and metrics["max_score"] <= THRESHOLDS["rating_max"]) else "FAIL"
    write_dq_result(conn, "score_range", "Validity", status, True, metrics["avg_score"], None, 
                    {"min": metrics["min_score"], "max": metrics["max_score"]})
    if status == "FAIL": failures.append("score_range")

    # --- Check 4: Date Validity (Review Date) ---
    print("\n── CHECK 4: Date Validity (Tính hợp lệ của ngày tháng) ─")
    current_date = datetime.now().date()
    # review_date không được trong tương lai (cho phép sai số 1 ngày do timezone)
    status = "PASS" if metrics["max_review_date"] <= current_date else "WARN"
    write_dq_result(conn, "review_date_validity", "Validity", status, False, details={"max_date": str(metrics["max_review_date"])})

    # --- Check 5: Freshness ---
    print("\n── CHECK 5: Freshness (Độ tươi mới của dữ liệu) ────────")
    if metrics["max_ingestion_ts"]:
        hours_old = (datetime.now() - metrics["max_ingestion_ts"]).total_seconds() / 3600
        status = "PASS" if hours_old <= THRESHOLDS["max_freshness_hours"] else "WARN"
        write_dq_result(conn, "freshness", "Freshness", status, False, hours_old, THRESHOLDS["max_freshness_hours"])
    
    # --- Check 6: Uniqueness (row_checksum) ---
    print("\n── CHECK 6: Uniqueness (Tính duy nhất của checksum) ────")
    # Vì bảng lớn, check distinct count có thể tốn tài nguyên.
    # Ta sẽ check mẫu hoặc dùng approx_count_distinct nếu cần cực nhanh.
    # Ở đây dùng count + distinct thông thường vì 1.5M vẫn ổn với cluster hiện tại.
    distinct_checksums = df.select("row_checksum").distinct().count()
    dup_count = total - distinct_checksums
    dup_pct = (dup_count / total) * 100
    status = "PASS" if dup_pct <= THRESHOLDS["max_duplicate_pct"] else "FAIL"
    write_dq_result(conn, "duplicate_checksum", "Uniqueness", status, True, dup_pct, THRESHOLDS["max_duplicate_pct"], {"duplicates": dup_count})
    if status == "FAIL": failures.append("duplicate_checksum")

    return failures

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 65)
    print("  DATA QUALITY — Hotels Reviews")
    print("=" * 65)
    print(f"  Table     : {TARGET_TABLE}")
    print(f"  Run time  : {RUN_TIMESTAMP}")
    print("=" * 65)

    spark = get_spark_session(app_name="DQ_Hotels_Reviews")

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
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'conn' in locals(): conn.close()
        spark.stop()

if __name__ == "__main__":
    main()
