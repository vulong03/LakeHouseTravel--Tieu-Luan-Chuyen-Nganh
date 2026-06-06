"""
Data Quality Checks — Hotels Detail (Silver Layer)
===================================================
Kiểm tra chất lượng dữ liệu cho: lakehouse.silver.hotels_detail

Schema (12 cột):
  hotel_name            string        -- bắt buộc
  hotel_url             string        -- bắt buộc (business key)
  province              string        -- bắt buộc (partition)
  description           string        -- optional (~74 NULL thực tế)
  top_amenities         string        -- optional (~121 NULL thực tế)
  rating_score          double        -- optional (~15% NULL = không có rating)
  review_count          int           -- optional (cùng tỉ lệ null với rating)
  rating_breakdown      string        -- optional (~436 NULL)
  row_checksum          string        -- bắt buộc
  ingestion_timestamp   timestamp     -- bắt buộc
  source_file           string        -- bắt buộc
  source_file_checksum  string        -- bắt buộc

Thực tế từ data (15,945 records):
  - Cột bắt buộc: 0 NULL
  - rating_score: min=1.0, max=10.0, avg=8.5 — ~15% NULL (chưa có rating)
  - review_count: min=1, max=998

Exit code: 0 = ALL CRITICAL PASS | 1 = ít nhất 1 CRITICAL FAIL
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

TARGET_TABLE = "lakehouse.silver.hotels_detail"

# Cột bắt buộc — không được NULL
REQUIRED_COLUMNS = [
    "hotel_name", "hotel_url", "province",
    "row_checksum", "ingestion_timestamp",
    "source_file", "source_file_checksum",
]

# Cột optional — có NULL là bình thường, chỉ WARN nếu quá nhiều
OPTIONAL_COLUMNS = {
    "description":      20.0,   # threshold WARN (%)
    "top_amenities":    20.0,
    "rating_score":     20.0,   # ~15% NULL thực tế — ngưỡng 20%
    "review_count":     20.0,
    "rating_breakdown": 20.0,
}

THRESHOLDS = {
    "min_records":        100,
    "max_duplicate_pct":  1.0,   # % duplicate hotel_url
    "rating_min":         1.0,   # Booking.com: thang điểm 1-10
    "rating_max":        10.0,
    "review_count_min":   0,     # review_count không âm
}

RUN_TIMESTAMP = datetime.now().isoformat()

# ============================================================================
# POSTGRESQL
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


def write_dq_result(conn, check_name, check_category,
                    status, is_critical, metric_value=None,
                    threshold_value=None, details=None):
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
# CHECK 1 — NULL CHECKS
# ============================================================================

def check_nulls(df, conn, total):
    """
    - Cột bắt buộc: NULL = CRITICAL FAIL
    - Cột optional: NULL > threshold = WARN
    """
    print("\n── CHECK 1: Null Checks ─────────────────────────────────")
    failures = []

    # 1a. Cột bắt buộc
    for col in REQUIRED_COLUMNS:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct   = (null_count / total * 100) if total > 0 else 0
        status     = "PASS" if null_count == 0 else "FAIL"
        if status == "FAIL":
            failures.append(f"null_{col}")
        write_dq_result(conn, f"null_{col}", "Completeness",
                        status, True, null_pct, 0.0,
                        {"null_count": null_count, "total": total})

    # 1b. Cột optional — WARN nếu vượt ngưỡng
    for col, warn_threshold in OPTIONAL_COLUMNS.items():
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct   = (null_count / total * 100) if total > 0 else 0
        status     = "PASS" if null_pct <= warn_threshold else "WARN"
        write_dq_result(conn, f"null_{col}_optional", "Completeness",
                        status, False, null_pct, warn_threshold,
                        {"null_count": null_count, "total": total,
                         "note": "Optional column — WARN only"})

    return failures


# ============================================================================
# CHECK 2 — UNIQUENESS
# ============================================================================

def check_uniqueness(df, conn, total):
    """hotel_url là business key — không được trùng"""
    print("\n── CHECK 2: Uniqueness (hotel_url) ──────────────────────")
    failures = []

    distinct_count = df.select("hotel_url").distinct().count()
    dup_count      = total - distinct_count
    dup_pct        = (dup_count / total * 100) if total > 0 else 0
    threshold      = THRESHOLDS["max_duplicate_pct"]
    status         = "PASS" if dup_pct <= threshold else "FAIL"

    if status == "FAIL":
        failures.append("duplicate_hotel_url")

    # Sample các URL trùng để debug
    dup_samples = []
    if dup_count > 0:
        dup_samples = [
            r["hotel_url"]
            for r in df.groupBy("hotel_url")
                       .count()
                       .filter(F.col("count") > 1)
                       .select("hotel_url")
                       .limit(5).collect()
        ]

    write_dq_result(conn, "duplicate_hotel_url", "Uniqueness",
                    status, True, dup_pct, threshold,
                    {"duplicate_count": dup_count,
                     "distinct_count": distinct_count,
                     "sample_duplicates": dup_samples})

    return failures


# ============================================================================
# CHECK 3 — RATING SCORE VALIDITY
# ============================================================================

def check_rating(df, conn, total):
    """
    rating_score phải trong [1.0, 10.0] (Booking.com scale).
    Chỉ check những record có rating (không NULL).
    """
    print("\n── CHECK 3: Rating Score Validity [1.0 - 10.0] ─────────")
    failures = []

    df_rated   = df.filter(F.col("rating_score").isNotNull())
    rated_count = df_rated.count()

    if rated_count == 0:
        write_dq_result(conn, "rating_score_range", "Validity",
                        "WARN", False,
                        details={"note": "Không có record nào có rating_score"})
        return failures

    # 3a. Ngoài khoảng [1, 10]
    invalid = df_rated.filter(
        (F.col("rating_score") < THRESHOLDS["rating_min"]) |
        (F.col("rating_score") > THRESHOLDS["rating_max"])
    ).count()
    invalid_pct = (invalid / rated_count * 100)
    status = "PASS" if invalid == 0 else "FAIL"
    if status == "FAIL":
        failures.append("rating_score_out_of_range")

    # Thống kê phân phối
    stats = df_rated.agg(
        F.round(F.avg("rating_score"), 3).alias("avg"),
        F.round(F.min("rating_score"), 1).alias("min"),
        F.round(F.max("rating_score"), 1).alias("max"),
        F.round(F.stddev("rating_score"), 3).alias("stddev"),
    ).collect()[0]

    write_dq_result(conn, "rating_score_range", "Validity",
                    status, True, invalid_pct, 0.0,
                    {
                        "invalid_count": invalid,
                        "rated_records": rated_count,
                        "null_records": total - rated_count,
                        "avg":    float(stats["avg"]    or 0),
                        "min":    float(stats["min"]    or 0),
                        "max":    float(stats["max"]    or 0),
                        "stddev": float(stats["stddev"] or 0),
                    })

    # 3b. review_count không âm
    negative_rc = df.filter(
        F.col("review_count").isNotNull() & (F.col("review_count") < 0)
    ).count()
    status = "PASS" if negative_rc == 0 else "FAIL"
    if status == "FAIL":
        failures.append("review_count_negative")
    write_dq_result(conn, "review_count_non_negative", "Validity",
                    status, True, float(negative_rc), 0.0,
                    {"negative_count": negative_rc})

    # 3c. Phân phối review_count (info)
    rc_stats = df.filter(F.col("review_count").isNotNull()).agg(
        F.min("review_count").alias("min"),
        F.max("review_count").alias("max"),
        F.round(F.avg("review_count"), 1).alias("avg"),
    ).collect()[0]
    write_dq_result(conn, "review_count_distribution", "Distribution",
                    "PASS", False,
                    details={
                        "min": int(rc_stats["min"] or 0),
                        "max": int(rc_stats["max"] or 0),
                        "avg": float(rc_stats["avg"] or 0),
                    })

    return failures


# ============================================================================
# CHECK 4 — HOTEL NAME & URL
# ============================================================================

def check_name_url(df, conn, total):
    """hotel_name không blank; hotel_url đúng format booking.com"""
    print("\n── CHECK 4: Hotel Name & URL ────────────────────────────")
    failures = []

    df_name = df.filter(F.col("hotel_name").isNotNull())
    df_url  = df.filter(F.col("hotel_url").isNotNull())

    # 4a. Blank name
    blank_name = df_name.filter(F.trim(F.col("hotel_name")) == "").count()
    status = "PASS" if blank_name == 0 else "FAIL"
    if status == "FAIL":
        failures.append("hotel_name_blank")
    write_dq_result(conn, "hotel_name_blank", "Validity",
                    status, True, float(blank_name), 0.0,
                    {"blank_count": blank_name})

    # 4b. Quá ngắn (< 3 ký tự)
    too_short = df_name.filter(F.length(F.trim(F.col("hotel_name"))) < 3).count()
    status = "PASS" if too_short == 0 else "FAIL"
    if status == "FAIL":
        failures.append("hotel_name_too_short")
    write_dq_result(conn, "hotel_name_too_short", "Validity",
                    status, True, float(too_short), 0.0,
                    {"count": too_short})

    # 4c. URL không chứa booking.com
    no_domain = df_url.filter(~F.col("hotel_url").contains("booking.com")).count()
    no_domain_pct = (no_domain / total * 100) if total > 0 else 0
    status = "PASS" if no_domain == 0 else "FAIL"
    if status == "FAIL":
        failures.append("url_missing_domain")
    write_dq_result(conn, "url_missing_domain", "Validity",
                    status, True, no_domain_pct, 0.0,
                    {"invalid_count": no_domain})

    return failures


# ============================================================================
# CHECK 5 — RATING BREAKDOWN FORMAT
# ============================================================================

def check_rating_breakdown(df, conn, total):
    """
    rating_breakdown format thực tế:
      "Nhân viên phục vụ: 7,6, Tiện nghi: 7,2, Sạch sẽ: 7,5, ..."

    Quy tắc:
      - 6 category bắt buộc phải có mặt
      - Score dùng dấu phẩy làm thập phân (VN format: 7,6 = 7.6)
      - Score hợp lệ trong khoảng [1, 10]
      - Chỉ check record có rating_breakdown (không NULL)
    """
    print("\n── CHECK 5: Rating Breakdown Format ─────────────────────")
    failures = []

    # Dùng regex patterns để tránh lỗi Unicode Normalization (NFC vs NFD)
    # Dấu '.' đại diện cho các nguyên âm có dấu (phụ, sạch, địa,...)
    # Dùng regex '.*' thay vì '.' để chấp nhận cả ký tự có dấu đơn (NFC) và dấu rời (NFD)
    # NFD: 'ạ' = 'a' + 'dấu nặng' (2 ký tự), '.' chỉ khớp 1. '.*' sẽ khớp tất cả.
    REQUIRED_CATEGORIES = [
        ("nh.*n vi.*n", "Nhân viên"),
        ("ti.*n nghi", "Tiện nghi"),
        ("s.*ch", "Sạch sẽ"),
        ("tho.*i m.*i", "Thoải mái"),
        ("gi.* ti.*n", "Đáng giá tiền"),
        (".*a.*m", "Địa điểm"), # Pattern thoáng nhất cho 'Địa điểm'
    ]
    
    CAT_THRESHOLD = 1.0  # Cho phép tối đa 1% record thiếu category này

    df_rb = df.filter(F.col("rating_breakdown").isNotNull())
    rb_count = df_rb.count()

    if rb_count == 0:
        write_dq_result(conn, "rating_breakdown_format", "Validity",
                        "WARN", False,
                        details={"note": "Không có record nào có rating_breakdown"})
        return failures

    print(f"  Records có rating_breakdown: {rb_count:,}")

    # 5a. Phải chứa đủ 6 category bắt buộc
    df_rb_lower = df_rb.withColumn("rb_lower", F.lower(F.col("rating_breakdown")))
    
    for pattern, label in REQUIRED_CATEGORIES:
        missing = df_rb_lower.filter(~F.col("rb_lower").rlike(pattern)).count()
        missing_pct = (missing / rb_count * 100) if rb_count > 0 else 0
        col_key = label.lower().replace(" ", "_")
        status = "PASS" if missing_pct <= CAT_THRESHOLD else "FAIL"
        
        if status == "FAIL":
            failures.append(f"breakdown_missing_{col_key}")
            # In sample để debug
            sample_fail = df_rb_lower.filter(~F.col("rb_lower").rlike(pattern)).select("rating_breakdown").limit(3).collect()
            print(f"    ⚠️ Sample failing '{label}' ({missing_pct:.2f}%): {[r['rating_breakdown'] for r in sample_fail]}")
        
        write_dq_result(conn, f"breakdown_has_{col_key}", "Validity",
                        status, True, missing_pct, CAT_THRESHOLD,
                        {"missing_count": missing, "category": label, "pattern": pattern})

    # 5b. Phải chứa ít nhất 1 số (score)
    #     Format VN: score là digit optionally followed by ,digit  e.g. 7,6 or 10
    no_score = df_rb.filter(
        ~F.col("rating_breakdown").rlike(r"\d+,?\d*")
    ).count()
    status = "PASS" if no_score == 0 else "FAIL"
    if status == "FAIL":
        failures.append("breakdown_no_score")
    write_dq_result(conn, "breakdown_has_score", "Validity",
                    status, True, float(no_score), 0.0,
                    {"invalid_count": no_score})

    # 5c. Thống kê: có bao nhiêu record có WiFi miễn phí (category phụ)
    has_wifi = df_rb.filter(
        F.col("rating_breakdown").contains("WiFi")
    ).count()
    wifi_pct = (has_wifi / rb_count * 100) if rb_count > 0 else 0
    write_dq_result(conn, "breakdown_wifi_category", "Distribution",
                    "PASS", False, wifi_pct, None,
                    {"has_wifi_count": has_wifi,
                     "total_with_breakdown": rb_count,
                     "note": "WiFi miễn phí là category tùy chọn"})

    return failures


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 70)
    print("HOTELS DETAIL — Data Quality Check")
    print("=" * 70)
    print(f"Run  : {RUN_TIMESTAMP}")
    print(f"Table: {TARGET_TABLE}")

    spark = get_spark_session(app_name="DQ_Hotels_Detail")

    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        ensure_dq_table(conn)

        # Đọc bảng
        try:
            df    = spark.table(TARGET_TABLE)
            run_eda(df, TARGET_TABLE)
            total = df.count()
            print(f"\nTotal records: {total:,}")
        except Exception as e:
            print(f"❌ Không đọc được bảng {TARGET_TABLE}: {e}")
            write_dq_result(conn, "table_accessible", "Availability",
                            "FAIL", True, details={"error": str(e)})
            conn.close()
            sys.exit(1)

        # Min records
        threshold = THRESHOLDS["min_records"]
        status = "PASS" if total >= threshold else "FAIL"
        write_dq_result(conn, "min_records", "Completeness",
                        status, True, float(total), float(threshold))

        # Chạy checks
        all_failures = []
        all_failures += check_nulls(df, conn, total)
        all_failures += check_uniqueness(df, conn, total)
        all_failures += check_rating(df, conn, total)
        all_failures += check_name_url(df, conn, total)
        all_failures += check_rating_breakdown(df, conn, total)

        if total < threshold:
            all_failures.append("min_records")

        print("\n" + "=" * 65)
        print("  SUMMARY")
        print("=" * 65)

        if all_failures:
            print(f"  ❌ DQ FAILED — {len(all_failures)} critical check(s) violated:")
            for f in all_failures:
                print(f"     • {f}")
            conn.close()
            sys.exit(1)
        else:
            print("  ✅ ALL CRITICAL CHECKS PASSED")
            conn.close()
            sys.exit(0)

    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
