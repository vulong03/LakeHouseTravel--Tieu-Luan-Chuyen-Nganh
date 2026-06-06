"""
Data Quality Checks — Hotels List (Silver Layer)
=================================================
Table: silver.silver.hotels_list

Schema:
  hotel_name            CHARACTER VARYING
  hotel_url             CHARACTER VARYING
  province              CHARACTER VARYING
  row_checksum          CHARACTER VARYING
  ingestion_timestamp   TIMESTAMP
  source_file           CHARACTER VARYING
  source_file_checksum  CHARACTER VARYING

Checks:
  1. No-Null          : Tất cả 7 cột không được NULL
  2. hotel_name       : Không blank, không chứa ký tự đặc biệt lạ
  3. hotel_url        : Đúng format http(s)://booking.com/...
  4. province         : Phải thuộc danh sách 63 tỉnh/thành Việt Nam

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

TARGET_TABLE = "lakehouse.silver.hotels_list"

# Tất cả 7 cột — không cột nào được NULL
ALL_COLUMNS = [
    "hotel_name",
    "hotel_url",
    "province",
    "row_checksum",
    "ingestion_timestamp",
    "source_file",
    "source_file_checksum",
]

# 63 tỉnh/thành Việt Nam (cột Province/city từ CSV)
VALID_PROVINCES = {
    "Bắc Giang", "Bắc Kạn", "Cao Bằng", "Hà Giang", "Lạng Sơn",
    "Phú Thọ", "Quảng Ninh", "Thái Nguyên", "Tuyên Quang",
    "Lào Cai", "Yên Bái", "Điện Biên", "Hòa Bình", "Lai Châu", "Sơn La",
    "Bắc Ninh", "Hà Nam", "Hải Dương", "Hưng Yên", "Nam Định",
    "Ninh Bình", "Thái Bình", "Vĩnh Phúc", "Hà Nội", "Hải Phòng",
    "Hà Tĩnh", "Nghệ An", "Quảng Bình", "Quảng Trị", "Thanh Hóa",
    "Thừa Thiên Huế",
    "Đắk Lắk", "Đắk Nông", "Gia Lai", "Kon Tum", "Lâm Đồng",
    "Bình Định", "Bình Thuận", "Khánh Hòa", "Ninh Thuận",
    "Phú Yên", "Quảng Nam", "Quảng Ngãi", "Đà Nẵng",
    "Bà Rịa Vũng Tàu", "Bình Dương", "Bình Phước",
    "Đồng Nai", "Tây Ninh", "Hồ Chí Minh",
    "An Giang", "Bạc Liêu", "Bến Tre", "Cà Mau", "Đồng Tháp",
    "Hậu Giang", "Kiên Giang", "Long An", "Sóc Trăng",
    "Tiền Giang", "Trà Vinh", "Vĩnh Long", "Cần Thơ",
}

THRESHOLDS = {
    "min_records": 100,
}

RUN_TIMESTAMP = datetime.now().isoformat()

# ============================================================================
# POSTGRESQL — GHI KẾT QUẢ
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
# CHECK 1 — NO NULL (tất cả 7 cột)
# ============================================================================

def check_no_null(df, conn, total):
    """Tất cả 7 cột không được có NULL"""
    print("\n── CHECK 1: No-Null (7 cột) ─────────────────────────────")
    failures = []

    for col in ALL_COLUMNS:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct   = (null_count / total * 100) if total > 0 else 0
        status     = "PASS" if null_count == 0 else "FAIL"
        if status == "FAIL":
            failures.append(f"null_{col}")
        write_dq_result(conn, f"null_{col}", "No-Null",
                        status, True, null_pct, 0.0,
                        {"null_count": null_count, "total": total})

    return failures


# ============================================================================
# CHECK 2 — HOTEL NAME VALIDITY
# ============================================================================

def check_hotel_name(df, conn, total):
    """
    hotel_name không được:
      - Blank (empty string hoặc chỉ toàn khoảng trắng)
      - Chứa toàn ký tự không phải chữ cái/số (ký tự lạ như ???, ###, ...)
      - Quá ngắn (< 3 ký tự sau trim)
    """
    print("\n── CHECK 2: Hotel Name Validity ─────────────────────────")
    failures = []

    df_name = df.filter(F.col("hotel_name").isNotNull())

    # 2a. Blank / chỉ khoảng trắng
    blank_count = df_name.filter(
        F.trim(F.col("hotel_name")) == ""
    ).count()
    status = "PASS" if blank_count == 0 else "FAIL"
    if status == "FAIL":
        failures.append("hotel_name_blank")
    write_dq_result(conn, "hotel_name_blank", "Validity",
                    status, True, float(blank_count), 0.0,
                    {"blank_count": blank_count})

    # 2b. Quá ngắn (< 3 ký tự sau trim)
    too_short = df_name.filter(
        F.length(F.trim(F.col("hotel_name"))) < 3
    ).count()
    status = "PASS" if too_short == 0 else "FAIL"
    if status == "FAIL":
        failures.append("hotel_name_too_short")
    write_dq_result(conn, "hotel_name_too_short", "Validity",
                    status, True, float(too_short), 0.0,
                    {"too_short_count": too_short, "min_length": 3})

    # 2c. Chứa toàn ký tự không phải chữ/số (ký tự lạ như ???, ###, ...)
    #     Chấp nhận: Latin (a-z), tiếng Việt Unicode (à-ỹ), số, khoảng trắng, dấu câu thông thường
    no_letter = df_name.filter(
        ~F.col("hotel_name").rlike(r"[a-zA-ZÀ-ỹ]")
    ).count()
    status = "PASS" if no_letter == 0 else "WARN"
    write_dq_result(conn, "hotel_name_no_letter", "Validity",
                    status, False, float(no_letter), 0.0,
                    {"count": no_letter,
                     "note": "Tên không chứa chữ cái Latin hoặc tiếng Việt"})

    # 2d. Sample các tên bất thường để debug
    sample_blank = df_name.filter(
        F.trim(F.col("hotel_name")) == ""
    ).select("hotel_name", "hotel_url").limit(5).collect()

    if sample_blank:
        write_dq_result(conn, "hotel_name_blank_samples", "Validity",
                        "INFO", False,
                        details={"samples": [r["hotel_url"] for r in sample_blank]})

    return failures


# ============================================================================
# CHECK 3 — HOTEL URL FORMAT
# ============================================================================

def check_hotel_url(df, conn, total):
    """
    hotel_url phải:
      - Bắt đầu bằng https:// hoặc http://
      - Chứa domain booking.com
      - Không có khoảng trắng
    """
    print("\n── CHECK 3: Hotel URL Format ────────────────────────────")
    failures = []

    df_url = df.filter(F.col("hotel_url").isNotNull())
    url_count = df_url.count()

    # 3a. Phải bắt đầu http(s)://
    invalid_scheme = df_url.filter(
        ~(F.col("hotel_url").startswith("https://") |
          F.col("hotel_url").startswith("http://"))
    ).count()
    invalid_pct = (invalid_scheme / url_count * 100) if url_count > 0 else 0
    status = "PASS" if invalid_scheme == 0 else "FAIL"
    if status == "FAIL":
        failures.append("url_invalid_scheme")
    write_dq_result(conn, "url_invalid_scheme", "Validity",
                    status, True, invalid_pct, 0.0,
                    {"invalid_count": invalid_scheme})

    # 3b. Phải chứa booking.com
    no_domain = df_url.filter(
        ~F.col("hotel_url").contains("booking.com")
    ).count()
    no_domain_pct = (no_domain / url_count * 100) if url_count > 0 else 0
    status = "PASS" if no_domain_pct <= 1.0 else "FAIL"
    if status == "FAIL":
        failures.append("url_missing_domain")
    write_dq_result(conn, "url_missing_domain", "Validity",
                    status, True, no_domain_pct, 1.0,
                    {"invalid_count": no_domain,
                     "note": "URL không chứa booking.com"})

    # 3c. Không có khoảng trắng trong URL
    has_space = df_url.filter(
        F.col("hotel_url").contains(" ")
    ).count()
    status = "PASS" if has_space == 0 else "FAIL"
    if status == "FAIL":
        failures.append("url_has_whitespace")
    write_dq_result(conn, "url_has_whitespace", "Validity",
                    status, True, float(has_space), 0.0,
                    {"count": has_space})

    # 3d. Uniqueness — mỗi URL chỉ xuất hiện 1 lần
    dup_count = total - df.select("hotel_url").distinct().count()
    dup_pct   = (dup_count / total * 100) if total > 0 else 0
    status    = "PASS" if dup_pct <= 1.0 else "FAIL"
    if status == "FAIL":
        failures.append("url_duplicate")
    write_dq_result(conn, "url_duplicate", "Uniqueness",
                    status, True, dup_pct, 1.0,
                    {"duplicate_count": dup_count, "total": total})

    return failures


# ============================================================================
# CHECK 4 — PROVINCE VALIDATION
# ============================================================================

def check_province(df, conn, total):
    """
    province phải thuộc danh sách 63 tỉnh/thành chính thức Việt Nam.
    Ghi log các giá trị không hợp lệ để dễ debug.
    """
    print("\n── CHECK 4: Province Validation (63 tỉnh/thành) ────────")
    failures = []

    df_prov = df.filter(F.col("province").isNotNull())

    # Build Spark filter: province NOT IN valid set
    valid_list = list(VALID_PROVINCES)
    invalid_df = df_prov.filter(~F.col("province").isin(valid_list))
    invalid_count = invalid_df.count()
    invalid_pct   = (invalid_count / total * 100) if total > 0 else 0

    status = "PASS" if invalid_count == 0 else "FAIL"
    if status == "FAIL":
        failures.append("province_invalid")

    # Lấy danh sách giá trị lạ để log
    invalid_values = [
        r["province"]
        for r in invalid_df.select("province").distinct().limit(20).collect()
    ]

    write_dq_result(conn, "province_invalid_value", "Validity",
                    status, True, invalid_pct, 0.0,
                    {
                        "invalid_count": invalid_count,
                        "total": total,
                        "sample_invalid_values": invalid_values,
                        "valid_province_count": len(VALID_PROVINCES),
                    })

    # Coverage: bao nhiêu tỉnh có trong data
    found_provinces = [
        r["province"]
        for r in df_prov.select("province").distinct().collect()
    ]
    coverage = len(set(found_provinces) & VALID_PROVINCES)
    status_cov = "PASS" if coverage >= 20 else "WARN"
    write_dq_result(conn, "province_coverage", "Distribution",
                    status_cov, False, float(coverage), 20.0,
                    {
                        "provinces_found": coverage,
                        "provinces_total": len(VALID_PROVINCES),
                        "missing": sorted(VALID_PROVINCES - set(found_provinces)),
                    })

    return failures


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 70)
    print("HOTELS LIST — Data Quality Check")
    print("=" * 70)
    print(f"Run       : {RUN_TIMESTAMP}")
    print(f"Table     : {TARGET_TABLE}")
    print(f"Provinces : {len(VALID_PROVINCES)} tỉnh/thành hợp lệ")

    spark = get_spark_session(app_name="DQ_Hotels_List")

    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        ensure_dq_table(conn)

        # ── Đọc bảng ────────────────────────────────────────────
        try:
            df    = spark.table(TARGET_TABLE)
            run_eda(df, TARGET_TABLE)
            total = df.count()
            print(f"\nTotal records: {total:,}")
        except Exception as e:
            print(f"❌ Không thể đọc bảng {TARGET_TABLE}: {e}")
            write_dq_result(conn, "table_accessible", "Availability",
                            "FAIL", True, details={"error": str(e)})
            conn.close()
            sys.exit(1)

        # Min records check
        threshold = THRESHOLDS["min_records"]
        status = "PASS" if total >= threshold else "FAIL"
        write_dq_result(conn, "min_records", "Completeness",
                        status, True, float(total), float(threshold),
                        {"total_records": total})

        # ── Chạy các checks ─────────────────────────────────────
        all_failures = []
        all_failures += check_no_null(df, conn, total)
        all_failures += check_hotel_name(df, conn, total)
        all_failures += check_hotel_url(df, conn, total)
        all_failures += check_province(df, conn, total)

        if total < threshold:
            all_failures.append("min_records")

        # ── Summary ─────────────────────────────────────────────
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
