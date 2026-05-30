"""
Shared Data Quality Utilities
==============================
Dùng chung cho tất cả DQ check scripts (hotel, tiktok...).
Tách riêng để tránh lặp code và dễ bảo trì.
"""

import json


def ensure_dq_table(conn):
    """Tạo bảng dq_results nếu chưa tồn tại."""
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


def write_dq_result(conn, run_timestamp, table_name, check_name, check_category,
                    status, is_critical,
                    metric_value=None, threshold_value=None, details=None):
    """
    Ghi 1 kết quả DQ check vào PostgreSQL.

    Args:
        conn            : psycopg2 connection (autocommit=False)
        run_timestamp   : ISO timestamp của lần chạy (dùng chung 1 giá trị cho toàn bộ run)
        table_name      : Tên bảng đang được check (e.g. "silver.silver.tiktok_videos")
        check_name      : Tên check cụ thể (e.g. "null_post_url")
        check_category  : Nhóm check: Completeness / Validity / Uniqueness / Integrity / Distribution
        status          : "PASS" / "WARN" / "FAIL"
        is_critical     : True → failure sẽ exit(1); False → chỉ cảnh báo
        metric_value    : Giá trị đo được (tỉ lệ NULL, số duplicate, ...)
        threshold_value : Ngưỡng so sánh
        details         : Dict với thông tin bổ sung (sẽ lưu dạng JSONB)
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dq_results
                (run_timestamp, table_name, check_name, check_category,
                 status, is_critical, metric_value, threshold_value, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        """, (
            run_timestamp, table_name, check_name, check_category,
            status, is_critical,
            float(metric_value) if metric_value is not None else None,
            float(threshold_value) if threshold_value is not None else None,
            json.dumps(details, ensure_ascii=False) if details else None
        ))
    # Không commit ở đây — để caller tự commit sau khi insert hết 1 batch
    icon = "✅" if status == "PASS" else ("❌" if status == "FAIL" else "⚠️ ")
    crit = "[CRITICAL]" if is_critical else "[optional]"

    # Hiển thị context: value vs threshold nếu có
    if metric_value is not None and threshold_value is not None:
        context = f"  →  {metric_value:.2f} vs threshold {threshold_value}"
    elif metric_value is not None:
        context = f"  →  value={metric_value:.2f}"
    else:
        context = ""

    print(f"  {icon} {crit} [{check_category}] {check_name}: {status}{context}")

