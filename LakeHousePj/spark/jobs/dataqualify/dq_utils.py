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

    desc = get_check_description(check_name)
    desc_str = f" ({desc})" if desc else ""
    print(f"  {icon} {crit} [{check_category}] {check_name}{desc_str}: {status}{context}")


def run_eda(df, table_name):
    """
    Thực hiện phân tích khám phá dữ liệu nhanh (EDA) và in ra log.
    Hiển thị: số dòng, số cột, kiểu dữ liệu, số lượng null và tỷ lệ null từng cột.
    Sử dụng 1 pass aggregation để tối ưu hóa hiệu năng trên Spark.
    """
    from pyspark.sql import functions as F
    
    print("\n" + "=" * 85)
    print(f"📊 EXPLORATORY DATA ANALYSIS (EDA) — {table_name.upper()}")
    print("=" * 85)
    
    total_rows = df.count()
    cols = df.columns
    total_cols = len(cols)
    
    print(f"  • Total Records : {total_rows:,}")
    print(f"  • Total Columns : {total_cols}")
    print("-" * 85)
    
    # Tính toán số lượng Null của tất cả các cột trong 1 pass aggregation duy nhất
    null_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in cols]
    
    try:
        null_row = df.agg(*null_exprs).collect()[0]
        null_counts = null_row.asDict()
    except Exception as e:
        print(f"  ⚠️ Failed to calculate null counts: {e}")
        null_counts = {c: 0 for c in cols}
        
    # In tiêu đề bảng
    print(f"  {'Column Name':<32} | {'Data Type':<15} | {'Null Count':<12} | {'Null %':<8}")
    print("  " + "-" * 81)
    
    dtypes_dict = dict(df.dtypes)
    
    for c in cols:
        dtype = dtypes_dict.get(c, "unknown")
        n_count = null_counts.get(c, 0)
        n_pct = (n_count / total_rows * 100) if total_rows > 0 else 0.0
        print(f"  {c:<32} | {dtype:<15} | {n_count:<12,} | {n_pct:<7.2f}%")
        
    print("=" * 85 + "\n")


CHECK_DESCRIPTIONS = {
    # General & Metastore checks
    "min_records": "Kiểm tra số lượng bản ghi tối thiểu trong bảng",
    "table_accessible": "Kiểm tra quyền truy cập bảng dữ liệu",
    "table_empty": "Kiểm tra bảng dữ liệu có bị trống rỗng không",
    "freshness": "Kiểm tra độ tươi mới của dữ liệu (thời gian nạp gần nhất)",

    # Hotel list checks
    "hotel_name_blank": "Kiểm tra tên khách sạn không được để trống (blank)",
    "hotel_name_too_short": "Kiểm tra tên khách sạn phải có độ dài từ 3 ký tự trở lên",
    "hotel_name_no_letter": "Kiểm tra tên khách sạn phải chứa ít nhất 1 chữ cái",
    "hotel_name_blank_samples": "Thống kê danh sách mẫu các khách sạn bị trống tên",
    "url_invalid_scheme": "Kiểm tra liên kết URL phải bắt đầu bằng http:// hoặc https://",
    "url_missing_domain": "Kiểm tra liên kết URL phải thuộc tên miền booking.com",
    "url_has_whitespace": "Kiểm tra liên kết URL không được chứa khoảng trắng",
    "url_duplicate": "Kiểm tra trùng lặp liên kết URL khách sạn",
    "province_invalid_value": "Kiểm tra tên tỉnh thành phải thuộc danh sách 63 tỉnh/thành Việt Nam",
    "province_coverage": "Kiểm tra số lượng tỉnh thành xuất hiện trong dữ liệu (ngưỡng tối thiểu)",

    # Hotel detail checks
    "duplicate_hotel_url": "Kiểm tra trùng lặp URL chi tiết khách sạn",
    "rating_score_range": "Kiểm tra điểm đánh giá phải nằm trong khoảng hợp lệ [1.0, 10.0]",
    "review_count_non_negative": "Kiểm tra số lượng đánh giá không được phép âm (< 0)",
    "review_count_distribution": "Thống kê phân phối số lượng đánh giá của các khách sạn",
    "rating_breakdown_format": "Kiểm tra định dạng rating breakdown có dữ liệu",
    "breakdown_has_score": "Kiểm tra định dạng điểm số trong rating breakdown",
    "breakdown_wifi_category": "Thống kê tỷ lệ khách sạn có thông tin WiFi miễn phí",

    # Hotel reviews checks
    "score_range": "Kiểm tra điểm đánh giá của bình luận nằm trong khoảng hợp lệ [1.0, 10.0]",
    "review_date_validity": "Kiểm tra ngày viết đánh giá không được trong tương lai",
    "duplicate_checksum": "Kiểm tra trùng lặp bản ghi thông qua checksum",

    # TikTok general checks
    "url_format": "Kiểm tra định dạng URL phải thuộc tiktok.com",
    "duplicate_url": "Kiểm tra trùng lặp URL video TikTok",

    # TikTok videos checks
    "region_consistency": "Kiểm tra tên vùng miền phải thuộc danh sách 8 vùng địa lý Việt Nam",
    "future_posted_date": "Kiểm tra ngày đăng video không được lớn hơn ngày hiện tại (tương lai)",

    # TikTok post metadata checks
    "null_post_date": "Kiểm tra giá trị trống (NULL) của ngày đăng bài viết",
    "metrics_stats": "Thống kê phân phối số lượng tương tác (likes, shares, comments)",

    # TikTok post comments checks
    "empty_comments": "Kiểm tra nội dung bình luận không được rỗng sau khi trim",
    "level_enum_validity": "Kiểm tra tính hợp lệ của level_comment (phải là Yes hoặc No)",
    "orphan_comments": "Kiểm tra bình luận mồ côi (không có video tương ứng trong gold layer)",
}


def get_check_description(check_name):
    if check_name in CHECK_DESCRIPTIONS:
        return CHECK_DESCRIPTIONS[check_name]
    
    # Dynamic column completeness checks
    if check_name.startswith("null_") and (check_name.endswith("_optional") or check_name.endswith("_opt")):
        col = check_name.replace("null_", "")
        for suffix in ["_optional", "_opt"]:
            if col.endswith(suffix):
                col = col[:-len(suffix)]
        return f"Kiểm tra tỷ lệ trống (NULL) của cột tùy chọn '{col}'"
    elif check_name.startswith("null_"):
        col = check_name.replace("null_", "")
        return f"Kiểm tra giá trị trống (NULL) của cột bắt buộc '{col}'"
        
    # Dynamic values checking
    if check_name.startswith("negative_"):
        col = check_name.replace("negative_", "")
        return f"Kiểm tra giá trị cột số '{col}' không được phép âm"
        
    # Rating breakdown specific dynamically generated check keys
    if check_name.startswith("breakdown_has_"):
        category = check_name.replace("breakdown_has_", "").replace("_", " ").title()
        return f"Kiểm tra sự tồn tại của điểm chi tiết '{category}'"
        
    return ""

