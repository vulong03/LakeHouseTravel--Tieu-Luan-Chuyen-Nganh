# Báo cáo Phân tích Hao hụt Dữ liệu - Tiktok Comments Silver Layer

**Ngày thực hiện:** 16/05/2026
**Phạm vi:** So sánh logic data cleaning giữa bản cũ (`1old.txt`, `2old.txt`) và bản hiện tại (`partition_utils.py`, `step_02_clean_load.py`)

## 1. So sánh `2old.txt` và `step_02_clean_load.py` (Mức độ hao hụt: RẤT LỚN)

File `step_02_clean_load.py` mới đã được thiết kế lại với các quy tắc làm sạch **khắt khe hơn rất nhiều** so với bản cũ. Đây chính là nguyên nhân cốt lõi khiến dung lượng data của bạn bị giảm sút.

### Lớp lọc 1: Loại bỏ Video Viral (> 5 Triệu Shares) - *Bộ lọc MỚI*
- **Bản cũ (`2old.txt`):** Không có giới hạn hoặc kiểm tra logic đối với các chỉ số tương tác (metrics). Mọi bài post đều được giữ nguyên.
- **Bản mới (`step_02_clean_load.py`):** Thêm bộ luật kiểm duyệt trong hàm `clean_and_transform_posts`:
```python
df_filtered = df_cleaned.filter(
    F.col("shares").isNull() | (F.col("shares") <= 5000000)
)
```
- **Hậu quả:** Bộ lọc này tự động xóa sạch các bài viết có hơn 5.000.000 lượt chia sẻ vì cho rằng đó là "dữ liệu lỗi từ tool crawl". Tại TikTok, những video này thường là những video cực kỳ viral và sở hữu hàng chục nghìn comment.

### Lớp lọc 2: Cơ chế "Trảm thảo trừ căn" ở Comments (Orphan Drop) - *Bổ sung MỚI*
- **Bản cũ (`2old.txt`):** Ghi nạp toàn bộ bình luận vào database một cách độc lập so với bài post.
- **Bản mới (`step_02_clean_load.py`):** Tại `Phase 2: BATCH PROCESSING`, hệ thống ép buộc phải `inner join` Comments với bảng Silver Posts hợp lệ:
```python
df_valid_posts = spark.table(SILVER_TABLE_POSTS).select("post_url").distinct()
df_comments_validated = df_comments_cleaned.join(
    df_valid_posts,
    on="post_url",
    how="inner"  # Chỉ lấy comments nếu bài post tồn tại ở Silver
)
```
- **Hậu quả:** Đây là "hiệu ứng Domino". Vi bài post viral bị xóa ở Lớp lọc 1, toàn bộ số lượng comment khổng lồ của bài post đó lập tức trở thành *Comment mồ côi (Orphan Comments)*. Cú `inner join` này thẳng tay xóa toàn bộ số comment mồ côi đó.

### Lớp lọc 3: Siết chặt cấu trúc Comment - *Bổ sung MỚI*
Trong `clean_and_transform_comments`, bản mới thêm các bộ lọc từ chối data hỏng:
- **Lỗi cột lệch (Column Shift):** Nếu dòng dữ liệu crawl bị lệch cột khiến `level_comment` chứa data rác thay vì (Yes, No, Null), dòng bình luận đó bị xóa lập tức.
- **Comment rỗng (Empty Text):** Xóa tất cả các bình luận bị Null hoặc chuỗi rỗng `""`.

---

## 2. So sánh `1old.txt` và `partition_utils.py` (Mức độ hao hụt: KHÔNG ĐỔI, NHƯNG LÀM RÕ LUẬT CHƠI)

Logic code của 2 file này thực ra **rất giống nhau**, tuy nhiên bản hiện tại (`partition_utils.py`) đã bổ sung các `docstrings` (comment giải thích hệ thống) để khẳng định một chiến lược nghiêm ngặt:

### Quy định "KHÔNG Re-crawl" (Cross-run dedup)
- Cả hai bản đều dùng hàm `filter_unprocessed_partitions` kiểm tra xem `post_url` đã tồn tại trong Silver hay chưa, nếu có thì loại bỏ (Skip).
- **Bản mới:** Thêm giải thích rõ ràng về *Design Decision*: Tuyệt đối không nạp lại (re-crawl) những post_url đã xử lý để tránh các ca khó hợp nhất (spam comment, duplicate user,...).
- **Hậu quả:** Nếu bạn crawl lại một video Tiktok nhiều lần để lấy comment mới nhất, hệ thống sẽ thẳng tay từ chối bỏ qua các gói data tiếp theo. Số comments của video đó sẽ vĩnh viễn khóa ở mức của lần crawl đầu tiên.

---

## TỔNG KẾT VÀ HƯỚNG GIẢI QUYẾT

**Kết luận:** 
Code của bạn sụt giảm data KHÔNG PHẢI DO LỖI, mà do thiết kế bộ lọc chất lượng data (Data Quality) của bản mới đang làm việc quá sức hiệu quả, vứt bỏ toàn bộ những dòng rác, dòng lỗi, và đáng kể nhất là **mạnh tay cắt đứt các bài đăng có > 5 triệu share**.

**Giải pháp (Nếu muốn lấy lại số lượng dữ liệu lớn):**
1. Vào file `step_02_clean_load.py` > hàm `clean_and_transform_posts`.
2. Chỉnh sửa hoặc comment out `#` đoạn code `df_cleaned.filter(shares <= 5000000)`.
3. Chờ chạy lại Pipeline để Lakehouse hấp thụ cả những video siêu viral cùng lượng bình luận khổng lồ đi kèm.
