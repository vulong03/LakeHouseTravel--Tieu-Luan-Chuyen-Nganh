# Luồng Dữ Liệu Machine Learning: Từ Xử Lý Ngôn Ngữ Tự Nhiên (NLP) đến Dự Báo Dòng Thời Gian (LSTM/GRU)

Tài liệu này mô tả chi tiết luồng xử lý dữ liệu và mô hình hóa, bắt đầu từ việc phân tích văn bản bình luận (NLP) cho đến khi biến đổi thành các đặc trưng đầu vào cho mô hình dự báo học sâu (Deep Learning LSTM/GRU).

Quá trình chia làm 3 giai đoạn chính:
1. **Giai đoạn NLP**: Đọc hiểu văn bản và trích xuất đặc trưng ngôn ngữ cho từng bình luận.
2. **Giai đoạn Tổng hợp (Aggregation)**: Nhóm các đặc trưng theo không gian (Tỉnh) và thời gian (Tháng).
3. **Giai đoạn Dự báo (Forecasting)**: Dùng dữ liệu lịch sử để dự đoán độ hot của tỉnh trong tương lai.

---

## GIAI ĐOẠN 1: QUY TRÌNH NLP PHOBERT (TRÍCH XUẤT ĐẶC TRƯNG VĂN BẢN)
* **Thực thi bởi:** `weak_labeling.py` -> `train_phobert.py` -> `inference_phobert.py` (Cấu hình bởi `config.py`)
* **Mục tiêu:** Biến đổi bình luận dạng chữ (Text) của du khách thành các con số toán học thể hiện cảm xúc, ý định và khía cạnh đánh giá.

### 1. Đầu vào (Inputs)
Hệ thống lấy dữ liệu trực tiếp từ các bảng chứa bình luận đã được làm sạch cơ bản:
* **`gold.gold.dim_comment`**: Bảng chứa thông tin bình luận cốt lõi.
  * `comment_sk`, `post_sk`: Khóa chính định danh bình luận và bài viết.
  * `province_sk`: Mã tỉnh thành mà bài viết đề cập tới.
  * `comment_date_sk`: Ngày tháng bình luận.
  * `comment_text`: Nội dung bình luận thô (Ví dụ: *"Đồ ăn ở đây đắt mà phục vụ chán quá"*).
* **`silver.silver.tiktok_post_comments`**: Lấy thêm thông tin về lượt thích (likes) của bình luận.

### 2. Quá trình xử lý
* **Weak Labeling:** Dùng tập luật từ khóa và Emojis (định nghĩa trong `config.py`) để gán nhãn tự động cho tập dữ liệu mẫu.
* **Fine-Tuning PhoBERT:** Đào tạo mô hình ngôn ngữ dựa trên tập dữ liệu đã gán nhãn.
* **Inference:** Dùng mô hình đã đào tạo để quét và chấm điểm lại toàn bộ hàng trăm nghìn bình luận trong Data Lake.

### 3. Đầu ra (Outputs)
Kết quả xuất ra bảng trung gian **`gold.gold.fact_comment_nlp_v2`**.
* **Độ chi tiết (Granularity):** Từng bình luận cá nhân.
* **Cấu trúc dữ liệu chính:**
  * Khóa liên kết: `comment_sk`, `post_sk`, `province_sk`, `comment_date_sk`.
  * **NLP Features:**
    * `sentiment_score` (Double, 0.0 - 1.0): Điểm cảm xúc, càng gần 1 càng tích cực.
    * `sentiment_label` (String): Gồm tiêu cực, trung lập, tích cực.
    * `aspect_...` (Double, 0.0 - 1.0): Xác suất bình luận nói về 6 khía cạnh cụ thể: *scenery* (cảnh quan), *food* (đồ ăn), *price* (giá cả), *service* (dịch vụ), *transport* (di chuyển), *accommodation* (nhà ở).
    * `intent_label` (String): Ý định (*recommend*, *complain*, *question*, *share*).
  * **Metadata Features:**
    * `word_count` (Int): Tổng số từ trong bình luận.
    * `emoji_count` (Int): Tổng số biểu tượng cảm xúc.

---

## GIAI ĐOẠN 2: CHUYỂN ĐỔI VÀ TỔNG HỢP (AGGREGATION - BƯỚC CẦU NỐI)
* **Trách nhiệm:** Hệ thống ETL/Data Warehouse.
* **Mục tiêu:** Chuyển đổi dữ liệu từ cấp độ **"từng bình luận cá nhân"** thành dạng **"thống kê theo Tỉnh và Tháng"** (Time-series) để phù hợp cho mô hình dự báo.

### 1. Đầu vào (Inputs)
* `gold.gold.fact_comment_nlp_v2` (Lấy yếu tố NLP)
* Các bảng Fact/Dim khác về khách sạn, lượng bài viết, lượt tương tác (views, likes, shares).

### 2. Quá trình xử lý
Nhóm dữ liệu lại theo cấu trúc `(province_sk, year_month)` (VD: Hà Nội - Tháng 10/2023) và tính trung bình, tổng số, độ lệch chuẩn,...

### 3. Đầu vào (Outputs - Đây chính là Input cho Giai đoạn 3)
Xuất ra bảng **`gold.gold.fact_province_month_dl_features`**.
* **Độ chi tiết:** Từng tỉnh, Từng tháng.
* **Các nhóm biến (Features):** (~40 features)
  * **Biến chuỗi thời gian (Temporal):** `month_sin`, `month_cos`.
  * **Biến tương tác (Engagement):** `avg_likes_per_post`, `avg_saves_per_post`, `viral_post_ratio`...
  * **Biến NLP (Kế thừa từ Giai đoạn 1):** 
    * `avg_sentiment`: Điểm cảm xúc trung bình của Tỉnh tháng đó.
    * `positive_ratio`, `negative_ratio`: Tỷ lệ % bình luận tích cực/tiêu cực.
    * `avg_word_count`: Mức độ đầu tư viết bình luận.
  * **Biến trễ (Lag/Target):** `hotness_lag_1`, `hotness_lag_2`, `hotness_lag_12`, v.v...
  * Biến mục tiêu **TARGET**: `hotness_score` (Điểm độ hot thực tế của tháng hiện tại).

---

## GIAI ĐOẠN 3: DỰ BÁO LSTM/GRU FORECASTING
* **Thực thi bởi:** `train_lstm_forecast.py`
* **Mục tiêu:** Tìm ra quy luật (Pattern) từ chuỗi thời gian lịch sử kết hợp với các biến NLP/Tương tác để dự đoán điểm độ hot (`hotness_score`) của Tỉnh trong 12 tháng kế tiếp.

### 1. Đầu vào (Inputs)
Đọc bảng feature sinh ra từ bước trước: **`gold.gold.fact_province_month_dl_features`**.
Không cần tính toán lại bất kỳ feature nào trong Pipeline này, toàn bộ ở dạng sẵn sàng để train.

### 2. Quá trình xử lý
1. **Prepare Sequences:** Tạo cửa sổ trượt (Sequence Window), ví dụ `SEQUENCE_LENGTH = 4` tháng liên tục để dự đoán tháng thứ 5.
2. **Scaling:** Chuẩn hóa dữ liệu bằng `MinMaxScaler` dựa trên tập Training.
3. **Training:** Đưa vào kiến trúc mạng `GRU + Lớp Attention`.
4. **Forecasting (Dự báo 12 tháng):** Dự báo cuốn chiếu (Autoregressive). Nó dùng mô hình để đoán Tháng T+1. Sau đó lấy kết quả T+1 nhét vào chuỗi đầu vào để dự đoán tiếp T+2, T+3,...

### 3. Đầu ra (Outputs)
Kết quả sẽ được ghi xuất dưới hai dạng: Artifact mô hình (MLFlow) và Dữ liệu dự báo.

1. **Mô hình Model (Lưu trên MLflow):** 
   Tên mô hình: `province_hotness_forecaster_lstm`. Các file hỗ trợ kèm theo: `/tmp/scaler_lstm.pkl`, `feature_config.json`.
2. **Bảng kết quả Iceberg trên DL:** **`gold.gold.province_month_forecast_lstm_next12`**.
   * **Cấu trúc:**
     * `province_sk`, `province_name`, `region`: Thông tin định danh của Tỉnh.
     * `year`, `month`, `year_month`: Thời điểm được dự báo tương lai.
     * `horizon_month` (Int, từ 1 tới 12): Quãng thời gian dự báo (Tháng T+1 tới T+12).
     * `predicted_hotness` (Double): **Kết quả dự đoán** - Điểm độ hot được hệ thống dự tính.
     * `forecast_date`: Ngày thực hiện dự báo.
3. **File Parquet Export:**
   * Một bản copy Parquet xuất thẳng lên Cloud/MinIO: `s3a://gold/ml_forecast/province_hotness_forecast_lstm_*` để API giao diện (Gradio App) có thể dễ dàng truy xuất phục vụ biểu đồ.
