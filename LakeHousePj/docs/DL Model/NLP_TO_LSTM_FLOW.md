# Luồng Dữ Liệu Machine Learning: Từ Xử Lý Ngôn Ngữ Tự Nhiên (NLP) đến Dự Báo Dòng Thời Gian (LSTM)
========================================================================================================

Tài liệu này mô tả chi tiết luồng xử lý dữ liệu và mô hình hóa, bắt đầu từ việc phân tích văn bản bình luận (NLP) cho đến khi biến đổi thành các đặc trưng đầu vào cho mô hình dự báo học sâu (Deep Learning LSTM).

Quá trình chia làm 3 giai đoạn chính:
1. **Giai đoạn NLP**: Đọc hiểu văn bản và trích xuất đặc trưng ngôn ngữ cho từng bình luận.
2. **Giai đoạn Tổng hợp (Aggregation)**: Nhóm các đặc trưng theo không gian (Tỉnh) và thời gian (Tháng).
3. **Giai đoạn Dự báo (Forecasting)**: Dùng dữ liệu lịch sử để dự đoán lượng đặt phòng thực tế của tỉnh trong tương lai.

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
* **Weak Labeling (Gán nhãn tự động/nhãn yếu):** Để giải quyết bài toán "thiếu dữ liệu mồi" trên tập văn bản lớn (>600k dòng), ta dùng kỹ thuật Weak Labeling:
  * **Cách hoạt động:** Xây dựng một hệ thống tập luật (Heuristic Rules) dựa trên Emojis (😍, 😡) và từ khóa (Lexicon) chỉ định trong `config.py`.
  * **Ví dụ:** Câu *"View khách sạn rất đẹp nhưng phục vụ kém 😡"* -> Hệ thống bắt được từ "kém" hoặc emoji "😡" để tự gán nhãn `Negative`, đồng thời bắt từ "phục vụ" để gán nhãn khía cạnh `Service`.
  * **Mục đích:** Nhanh chóng tạo ra một tập dữ liệu nhãn mồi (pseudo-labels) có độ chính xác tương đối tốt (~70-80%) để làm "giáo trình" dạy cho PhoBERT ở bước tiếp theo.
* **Fine-Tuning PhoBERT (Tinh chỉnh mô hình):** Bản chất là kỹ thuật Học chuyển giao (Transfer Learning). Hệ thống lấy mạng nơ-ron PhoBERT (đã được VinAI dạy sẵn ngữ pháp Tiếng Việt), gắn thêm lớp phân loại (Classification Head) và dùng thuật toán tối ưu (AdamW) để huấn luyện nhẹ lại trên tập dữ liệu du lịch đã gán nhãn.
  * **Tích hợp Domain Du lịch (Domain Adaptation):** Giúp PhoBERT cập nhật lại ma trận chú ý (Attention), tự khắc kết nối các tiếng lóng du lịch (ví dụ: *"cháy phòng"*, *"view sạch nước cản"*, *"chặt chém"*) với khía cạnh `Accommodation`, hoặc nhãn `Negative` của khía cạnh `Price`. Quá trình này biến PhoBERT từ một mô hình tổng quát thành một **Chuyên gia Tâm lý Du lịch**.
* **Inference (Dự đoán hàng loạt - Output của quá trình Fine-tuning):** Đầu ra của quá trình Fine-tuning là một Model Weights (đã được tinh chỉnh). Ta dùng cục Model này quét qua toàn bộ hàng trăm nghìn bình luận thô trong Data Lake để chấm điểm (score) cho chúng.

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
  * **Biến trễ (Lag/Target):** `hotel_vol_lag_1`, `hotel_vol_lag_2`, `hotel_vol_lag_12`, v.v...
  * Biến mục tiêu **TARGET**: `hotel_review_volume` (Lượt đặt phòng thực tế của tháng hiện tại - được log-normalized).

---

## GIAI ĐOẠN 3: DỰ BÁO LSTM FORECASTING
* **Thực thi bởi:** `train_lstm_forecast.py`
* **Mục tiêu:** Tìm ra quy luật (Pattern) từ chuỗi thời gian lịch sử kết hợp với các biến NLP/Tương tác để dự đoán lượng đặt phòng thực tế (`hotel_review_volume`) của Tỉnh trong 12 tháng kế tiếp.

### 1. Đầu vào (Inputs)
Đọc bảng feature sinh ra từ bước trước: **`gold.gold.fact_province_month_dl_features`**.
Không cần tính toán lại bất kỳ feature nào trong Pipeline này, toàn bộ ở dạng sẵn sàng để train.

### 2. Quá trình xử lý
1. **Prepare Sequences:** Tạo cửa sổ trượt (Sequence Window), ví dụ `SEQUENCE_LENGTH = 4` tháng liên tục để dự đoán tháng thứ 5.
2. **Scaling:** Chuẩn hóa dữ liệu bằng `MinMaxScaler` dựa trên tập Training.
3. **Training:** Đưa vào kiến trúc mạng `LSTM + Lớp Attention`.
4. **Forecasting (Dự báo 12 tháng):** Dự báo cuốn chiếu (Autoregressive). Nó dùng mô hình để đoán Tháng T+1. Sau đó lấy kết quả T+1 nhét vào chuỗi đầu vào để dự đoán tiếp T+2, T+3,... và tính toán lại động các thuộc tính lag (như `rolling_avg` và `lag_12` từ quá khứ).
5. **Inverse Transform:** Sử dụng `expm1` đưa kết quả từ log-scale về định dạng volume thực tế.

### 3. Đầu ra (Outputs)
Kết quả sẽ được ghi xuất dưới hai dạng: Artifact mô hình (MLFlow) và Dữ liệu dự báo.

1. **Mô hình Model (Lưu trên MLflow):** 
   Tên mô hình: `province_hotel_volume_forecaster_lstm`. Các file hỗ trợ kèm theo: `scaler_lstm.pkl`, `feature_config.json`.
2. **Bảng kết quả Iceberg trên DL:** **`gold.gold.province_month_forecast_lstm_next12`**.
   * **Cấu trúc:**
     * `province_sk`, `province_name`, `region`: Thông tin định danh của Tỉnh.
     * `year`, `month`, `year_month`: Thời điểm được dự báo tương lai.
     * `horizon_month` (Int, từ 1 tới 12): Quãng thời gian dự báo (Tháng T+1 tới T+12).
     * `predicted_hotel_volume_actual` (Double): **Kết quả dự đoán** - Lượt đặt phòng thực tế sau expm1.
     * `predicted_growth_pct` (Double): Tốc độ tăng trưởng dự tính.
     * `forecast_date`: Ngày thực hiện dự báo.
3. **File Parquet Export:**
   * Một bản copy Parquet xuất thẳng lên Cloud/MinIO: `s3a://gold/ml_forecast/province_hotel_volume_forecast_lstm_*` để API giao diện (Gradio App) có thể dễ dàng truy xuất phục vụ biểu đồ.
