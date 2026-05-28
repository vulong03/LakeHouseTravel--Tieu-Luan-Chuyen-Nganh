# BÁO CÁO MÔ HÌNH DEEP LEARNING: DỰ BÁO ĐỘ "HOT" CỦA ĐIỂM ĐẾN DU LỊCH
*(Tài liệu phục vụ Báo cáo Đồ án / Tiểu luận)*

## 1. Tổng quan Bài toán (Problem Statement)
* **Mục tiêu:** Dự báo chỉ số mức độ quan tâm (Province Hotness Score) của từng tỉnh thành trong vòng 12 tháng tới.
* **Đầu vào (Input):** Hơn 40 đặc trưng (features) đa chiều được tổng hợp sẵn từ lớp Gold (`gold.gold.fact_province_month_dl_features`).
* **Đầu ra (Output):** Dự báo độ hot của mỗi tỉnh trong 12 tháng kế tiếp, lưu trữ tại bảng Iceberg `gold.gold.province_month_forecast_lstm_next12`.
* **Thuật toán cốt lõi:** Mạng nơ-ron hồi quy cổng kết hợp cơ chế chú ý thời gian (**GRU + Temporal Attention**).

---

## 2. Dữ liệu và Kỹ thuật Trích xuất Đặc trưng (Feature Engineering)
Thay vì chỉ dùng dữ liệu lịch sử tự thân (Univariate), hệ thống tiếp cận theo hướng **Multivariate Time Series** bằng cách gom nhóm 6 loại đặc trưng khác nhau:

1. **Temporal Features:** `month_sin`, `month_cos` (Mã hóa chu kỳ thời gian theo vòng tròn lượng giác, giúp mô hình hiểu được tính mùa vụ lặp lại của du lịch).
2. **Lag Features & Momentum:** `hotness_lag_1` đến `lag_12`, `hotness_rolling_3m`, `hotness_momentum` (Độ trễ và động lượng độ hot trong quá khứ).
3. **Volume Features:** Số lượng bài đăng, bình luận, đánh giá khách sạn.
4. **Engagement Features:** Tỉ lệ bài đăng viral, lượt thích/lưu/chia sẻ trung bình (Đo lường sức lan tỏa mạng xã hội).
5. **NLP Features:** Điểm cảm xúc (Sentiment), sắc thái emoji, tỉ lệ bình luận tích cực/tiêu cực (Phân tích bằng kỹ thuật NLP ở bước trước).
6. **Hotel Features:** Điểm số khách sạn, tỉ lệ đánh giá cao.

*(Dữ liệu được chuẩn hóa bằng `MinMaxScaler` và chỉ fit trên tập Train để tránh rò rỉ dữ liệu - Data Leakage).*

---

## 3. Kiến trúc Mô hình (Model Architecture)
Tuy tên file gán nhãn là "LSTM", nhưng kiến trúc thực tế được tối ưu hóa bằng thuật toán **GRU (Gated Recurrent Unit)** kết hợp **Layer Normalization** và **Temporal Attention**. 

Sơ đồ mạng (Mạng Nơ-ron tùy chỉnh):
1. **Lớp GRU (`nn.GRU`):** Xử lý chuỗi thời gian đầu vào với `sequence_length = 4`. GRU được chọn thay vì LSTM bởi vì GRU ít tham số hơn, tốc độ hội tụ nhanh hơn nhưng vẫn giữ được khả năng ghi nhớ dài hạn xuất sắc.
2. **Layer Norm (`nn.LayerNorm`):** Giúp chuẩn hóa mảng dữ liệu nội bộ sau GRU, chống hiện tượng mất mát đạo hàm (vanishing gradient) và giúp mô hình hội tụ ổn định hơn.
3. **Mạng Chú ý Thời gian (`TemporalAttention`):** Dùng để "dạy" mô hình tự học xem trong 4 tháng quá khứ, tháng nào quan trọng nhất hình thành nên trend du lịch hiện tại. Thay vì áp trọng số đều nhau, Attention sẽ đánh trọng số cao cho tháng có ảnh hưởng mạnh nhất.
4. **Mạng kết nối đầy đủ (`Fully Connected`):** Đưa vector ngữ cảnh (Context Vector) tạo ra từ Attention đi qua một lớp Linear(16) -> ReLU -> Dropout(0.3) -> Linear(1) để xuất ra Hotness Score (0-1).

---

## 4. Chiến lược Huấn luyện & Đánh giá (Training Strategy)
Để đảm bảo mô hình dự báo học tính xu hướng chính xác:
* **Tách tập dữ liệu (Data Split):** Áp dụng Time-based split (`TRAIN_TEST_SPLIT = 0.7`). Cắt theo trục mốc thời gian để train trên dữ liệu quá khứ và dự báo (test) trên tương lai, tuyệt đối không dùng Random Split phá vỡ luồng thời gian.
* **Hàm mất mát (Loss Function):** Sử dụng **Huber Loss** (`delta=0.5`). Lợi thế cực lớn của HuberLoss là khả năng chống chịu nhiễu (outliers). Trong du lịch thường có các sự kiện đột biến (như Lễ hội, scandal), Huber Loss sẽ không bị lệch hướng quá mạnh như MSE.
* **Tối ưu hóa (Optimizer & Scheduler):** Dùng `AdamW` (Thêm Weight Decay 1e-4) chống Overfitting. Cùng `ReduceLROnPlateau` tự động giảm một nửa tốc độ học (learning rate) nếu Validation Loss không giảm sau 7 epochs.
* **Early Stopping:** Dừng sớm sau 20 epochs không cải thiện `patience=20`.

---

## 5. Chiến thuật Dự báo Tương lai (Recursive Autoregressive Forecasting)
Để dự phóng cho 12 tháng chưa từng xảy ra thay vì chỉ dự phóng 1 tháng:
1. Mô hình dự báo ra độ hot của Tháng 1 (Horizon = 1).
2. Kết quả tháng 1 ngay lập tức được hệ thống "bắt lấy", đẩy ngược vào làm đầu vào (`hotness_lag_1`) để tính tiếp cho Tháng 2.
3. Vòng lặp đệ quy đẩy lùi chỉ số lịch sử xuống (`lag_1` biến thành `lag_2`, cập nhật lại biến thời gian `sin/cos`) để sinh ra một chu kỳ khép kín 12 tháng dự báo.
Cơ chế này mô phỏng sát nhất sự tiếp nối của chuỗi hiện thực (tháng sau thừa hưởng dư âm tháng trước).

---

## 6. MLOps Cơ sở Hệ thống
* **Theo dõi thí nghiệm:** Toàn bộ thông số thiết lập (`hyperparameters`), biểu đồ Loss, biểu đồ sai số (Residuals) đều được log trực tiếp thời gian thực về máy chủ tracking **MLflow**.
* **Model Registry:** Mô hình GRU Custom cùng với biến tỷ lệ (`MinMaxScaler`) sẽ tự động được đóng gói (`mlflow.pytorch.log_model`) và đăng ký lên MLflow, sẵn sàng gọi API cho luồng suy luận sau này.