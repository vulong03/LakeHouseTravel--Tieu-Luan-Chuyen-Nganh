# Báo Cáo Mô Hình Deep Learning: Dự Báo Lượng Đặt Phòng Khách Sạn (v5.0)
*(Tài liệu phục vụ Báo cáo Đồ án / Tiểu luận)*

---

## 1. Tổng quan Bài toán (Problem Statement)
* **Mục tiêu:** Dự báo lượng đặt phòng khách sạn thực tế (thông qua proxy đại diện là số lượng phản hồi/đánh giá của khách hàng `hotel_review_volume`) của 62 tỉnh/thành Việt Nam trong vòng 12 tháng tới.
* **Đầu vào (Input):** 50 đặc trưng (features) đa chiều kết hợp giữa mạng xã hội (TikTok) và lưu trú (Booking.com) được tổng hợp sẵn từ lớp Gold (`gold.gold.fact_province_month_dl_features`).
* **Đầu ra (Output):** Dự báo lượng đặt phòng thực tế (sau khi chuyển đổi ngược từ log-scale qua hàm `expm1`), lưu trữ tại bảng Iceberg `gold.gold.province_month_forecast_lstm_next12`.
* **Thuật toán cốt lõi:** Mạng nơ-ron hồi quy dài-ngắn hạn kết hợp cơ chế chú ý thời gian (**LSTM + Temporal Attention**).

---

## 2. Dữ liệu và Kỹ thuật Trích xuất Đặc trưng (Feature Engineering)
Thay vì chỉ dùng dữ liệu lịch sử tự thân (Univariate), hệ thống tiếp cận theo hướng **Multivariate Time Series** bằng cách gom nhóm 9 loại đặc trưng khác nhau:

1. **Temporal Features:** `month_sin`, `month_cos` (Mã hóa chu kỳ thời gian theo vòng tròn lượng giác, giúp mô hình hiểu được tính mùa vụ lặp lại của du lịch).
2. **Hotel Lag Features & Momentum:** `hotel_vol_lag_1` đến `lag_12`, `hotel_vol_rolling_3m`, `hotel_vol_momentum` (Độ trễ và tốc độ biến động của lượng đặt phòng).
3. **Hotness Lag Features & Momentum:** `hotness_lag_1` đến `lag_12`, `hotness_rolling_3m`, `hotness_momentum` (TikTok delayed attention signal).
4. **Volume Features:** Số lượng bài đăng, bình luận trên TikTok.
5. **Engagement Features:** Tỉ lệ bài đăng viral, lượt thích/lưu trung bình trên TikTok (Đo lường sức lan tỏa mạng xã hội).
6. **NLP Features:** Điểm cảm xúc (Sentiment), sắc thái emoji, tỉ lệ bình luận tích cực/tiêu cực phân tích bằng thư viện NLP.
7. **Aspect Features:** 6 đặc trưng khía cạnh từ PhoBERT ABSA (scenery, food, price, service, transport, accommodation).
8. **Hotel Features:** Điểm số khách sạn trung bình, tỉ lệ đánh giá cao.
9. **Custom Features:** Tỷ lệ social/booking, thay đổi cực tính cảm xúc, độ lệch chuẩn lăn lượng đặt phòng.

*(Dữ liệu được chuẩn hóa bằng `RobustScaler` (sử dụng Median và IQR để tăng tính chống nhiễu từ các tỉnh có tương tác cực lớn) và chỉ fit trên tập Train để tránh rò rỉ dữ liệu - Data Leakage).*
*(Target variable được biến đổi log-scale qua hàm `log1p` để thu hẹp khoảng biến động lớn giữa các địa phương).*

---

## 3. Kiến trúc Mô hình (Model Architecture)
Mô hình sử dụng kiến trúc mạng **LSTM (Long Short-Term Memory)** kết hợp **Layer Normalization** và **Temporal Attention**.

Sơ đồ mạng (Mạng Nơ-ron tùy chỉnh):
1. **Lớp LSTM (`nn.LSTM`):** Xử lý chuỗi thời gian đầu vào với `sequence_length = 3` tháng liên tiếp. LSTM được chọn vì khả năng ghi nhớ dài hạn vượt trội nhờ cơ chế 3 cổng (input gate, forget gate, output gate) và cell state riêng biệt — đặc biệt phù hợp để nắm bắt tính mùa vụ (seasonality) và xu hướng dài hạn trong dữ liệu du lịch theo tháng.
2. **Layer Norm (`nn.LayerNorm`):** Giúp chuẩn hóa mảng dữ liệu nội bộ sau LSTM, chống hiện tượng mất mát đạo hàm (vanishing gradient) và giúp mô hình hội tụ ổn định hơn.
3. **Mạng Chú ý Thời gian (`TemporalAttention`):** Dùng để "dạy" mô hình tự học xem trong 3 tháng quá khứ, tháng nào quan trọng nhất hình thành nên hành vi du lịch hiện tại. Thay vì áp trọng số đều nhau, Attention sẽ đánh trọng số cao cho tháng có ảnh hưởng mạnh nhất.
4. **Mạng kết nối đầy đủ (`Fully Connected`):** Đưa vector ngữ cảnh (Context Vector) tạo ra từ Attention đi qua một lớp Linear(hidden) → GELU → Dropout(0.43) → Linear(16) → ReLU → Linear(1) để xuất ra giá trị log-volume dự báo (với `hidden_size = 48` và `num_layers = 2`).

---

## 4. Chiến lược Huấn luyện & Đánh giá (Training Strategy)
* **Tách tập dữ liệu (Data Split):** Áp dụng 3-way split theo thời gian (`TRAIN_RATIO = 0.70`, `VAL_RATIO = 0.15`, `TEST_RATIO = 0.15`). Cắt theo trục mốc thời gian để train trên dữ liệu quá khứ, val dùng chọn epoch tốt nhất cho early stopping và test dùng kiểm tra khách quan chỉ một lần cuối cùng.
* **Hàm mất mát (Loss Function):** Sử dụng **Hybrid Loss** (70% Huber Loss với `delta=0.5` + 30% Symmetric MAPE Loss). Lợi thế là HuberLoss giúp ổn định gradient và giảm thiểu tác động của ngoại lệ, trong khi SMAPELoss hướng mô hình tối ưu trực tiếp cho phần trăm sai số (MAPE).
* **Tối ưu hóa (Optimizer & Scheduler):** Dùng `Adam` (weight_decay=7e-4) chống Overfitting. Kết hợp `CosineAnnealingWarmRestarts` giúp tối ưu hóa tốt hơn và tránh bị kẹt ở các cực trị cục bộ (step đồng bộ theo epoch).
* **Early Stopping:** Dừng sớm sau 25 epochs không cải thiện (`patience=25` trên Val Loss).

---

## 5. Chiến thuật Dự báo Tương lai (Recursive Autoregressive Forecasting)
Để dự phóng cho 12 tháng chưa từng xảy ra thay vì chỉ dự phóng 1 tháng:
1. Mô hình dự báo ra lượng đặt phòng (log-scale) của Tháng 1 (Horizon = 1).
2. Kết quả này được giải scale (RobustScaler) và đẩy ngược vào làm đầu vào (`hotel_vol_lag_1`) để tính tiếp cho Tháng 2.
3. Vòng lặp đệ quy đẩy lùi chỉ số lịch sử xuống (`lag_1` biến thành `lag_2`, cập nhật lại biến thời gian `sin/cos`, tự động tính toán lại `rolling_avg` và nạp thêm `lag_12` thực tế từ quá khứ) để sinh ra một chu kỳ khép kín 12 tháng dự báo.
4. Cuối cùng, hàm **`expm1`** được sử dụng để đưa toàn bộ chuỗi dự báo về thang đo số lượng thực tế trước khi lưu vào Iceberg.

---

## 6. MLOps Cơ sở Hệ thống
* **Theo dõi thí nghiệm:** Toàn bộ thông số thiết lập (`hyperparameters`), biểu đồ Loss, biểu đồ sai số (Residuals) đều được log trực tiếp thời gian thực về máy chủ tracking **MLflow**.
* **Model Registry:** Mô hình LSTM Custom cùng với biến tỷ lệ (`RobustScaler`) sẽ tự động được đóng gói (`mlflow.pytorch.log_model`) và đăng ký lên MLflow dưới tên `province_hotel_volume_forecaster_lstm_v5`, sẵn sàng gọi API cho luồng suy luận sau này.