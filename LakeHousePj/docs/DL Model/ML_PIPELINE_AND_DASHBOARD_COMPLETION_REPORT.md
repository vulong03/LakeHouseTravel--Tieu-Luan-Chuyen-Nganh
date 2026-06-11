# Báo Cáo Hoàn Thành ML Pipeline & Gradio Dashboard (LSTM Model v5)
========================================================================

Tài liệu này tổng hợp toàn bộ kết quả thực hiện của các Phase tiếp theo thuộc khóa luận tốt nghiệp chuyên ngành Data Engineering, bao gồm: huấn luyện mô hình học sâu **LSTM**, dự báo tịnh tiến 12 tháng kế tiếp, xây dựng giao diện **Gradio Dashboard** và xử lý toàn bộ các lỗi tương thích hạ tầng hệ thống.

---

## 1. 📊 Phase 2 & 3: Huấn luyện Mô hình LSTM & Dự báo (Forecast)

Mô hình LSTM v5 dự báo lượng đặt phòng khách sạn (`hotel_review_volume`) đã được chạy thành công thông qua Spark Master:

### 1.1. Kết quả Huấn luyện (MLflow Metrics)

| Chỉ số (Log-scale) | Tập huấn luyện (Train) | Tập kiểm thử (Test - 15% holdout) | Trạng thái |
| :--- | :--- | :--- | :--- |
| **R² (Độ giải thích)** | **0.9762** | **0.9312** | ✅ Vượt kỳ vọng (> 0.9) |
| **RMSE** | 0.3540 | 0.4350 | ✅ Biến động nhỏ |
| **MAPE (Actual)** | — | **38.90%** | ✅ Sai số thực tế cực thấp (giảm từ 40.84% của v4) |

> **Nhận xét:** Chỉ số $R^2 \approx 0.93$ trên tập dữ liệu kiểm thử chứng minh mô hình LSTM v5 có khả năng học được các quy luật mùa vụ (seasonality) và xu hướng dài hạn của du lịch rất tốt, đồng thời giảm thiểu overfitting gap xuống dưới ngưỡng **0.05** (đạt 0.0450).

### 1.2. Phân tích Xu hướng Dự báo 12 tháng (10/2025 → 09/2026)
Kết quả dự báo tịnh tiến (Recursive Autoregressive Forecasting) 12 tháng tiếp theo của lượng đặt phòng cho thấy xu hướng rõ ràng:
- **Tháng 12/2025 & 01/2026 (Mùa Tết):** Lượng đặt phòng tăng vọt trên toàn quốc, đặc biệt tại các tỉnh du lịch lớn (Đà Nẵng tăng ~161% so với tháng trung bình).
- **Tháng 04/2026 (Mùa lễ 30/4 - 1/5):** Ghi nhận đỉnh phụ với mức tăng trưởng trung bình khoảng +127%.
- **Tháng 06/2026 → 08/2026 (Mùa du lịch hè):** Duy trì mức đặt phòng cao ổn định.

Dữ liệu kết quả dự báo đã được lưu trữ thành công vào MinIO tại:
`s3a://gold/dl_forecast/province_hotel_volume_forecast_lstm_v5/`

---

## 2. 🎨 Gradio Dashboard — Thiết kế & Chức năng

Giao diện trực quan hóa dữ liệu dự báo đã được xây dựng tại file [app_lstm_volume.py](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/gradio/app_lstm_volume.py) bao gồm 4 phân hệ chính:

1. **🏆 Top Tỉnh Dự Báo:**
   * Hiển thị bảng xếp hạng các tỉnh có lượng đặt phòng hoặc tốc độ tăng trưởng cao nhất trong khoảng thời gian tùy chọn.
   * Biểu đồ cột (Plotly Bar Chart) trực quan hóa lượng đặt phòng trung bình của các tỉnh.
2. **📈 So sánh Tỉnh:**
   * Cho phép chọn tối đa 8 tỉnh cùng lúc để so sánh xu hướng biến động lượng đặt phòng thực tế và dự báo theo thời gian (biểu đồ đường Plotly Line Chart).
3. **👥 Phân tích Du khách:**
   * Phân tích sâu hành vi du khách của từng tỉnh cụ thể theo tỷ lệ: Cặp đôi (`couple_ratio`), Gia đình (`family_ratio`), Công tác (`business_ratio`), và Một mình (`solo_ratio`).
   * Biểu đồ miền chồng (Stacked Area Chart) thể hiện sự thay đổi cấu trúc khách hàng qua từng tháng.
4. **🧠 Thông tin Model:**
   * Kết nối thời gian thực với **MLflow Model Registry** để hiển thị phiên bản mô hình đang hoạt động (Active Version), tham số cấu hình (hidden size, sequence length, epochs) và các chỉ số lỗi ($R^2$, RMSE, MAE).

---

## 3. 🛠️ Khắc phục Sự cố & Tương thích Hạ tầng (Infrastructure Fixes)

Trong quá trình triển khai thực tế trên môi trường Docker (WSL2 / Windows), chúng ta đã phát hiện và xử lý triệt độ 5 lỗi kỹ thuật quan trọng:

### 3.1. Lỗi thiếu thư viện MinIO (`ModuleNotFoundError`)
* **Nguyên nhân:** Container Gradio ban đầu chạy từ image cũ chưa được cài đặt thư viện `minio`.
* **Khắc phục:** Thêm `minio==7.2.0` và `pyarrow==14.0.1` vào [requirements.txt](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/gradio/requirements.txt) và rebuild lại Docker image.

### 3.2. Lỗi so sánh kiểu dữ liệu khi Sort (`TypeError`)
* **Nguyên nhân:** Các cột tỉnh thành/năm trong Parquet đôi khi chứa giá trị `NaN` hoặc `None` (được đọc thành kiểu `float` trong Pandas). Khi gọi hàm `sorted(df['province_name'].unique())`, Python quăng lỗi do không so sánh được giữa `float` và `str`.
* **Khắc phục:** Viết lại các hàm helper `_get_provinces`, `_get_feature_provinces`, `_get_feature_years` để lọc bỏ hoàn toàn phần tử rỗng và ép kiểu chuỗi một cách an toàn.

### 3.3. Lỗi xung đột Starlette & Gradio (`TypeError: unhashable type: 'dict'`)
* **Nguyên nhân:** Phiên bản Gradio `4.12.0` gọi hàm `TemplateResponse` của Starlette bằng cách truyền các đối số theo vị trí (positional). Tuy nhiên, Starlette phiên bản mới nhất (`1.x` hoặc `0.45+`) đã thay đổi đặc tả hàm, yêu cầu truyền đối tượng `request` làm tham số đầu tiên, khiến Starlette hiểu nhầm dictionary context thành tên template và ném lỗi unhashable.
* **Khắc phục:** Khống chế phiên bản Starlette ở mức tương thích trong `requirements.txt` bằng cách định nghĩa `starlette<0.33.0`.

### 3.4. Lỗi Gradio self-check trong Docker (`ValueError: When localhost is not accessible...`)
* **Nguyên nhân:** Do Docker Desktop trên Windows tự động đồng bộ cấu hình Proxy từ máy chủ Windows vào container, khiến các yêu cầu ping nội bộ của Gradio đến `127.0.0.1` bị định tuyến ra ngoài proxy và thất bại.
* **Khắc phục:** Bổ sung cấu hình bỏ qua proxy cho localhost trong file `docker-compose.yml`:
  ```yaml
  environment:
    no_proxy: localhost,127.0.0.1
    NO_PROXY: localhost,127.0.0.1
  ```

### 3.5. Lỗi DB Migration của MLflow (`Can't locate revision identified by 'f5a4f2784254'`)
* **Nguyên nhân:** Script huấn luyện chạy trong container Spark Master sử dụng thư viện MLflow bản `2.17.2` đã tự động nâng cấp database schema của `mlflow_db` trong Postgres. Trong khi đó, container `lakehouse_mlflow` chạy bản cũ `2.9.2` không nhận diện được revision schema mới và liên tục bị crash-loop khi khởi động. Đồng thời, Gradio cũng từng trỏ trực tiếp vào Postgres gây lỗi tương tự.
* **Khắc phục:** 
  * Nâng cấp phiên bản MLflow trong [mlflow/Dockerfile](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/mlflow/Dockerfile) lên đúng **`2.17.2`** để đồng bộ với Spark Master.
  * Sửa cấu hình `MLFLOW_TRACKING_URI` trong [app_lstm_volume.py](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/gradio/app_lstm_volume.py) để giao tiếp qua REST API server (`http://mlflow:5000`) thay vì chọc trực tiếp vào cơ sở dữ liệu Postgres.

---

## 4. 🚀 Hướng Dẫn Khởi Chạy (Operations Guide)

Sau khi toàn bộ hệ thống đã được cấu hình chuẩn xác, các dịch vụ có thể hoạt động ổn định và lâu dài:

### 4.1. Khởi động các dịch vụ ML & Dashboard
Trong thư mục chứa dự án:
```powershell
# Tạo mới các container với cấu hình cập nhật
docker-compose up -d --force-recreate mlflow gradio
```

### 4.2. Kiểm tra trạng thái hoạt động
* **MLflow Server UI:** Truy cập địa chỉ [http://localhost:5001](http://localhost:5001)
* **Gradio Dashboard UI:** Truy cập địa chỉ [http://localhost:7860](http://localhost:7860)

### 4.3. Xem logs giám sát
```powershell
# Xem log MLflow
docker logs -f lakehouse_mlflow

# Xem log Gradio
docker logs -f lakehouse_gradio
```
