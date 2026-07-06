# CÔNG THỨC TÍNH TOÁN CÁC CỘT PHÁI SINH VÀ LOGIC DỰ BÁO TỰ HỒI QUY (LSTM & GRU)
*(Tài liệu học thuật thuyết minh phương pháp tính toán và xử lý dữ liệu đầu ra)*

---

## 1. Tổng quan
Trong mô hình học sâu dự báo lượng lưu trú của đề tài (LSTM và GRU), biến mục tiêu thực tế được biến đổi sang thang đo logarit nhằm thu hẹp khoảng dao động cực lớn giữa các tỉnh thành lớn (như TP. Hồ Chí Minh, Hà Nội) và các tỉnh thành nhỏ. 

Giá trị đầu ra trực tiếp của mô hình hồi quy (được ký hiệu là $\hat{y}$) là giá trị dự báo ở **thang đo logarit**:
$$\hat{y} = \log(1 + y_{\text{actual}})$$

Để phục vụ trực quan hóa dữ liệu trên ứng dụng Dashboard (Gradio) và lưu trữ dữ liệu hoàn chỉnh ở tầng Gold phục vụ phân tích BI, hệ thống tự động thực hiện các phép biến đổi toán học để sinh ra các **cột dữ liệu phái sinh**. Tài liệu này thuyết minh chi tiết công thức toán học và logic cập nhật chuỗi thời gian tự hồi quy phục vụ việc viết khóa luận/báo cáo.

---

## 2. Công thức toán học các cột phái sinh

Sau mỗi bước dự báo tự hồi quy của mô hình đối với một tỉnh thành, từ giá trị dự báo gốc $\hat{y}$ (`predicted_hotel_volume`), hệ thống tính toán ra các cột dữ liệu phái sinh sau:

### 2.1. Lượng lưu trú thực tế dự kiến (`predicted_hotel_volume_actual`)
* **Mục đích:** Khôi phục giá trị dự báo về đơn vị đếm thực tế (số lượng đánh giá/phòng) để hiển thị trên biểu đồ và báo cáo.
* **Công thức toán học:**
  $$y_{\text{actual}} = e^{\hat{y}} - 1$$
* **Triển khai trong mã nguồn:**
  ```python
  pred_actual = float(np.expm1(pred_log))
  ```
  *(Sử dụng hàm `np.expm1` giúp tránh sai số số học dấu phẩy động khi giá trị dự báo gần bằng 0 so với phép tính $e^x - 1$ truyền thống).*

### 2.2. Tỷ lệ tăng trưởng dự kiến (`predicted_growth_pct`)
* **Mục đích:** Đo lường phần trăm biến động về nhu cầu lưu trú của tỉnh thành so với bước thời gian (tháng) liền trước.
* **Công thức toán học:**
  $$\text{Growth Pct}_{t} = \frac{y_{\text{actual}, t} - y_{\text{actual}, t-1}}{y_{\text{actual}, t-1}} \times 100\%$$
  Trong đó:
  * $y_{\text{actual}, t}$ là giá trị dự báo thực tế tại bước thời gian hiện tại (tháng $t$).
  * $y_{\text{actual}, t-1}$ là giá trị thực tế (hoặc giá trị dự báo) ở tháng ngay trước đó (tháng $t-1$).
* **Triển khai trong mã nguồn:**
  ```python
  prev_volume = recent_volumes[-1] if recent_volumes else 0.0
  growth_pct  = float((pred_actual - prev_volume) / prev_volume * 100.0) if prev_volume > 0 else 0.0
  ```
  > [!NOTE]
  > **Điểm cải tiến ở v5.0:** Tỷ lệ tăng trưởng được tính động dựa trên giá trị của tháng liền trước ($t-1$) thay vì so sánh cố định với tháng lịch sử cuối cùng có dữ liệu thực tế. Điều này phản ánh chính xác xu hướng tăng giảm theo từng bước thời gian của chu kỳ 12 tháng dự báo.

### 2.3. Bước dự báo tương lai (`horizon_month`)
* **Định nghĩa:** Giá trị số nguyên chạy từ $1$ đến $12$, đại diện cho số bước (tháng) dự báo dịch chuyển về tương lai kể từ mốc lịch sử cuối cùng có dữ liệu thực tế ($t_0$).
  * $\text{Horizon} = 1$: Dự báo cho tháng $t_0 + 1$.
  * $\text{Horizon} = 12$: Dự báo cho tháng $t_0 + 12$.

### 2.4. Các thông tin định danh và thời gian
* **`year` & `month`:** Năm và tháng tương ứng với bước dự báo hiện tại.
* **`year_month` (`YYYYMM`):** Khóa định danh thời gian (ví dụ: `202512` đại diện cho tháng 12 năm 2025) phục vụ phân vùng (partition) bảng dữ liệu Iceberg.
* **`forecast_date`:** Ngày hệ thống thực hiện tiến trình chạy dự báo (định dạng `YYYY-MM-DD`).
* **`model_version`:** Phiên bản mô hình (ví dụ: `lstm_v5` hoặc `gru_v5`) đăng ký trên MLflow Model Registry để truy vết nguồn gốc dữ liệu.

---

## 3. Logic cập nhật chuỗi thời gian tự hồi quy (Recursive Autoregressive)

Mô hình học sâu sử dụng cửa sổ lịch sử trượt với độ dài $L = 3$ tháng liên tiếp để dự báo cho tháng tiếp theo. Khi tiến hành dự báo 12 tháng tương lai (nơi dữ liệu thực tế chưa xuất hiện), hệ thống áp dụng **cơ chế tự hồi quy đệ quy (Recursive Autoregressive)**: lấy kết quả dự báo của bước trước làm một phần đầu vào để dự báo cho bước sau.

Tại mỗi bước dự báo (từ horizon 1 đến 12), ma trận đặc trưng $3 \times 50$ của tỉnh thành được cập nhật động như sau:

```
[Tháng t-3, Tháng t-2, Tháng t-1] ──► [ Mô hình Deep Learning ] ──► Dự báo Tháng t (pred_actual)
       │                                                                  │
       ▼ (Tịnh tiến cửa sổ - Shift Window)                                 │
[Tháng t-2, Tháng t-1, Dự báo Tháng t] ◄──────────────────────────────────┘
```

### 3.1. Cập nhật các đặc trưng lượng giác thời gian (Temporal Features)
Các đặc trưng tuần hoàn mùa vụ được cập nhật chính xác theo lịch của tháng dự báo hiện tại ($m \in [1, 12]$) và chuẩn hóa qua RobustScaler:
$$x_{\sin} = \sin\left(\frac{2\pi \cdot m}{12}\right)$$
$$x_{\cos} = \cos\left(\frac{2\pi \cdot m}{12}\right)$$

### 3.2. Cập nhật động các đặc trưng độ trễ lưu trú (Hotel Volume Lags)
Các đặc trưng tự tương quan (autocorrelation) của lượng lưu trú được cập nhật tịnh tiến đệ quy:
* **Độ trễ 1 tháng (`hotel_vol_lag_1`):** Nhận giá trị dự báo thực tế của tháng ngay trước đó sau khi chuẩn hóa bằng RobustScaler:
  $$\text{New Lag 1} = \text{Scale}\left(y_{\text{actual}, t-1}\right)$$
* **Độ trễ 2 & 3 tháng (`hotel_vol_lag_2`, `hotel_vol_lag_3`):** Lần lượt dịch chuyển giá trị cũ sang:
  $$\text{New Lag 2} = \text{Lag 1 cũ}$$
  $$\text{New Lag 3} = \text{Lag 2 cũ}$$
* **Độ trễ 12 tháng (`hotel_vol_lag_12`):** Nhận giá trị thực tế hoặc giá trị đã dự báo tại thời điểm 12 tháng trước đó:
  $$\text{New Lag 12} = \text{Scale}\left(y_{\text{actual}, t-12}\right)$$
* **Lượng lưu trú trung bình trượt 3 tháng (`hotel_vol_rolling_3m`):** Được tính toán động bằng trung bình cộng lượng lưu trú (đã giải scale) của 3 tháng gần nhất, sau đó mới chuẩn hóa:
  $$\text{Rolling 3M} = \text{Scale}\left(\frac{y_{\text{actual}, t-1} + y_{\text{actual}, t-2} + y_{\text{actual}, t-3}}{3}\right)$$
* **Gia tốc biến động lượng lưu trú (`hotel_vol_momentum`):** Tính toán từ chênh lệch hoặc tỷ lệ giữa các kỳ trễ gần nhất để phản ánh động năng của chuỗi thời gian.

### 3.3. Giả định tính ổn định của các đặc trưng Xã hội và NLP (Persistence Assumption)
Đối với các đặc trưng truyền thông mạng xã hội và phân tích cảm xúc (ví dụ: `hotness_score`, `avg_sentiment`, các khía cạnh PhoBERT như `avg_aspect_scenery`, v.v.), do không có mô hình dự báo riêng cho 50 biến này trong tương lai, hệ thống áp dụng **giả định duy trì (Persistence Assumption)**:
> *"Các đặc trưng tương tác mạng xã hội, cảm xúc và khía cạnh thảo luận của khách hàng được giữ cố định bằng giá trị thực tế tại tháng lịch sử cuối cùng trong suốt quá trình dự báo 12 tháng tương lai. Điều này tương ứng với giả định rằng mức độ quan tâm truyền thông và xu hướng cảm nhận của người dùng về du lịch của một địa phương sẽ duy trì trạng thái ổn định ngắn hạn trong vòng một năm tiếp theo."*

---

## 4. Tích hợp hiển thị trên Dashboard (Gradio)
Dữ liệu dự báo hoàn chỉnh sau khi xuất ra định dạng Parquet tại MinIO sẽ được ứng dụng Gradio đọc và hiển thị qua các tab chức năng:
1. **Dự báo xu hướng:** Vẽ biểu đồ đường nối liền giữa chuỗi dữ liệu thực tế lịch sử và 12 tháng dữ liệu dự báo phái sinh (`predicted_hotel_volume_actual`), kèm theo khoảng tin cậy.
2. **Xếp hạng tăng trưởng:** Sử dụng cột `predicted_growth_pct` để lọc ra top các tỉnh thành có tốc độ phục hồi hoặc tăng trưởng du lịch mạnh nhất trong khoảng thời gian người dùng lựa chọn.
3. **Phân tích cơ cấu:** Kết hợp dữ liệu dự báo thể tích phòng với các đặc trưng hành vi khách hàng (`couple_ratio`, `solo_ratio`, v.v.) để đưa ra khuyến nghị phân khúc khách sạn tương lai.
