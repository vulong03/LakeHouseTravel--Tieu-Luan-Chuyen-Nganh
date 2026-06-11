# Báo cáo Vấn đề: Huấn luyện Mô hình PhoBERT bị tràn bộ nhớ (OOM)

Tài liệu này giải thích chi tiết nguyên nhân dẫn đến tình trạng "Docker bị dead" (treo hoặc tắt đột ngột) khi chạy file `train_phobert.py` và đề xuất các phương án giải quyết để hoàn thiện 파ipeline Machine Learning.

---

## 1. Vấn đề đang gặp phải
Khi cố gắng thực thi lệnh huấn luyện mô hình bên trong container Spark:
```bash
docker exec -it lakehouse_spark_master /opt/spark/bin/spark-submit /opt/spark/jobs/dl/nlp/train_phobert.py
```
Tiến trình bị ngắt đột ngột (Exit Code: 1 hoặc Docker bị dead). 

Hệ điều hành ảo (WSL2/Linux) chứa Docker đã tự động kích hoạt cơ chế phòng vệ **OOM Killer (Out-Of-Memory Killer)** để hủy tiến trình này nhằm bảo vệ máy tính khỏi việc bị treo cứng toàn bộ.

## 2. Phân tích Nguyên nhân Cốt lõi
Sự cố tràn bộ nhớ RAM (OOM) đến từ 3 yếu tố kết hợp cộng dồn:

**A. Khối lượng dữ liệu (Data Size) quá lớn so với RAM:**
Đoạn code sử dụng `df.toPandas()` để nạp toàn bộ **661.842 dòng** văn bản vào RAM cùng lúc. Việc này tiêu tốn rất nhiều không gian biến nhớ của tiến trình Python.

**B. Kích thước Mô hình PhoBERT (Model Size):**
Bạn đang sử dụng `vinai/phobert-base-v2`, đây là một mô hình ngôn ngữ lớn dựa trên kiến trúc Transformer với **135 triệu tham số**. Việc tính toán ma trận với batch size = 32 trên mạng neural này đòi hỏi cấp phát lượng bộ nhớ khổng lồ.

**C. Huấn luyện "Thuần CPU" bên trong Docker (Nút thắt cổ chai lớn nhất):**
- Theo mặc định, các vùng chứa (container) Docker **KHÔNG NHẬN DIỆN VÀ KHÔNG SỬ DỤNG ĐƯỢC Card đồ họa (GPU)** của máy tính (Windows) trừ khi được cấu hình siêu chuyên biệt.
- Gói `PyTorch` hiện tại trong container Spark là phiên bản chạy hoàn toàn bằng CPU.
- Khi một mô hình Deep Learning lớn buộc phải tính toán forward/backward pass trên CPU thay vì GPU, nó sẽ "ăn vã" RAM hệ thống (System RAM) thay vì VRAM của Card đồ họa. Kết hợp với việc Docker trên WSL2 thường bị giới hạn lượng RAM cố định, tiến trình nhanh chóng chạm ngưỡng tối đa và bị hệ thống "giết chết".

---

## 3. Các hướng giải quyết (Solutions)

Tùy thuộc vào cấu hình phần cứng hiện tại của bạn, chúng ta có 4 giải pháp từ dễ đến khó:

### Giải pháp 1: Rút gọn dữ liệu (Chỉ để Demo/Kiểm thử luồng) - *Dễ nhất*
Nếu mục đích chính của bạn là chứng minh toàn bộ quy trình kiến trúc Lakehouse (Data Ingestion -> Silver -> Gold -> Train -> Log MLflow) hoạt động trơn tru từ đầu đến cuối mà không cần mô hình phải quá "thông minh".
* **Cách làm:** Sửa code `train_phobert.py` để giới hạn số lượng mẫu xuống còn **mỗi class 1,000 - 2,000 dòng** (tổng khoảng vài ngàn dòng). Giảm `batch_size` xuống 8 hoặc 16.
* **Ưu điểm:** Khắc phục ngay lập tức lỗi OOM, chạy thành công bằng CPU trong Docker, ghi nhận model vào MLflow hoàn chỉnh.
* **Nhược điểm:** Mô hình sinh ra sẽ có độ chính xác (Accuracy) thấp do không được học đủ dữ liệu.

### Giải pháp 2: Kiến trúc Lai (Hybrid Local) - *Khuyên dùng nếu có Card NVIDIA GPU (từ 6GB VRAM)*
Tách quá trình tính toán nặng nề (Training) ra khỏi Docker, đưa ra chạy trực tiếp trên Windows của bạn để tận dụng tối đa sức mạnh phần cứng thật.
* **Cách làm:** 
    1. Giữ MinIO và MLflow chạy trong Docker (`localhost:9000` và `localhost:5001`).
    2. Cài Python và thư viện `torch` phiên bản CUDA (hỗ trợ GPU) trực tiếp trên Windows.
    3. Chạy lệnh python file `train_phobert.py` ngay trên Terminal PowerShell ngoài Windows. Code sẽ tải data từ MinIO ảo, train bằng Card Màn Hình thật, và gửi log vào MLflow ảo.
* **Ưu điểm:** Dễ thiết lập, tận dụng sức mạnh GPU phần cứng của bạn, train thực tế toàn bộ data.
* **Nhược điểm:** Phải cài môi trường Python & Cuda ngoài Windows của bạn.

### Giải pháp 3: Sử dụng Điện toán Đám mây (Cloud GPU) - *Khuyên dùng nếu máy không có Card rời*
Đưa dữ liệu lên một môi trường có sẵn Card đồ họa mạnh (như Google Colab hoặc Kaggle) để huấn luyện, sau đó mang thành quả về lưu trữ nội bộ.
* **Cách làm:**
    1. Trích xuất file `.parquet` (660K dòng) tải ra máy tính.
    2. Đưa file và code lên Google Colab, bật Hardware Accelerator thành GPU T4.
    3. Huấn luyện xong, tải file cấu hình Model (`.pt` / `.bin`) về máy.
    4. Viết 1 script nhỏ đẩy Model đó vào hệ thống MLflow cục bộ trong Docker.
* **Ưu điểm:** Miễn phí GPU mạnh (T4 16GB VRAM), không lo cháy máy, chạy siêu nhanh. Train được Full data.
* **Nhược điểm:** Việc chuyển file qua lại giữa Local và Cloud tốn thời gian upload/download.

### Giải pháp 4: Ép Docker nhận Card đồ họa (Docker GPU Passthrough) - *Gian nan nhất*
Mang Card đồ họa Windows vào bên trong Container Spark.
* **Cách làm:** Cài đặt `NVIDIA Container Toolkit` lên bộ nhân WSL2. Cấu hình lại `docker-compose.yml` (Thêm khối `deploy.resources.reservations.devices.nvidia`). Viết lại Dockerfile cho tiến trình Spark để tải bộ Image PyTorch CUDA siêu nặng (5GB+).
* **Ưu điểm:** Mọi thứ vẫn "All-in-one" nằm gọn gàng bên trong Docker.
* **Nhược điểm:** Khó thiết lập do Windows/WSL2/Docker Desktop version hay bị xung đột driver.

---
**💡 Yêu cầu Quyết định:** 
Bạn hãy xem xét tài liệu này, kiểm tra thông số máy tính của bạn (Có GPU hãng NVIDIA không? RAM bao nhiêu?) và chọn ra 1 trong 4 giải pháp trên. Tôi sẽ hướng dẫn từng bước cụ thể để thực hiện.