# Hướng dẫn Nâng cấp & Tối ưu hóa cấu hình Spark (VM Google Cloud 32GB RAM / 100GB SSD)

Tài liệu này hướng dẫn cách cấu hình phân bổ tài nguyên cho Apache Spark trên máy ảo Google Cloud Platform (GCP) cấu hình **32GB RAM / 100GB SSD**, với mục tiêu dành riêng **8GB RAM** cho cụm Spark hoạt động ổn định và tối ưu, phần còn lại (24GB RAM) để chia sẻ cho các dịch vụ khác (Dremio, Airflow, PostgreSQL, Apache Superset, MinIO).

---

## 1. Các vấn đề gặp phải khi chạy Spark trên VM Cloud

Khi chạy cụm Lakehouse trên một máy ảo duy nhất có nhiều dịch vụ chạy song song với Spark, bạn sẽ đối mặt với các nguy cơ sau:

### a. Container Worker bị kill đột ngột (OOM Killer của OS/Docker)
*   **Nguyên nhân:** Spark chạy trên môi trường JVM (Java Virtual Machine). RAM của JVM tăng trưởng gồm 2 phần: **Heap memory** (bộ nhớ tính toán thực tế trong cấu hình) và **Off-heap memory/Overhead** (bộ nhớ hệ thống, network buffer, bộ đệm của OS).
*   **Hậu quả:** Nếu tổng RAM thực tế của container Worker vượt quá giới hạn RAM quy định trong `docker-compose.yml` (`limits.memory`), Docker daemon hoặc Linux kernel sẽ tắt ngay lập tức (kill) container Worker để cứu hệ thống, khiến Spark job bị lỗi thất bại (`Exit code 137`).

### b. Tranh chấp tài nguyên RAM với Dremio
*   **Nguyên nhân:** Dremio là một công cụ truy vấn cực kỳ ngốn RAM. Nếu không giới hạn tài nguyên của cả Spark và Dremio, cả hai sẽ cố gắng tranh giành bộ nhớ RAM của hệ thống cho đến khi máy ảo bị đơ (freeze) hoặc sập.

### c. Tràn đĩa cứng (100GB SSD) do Spark Shuffle files
*   **Nguyên nhân:** Khi Spark xử lý các tác vụ phức tạp như gom nhóm (`group by`), phân vùng lại (`repartition`), hoặc sắp xếp dữ liệu lớn, nó sẽ ghi các file dữ liệu trung gian xuống ổ đĩa cục bộ (gọi là **Shuffle Spill**). Với dung lượng SSD giới hạn là 100GB, nếu chạy các tác vụ ETL lớn, đĩa cứng rất dễ bị đầy 100% dẫn đến treo toàn bộ VM.

### d. Cấu hình mới không được áp dụng do bị ghi đè cục bộ
*   **Nguyên nhân:** Các nhà phát triển thường gán cứng cấu hình bộ nhớ trong code Python (`.config("spark.executor.memory", "2g")`) hoặc trong DAG Airflow. Những dòng code này sẽ vô hiệu hóa cấu hình hệ thống bạn thiết lập trong `spark-defaults.conf`.

---

## 2. Các chỉ số cấu hình cần quan tâm

Khi cấu hình Spark chạy tối đa **8GB RAM**, bạn cần cân đối mối quan hệ giữa các thông số sau:

### Cấp độ hạ tầng (Docker Compose):
*   **`SPARK_WORKER_MEMORY`**: Tổng RAM tối đa mà Spark Master cho phép phân bổ cho các Executor chạy trên Worker này. Đặt là **`8g`**.
*   **Docker Memory Limits (`limits.memory`)**: Giới hạn RAM của Container. Phải luôn lớn hơn `SPARK_WORKER_MEMORY` từ 1GB - 1.5GB để chừa chỗ cho các tiến trình phụ của Java và hệ điều hành của container. Đặt là **`9.5g`**.

### Cấp độ ứng dụng (Spark Defaults):
*   **`spark.executor.instances`**: Số lượng Executor hoạt động. Khuyên dùng **`1`** (để gom toàn bộ 8GB RAM vào 1 tiến trình xử lý lớn, hạn chế overhead của JVM và chạy được các mô hình ML/DL).
*   **`spark.executor.memory`**: RAM cấp cho JVM Heap của Executor. Với quỹ 8GB của Worker, nên đặt khoảng **`5g`** đến **`6g`**.
*   **`spark.executor.memoryOverhead`**: RAM dự phòng ngoài JVM. Nên để khoảng **`1g`** đến **`1.5g`** (khoảng 15% - 20% dung lượng RAM Executor để đảm bảo an toàn tối đa cho môi trường Cloud).
*   **`spark.driver.memory`**: RAM dành cho tiến trình điều khiển (Driver). Nên đặt từ **`2g`** đến **`3g`**.
*   **`spark.local.dir`**: Thư mục lưu file tạm thời khi shuffle. Mặc định là `/tmp`. Cần đảm bảo thư mục này nằm trên phân vùng SSD có đủ dung lượng trống.

---

## 3. Các bước cần làm để chuyển đổi cấu hình Spark

Để tiến hành chuyển đổi và áp dụng cấu hình mới trên VM, bạn hãy thực hiện theo 4 bước sau:

### Bước 1: Cập nhật file [docker-compose.yml](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/docker-compose.yml)
Tìm dịch vụ `spark-worker-1` và sửa lại phần tài nguyên như sau:

```yaml
  spark-worker-1:
    # ... giữ nguyên phần build, image, ports ...
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_CORES: 4        # Tận dụng 4 cores CPU cho Spark
      SPARK_WORKER_MEMORY: 8g      # Giới hạn Worker chỉ cấp tối đa 8GB RAM cho các Executor
      SPARK_WORKER_WEBUI_PORT: 8081
    deploy:
      resources:
        limits:
          memory: 9.5g             # Giới hạn cứng của container (tránh bị OOM do overhead)
        reservations:
          memory: 3g               # Đảm bảo giữ tối thiểu 3GB khi khởi động
```

### Bước 2: Cập nhật file [spark-defaults.conf](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/spark/spark-defaults.conf)
Sửa đổi các dòng cấu hình tài nguyên ở phần đầu file thành:

```properties
# ============================================
# Spark Application Configuration - CLUSTER MODE
# ============================================
spark.master                        spark://spark-master:7077
spark.submit.deployMode             client
spark.driver.memory                 2g       # RAM cho Driver điều phối
spark.executor.instances            1        # Chạy 1 Executor lớn để tối ưu hóa RAM
spark.executor.cores                4        # Executor sử dụng tối đa 4 luồng tính toán
spark.executor.memory               5g       # RAM tính toán thực tế cho Executor (5GB)
spark.executor.memoryOverhead       1536m    # RAM hệ thống ngoài Heap cho Executor (1.5GB)
spark.driver.maxResultSize          2g       # Giới hạn kết quả trả về Driver
spark.sql.shuffle.partitions        20       # Chia nhỏ partition để giảm dung lượng file ghi đĩa tạm
```
> [!NOTE]  
> Tổng dung lượng RAM của Executor khi chạy sẽ là: $5\text{GB (Memory)} + 1.5\text{GB (Overhead)} = 6.5\text{GB}$. Mức này nằm cực kỳ an toàn trong giới hạn $8\text{GB}$ của `SPARK_WORKER_MEMORY`.

### Bước 3: Dọn dẹp các cấu hình ghi đè cứng trong code Python và Airflow
Để Spark defaults có hiệu lực, bạn cần:
1.  **Trong code Python của các ML/DL Job:** Tìm và xóa dòng `.config("spark.executor.memory", "2g")` tại các file như [inference_phobert.py](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/spark/jobs/dl/nlp/inference_phobert.py) và [weak_labeling.py](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/spark/jobs/dl/nlp/weak_labeling.py).
2.  **Trong Airflow DAG Command helpers:** Tại [spark_operators.py](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/airflow/dags/common/spark_operators.py), xóa bớt các đoạn tham số gán cứng `--conf spark.executor.memory=...` trong các hàm định nghĩa lệnh submit (như hàm `silver_job` và `bronze_raw_job`).

### Bước 4: Khởi động lại cụm dịch vụ và giám sát
Sau khi chỉnh sửa xong các cấu hình trên máy ảo:
1.  Chạy lệnh để build lại cấu hình container:
    ```bash
    docker compose down
    docker compose up -d spark-master spark-worker-1
    ```
2.  Truy cập Spark Web UI tại cổng `8080` (Master Web UI) và `8081` (Worker Web UI) trên VM của bạn để kiểm tra xem chỉ số RAM hiển thị của Worker đã nhận đủ **8 GB** và Executor đã nhận đúng cấu hình hay chưa.
3.  Theo dõi dung lượng đĩa trống bằng lệnh `df -h` trong suốt quá trình chạy thử nghiệm các DAG nặng để đảm bảo không bị tràn ổ cứng 100GB SSD.
