# 🚀 Hướng Dẫn Triển Khai Foundation Layer

## Bước 1: Khởi động các services

```powershell
# Tại thư mục LakeHousePj
docker-compose up -d
```

**Chờ khoảng 2-3 phút** để các services khởi động đầy đủ.

---

## Bước 2: Kiểm tra trạng thái

```powershell
# Xem danh sách containers đang chạy
docker-compose ps

# Kết quả mong đợi:
# lakehouse_postgres          running (healthy)
# lakehouse_minio             running (healthy)
# lakehouse_hive_metastore    running (healthy)
# lakehouse_minio_init        exited (0)
```

---

## Bước 3: Chạy script test

```powershell
.\scripts\test-foundation.ps1
```

**Kết quả mong đợi:**
```
✅ PostgreSQL is healthy
✅ MinIO is healthy
✅ Hive Metastore is listening on port 9083
```

---

## Bước 4: Truy cập MinIO Console

1. Mở browser: http://localhost:9001
2. Đăng nhập:
   - **Username**: `minioadmin`
   - **Password**: `minioadmin123`

3. Kiểm tra 3 buckets đã được tạo:
   - ✅ `bronze` - Raw data layer
   - ✅ `silver` - Cleaned data layer
   - ✅ `gold` - Aggregated data layer

---

## Bước 5: Kiểm tra Hive Metastore kết nối PostgreSQL

```powershell
# Kiểm tra Hive logs
docker-compose logs hive-metastore | Select-String -Pattern "Connected to"

# Kiểm tra PostgreSQL có Hive metadata tables
docker exec -it lakehouse_postgres psql -U lakehouse_user -d metastore_db -c "\dt"
```

---

## 🎯 Checklist Hoàn Thành

- [ ] Docker containers đang chạy
- [ ] PostgreSQL healthy (port 5432)
- [ ] MinIO healthy (ports 9000, 9001)
- [ ] Hive Metastore listening (port 9083)
- [ ] 3 buckets (bronze, silver, gold) đã tạo
- [ ] Hive Metastore kết nối được PostgreSQL
- [ ] MinIO Console truy cập được

---

## 🐛 Troubleshooting

### Lỗi: Container không start

```powershell
# Xem logs để biết lỗi
docker-compose logs [service-name]

# Ví dụ:
docker-compose logs postgres
docker-compose logs minio
docker-compose logs hive-metastore
```

### Lỗi: Port đã được sử dụng

Sửa file `.env` để đổi port:
```
POSTGRES_PORT=5433  # thay vì 5432
MINIO_PORT=9001     # thay vì 9000
```

### Lỗi: Hive không kết nối được PostgreSQL

```powershell
# Restart services theo thứ tự
docker-compose restart postgres
# Đợi 10 giây
docker-compose restart hive-metastore
```

### Reset toàn bộ hệ thống

```powershell
# Dừng và xóa tất cả (bao gồm volumes)
docker-compose down -v

# Xóa folder data cũ
Remove-Item -Recurse -Force postgres/data, minio/data, hive/data -ErrorAction SilentlyContinue

# Khởi động lại
docker-compose up -d
```

---

## 📊 Kiểm tra từng service riêng lẻ

### PostgreSQL
```powershell
# Test connection
docker exec lakehouse_postgres pg_isready -U lakehouse_user -d metastore_db

# Connect to database
docker exec -it lakehouse_postgres psql -U lakehouse_user -d metastore_db

# Trong psql shell:
# \l          # List databases
# \dt         # List tables
# \q          # Quit
```

### MinIO
```powershell
# List buckets
docker exec lakehouse_minio mc ls myminio

# Or access web console
start http://localhost:9001
```

### Hive Metastore
```powershell
# Check if service is listening
docker exec lakehouse_hive_metastore nc -z localhost 9083

# View logs
docker logs lakehouse_hive_metastore --tail 50
```

---

## ⏭️ Bước tiếp theo

Sau khi Foundation Layer hoạt động ổn định:

1. ✅ **Thêm Apache Spark** - Processing engine
2. ✅ **Thêm Apache Iceberg** - Table format
3. ✅ **Thêm Apache Airflow** - Orchestration
4. ✅ **Thêm Dremio** - Query engine

---

## 📝 Ghi chú

- Dữ liệu được lưu trong các folder: `postgres/data/`, `minio/data/`, `hive/data/`
- Để backup, chỉ cần backup các folder này
- Để xóa dữ liệu: `docker-compose down -v`
- Logs được lưu tự động bởi Docker
