# Dremio Setup - Kết nối MinIO

## Cấu hình S3 Source với Compatibility Mode

### Bước 1: Truy cập Dremio UI
```
http://localhost:9047
```

### Bước 2: Add S3 Source
1. Settings → Sources → Add Source → **Amazon S3**
2. Name: `minio_s3`

### Bước 3: General Configuration
- **Access Key:** `minioadmin`
- **Secret Key:** `minioadmin123`
- **✅ Enable Compatibility Mode** (QUAN TRỌNG - tắt AWS STS)

### Bước 4: Advanced Options
```
fs.s3a.endpoint = http://minio:9000
fs.s3a.connection.ssl.enabled = false
fs.s3a.path.style.access = true
```

### Bước 5: Encryption
- Chọn: **None**

### Bước 6: Save

---

## Hoặc dùng Hive Metastore (Khuyến nghị)

### Add Hive Source
1. Settings → Sources → Add Source → **Hive**
2. Name: `hive_metastore`
3. Configuration:
   - Metastore Host: `hive-metastore`
   - Metastore Port: `9083`
4. Save

Sau đó truy cập:
- `hive_metastore.bronze`
- `hive_metastore.silver` (có `tiktok_post_comments`)
- `hive_metastore.gold` (có `dim_post`, `dim_comment`)

---

## Lưu ý

### Tại sao phải bật Compatibility Mode?
- **AWS Mode (mặc định):** Dremio gọi AWS STS để verify credentials → fail với MinIO
- **Compatibility Mode:** Dremio bỏ qua STS, kết nối trực tiếp endpoint custom

### Tại sao endpoint không có `https://`?
- MinIO không dùng SSL
- Phải set `fs.s3a.connection.ssl.enabled = false`

### Tại sao phải bật Path-Style Access?
- MinIO sử dụng path-style URLs: `http://minio:9000/bucket/key`
- AWS S3 mặc định dùng virtual-hosted style: `http://bucket.s3.amazonaws.com/key`

