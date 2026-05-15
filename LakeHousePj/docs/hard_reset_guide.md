# Hướng dẫn Hard Reset bảng dữ liệu (Manual)

Sử dụng các lệnh này khi bạn muốn xóa sạch một bảng (Metadata, Data, Logs) để nạp lại từ đầu. Thay thế `YOUR_TABLE_NAME` bằng tên bảng thực tế (ví dụ: `tiktok_post_metadata`).

---

### Bước 1: Tìm ID của bảng trong Hive Metastore
Lệnh này giúp bạn lấy được `TBL_ID`. Bạn cần ID này để xóa chính xác các tham số ở Bước 2.

```bash
echo 'SELECT "TBL_ID", "TBL_NAME" FROM "TBLS" WHERE "TBL_NAME" = '"'YOUR_TABLE_NAME'"';' | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db
```
*   **Giải thích**: Truy vấn bảng hệ thống `TBLS` để lấy ID định danh.

---

### Bước 2: Xóa Metadata trong Hive Metastore (Postgres)
Thay `ID_CUA_BAN` bằng con số bạn vừa tìm được ở Bước 1.

```bash
echo 'DELETE FROM "TABLE_PARAMS" WHERE "TBL_ID" = ID_CUA_BAN; DELETE FROM "PARTITION_KEYS" WHERE "TBL_ID" = ID_CUA_BAN; DELETE FROM "TBLS" WHERE "TBL_ID" = ID_CUA_BAN;' | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db
```
*   **Thứ tự xóa**:
    1.  `TABLE_PARAMS`: Xóa các thuộc tính cấu hình Iceberg/Hive.
    2.  `PARTITION_KEYS`: Xóa định nghĩa cột phân vùng.
    3.  `TBLS`: Xóa bản ghi chính của bảng (Unregister table).

---

### Bước 3: Xóa dữ liệu vật lý trong MinIO
Lệnh này xóa các file Parquet và Metadata thực tế trên ổ đĩa.

```bash
docker exec lakehouse_minio sh -lc 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc rm --recursive --force local/silver/lakehouse/silver.db/YOUR_TABLE_NAME/'
```
*   **Giải thích**: Tạo lại alias `local` với quyền admin của MinIO rồi mới xóa sạch thư mục của bảng trong bucket `silver`. Nếu không đăng nhập bằng admin, `mc rm` sẽ bị `Access Denied`.

---

### Bước 4: Xóa log nạp file (Ingestion Tracker)
Lệnh này cho phép Spark quét lại các file Bronze cũ để nạp lại.

```bash
echo "DELETE FROM file_ingestion_log WHERE table_name LIKE '%YOUR_TABLE_NAME%';" | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db
```
*   **Giải thích**: Xóa lịch sử vết cắn (checksum) của các file nguồn trong bảng tracking của dự án.

---

### Thứ tự thực hiện khuyến nghị:
1.  **Bước 1** (Lấy ID) -> **Bước 2** (Xóa Metadata) -> **Bước 3** (Xóa Data) -> **Bước 4** (Xóa Log).
2.  Sau khi xong 4 bước, bạn có thể chạy lại Spark Job/Airflow DAG một cách "sạch sẽ" như mới.
