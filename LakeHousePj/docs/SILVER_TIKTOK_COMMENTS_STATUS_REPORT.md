# Silver TikTok Comments - Status Report

## Muc tieu tai lieu

Tai lieu nay tong hop nhanh toan bo boi canh hien tai cua pipeline `tiktok_comments` (Bronze -> Scratch -> Silver), cac van de chat luong du lieu da ghi nhan, rui ro den DL model, va ke hoach hanh dong de bao cao quan ly.

## 1) Hien trang kien truc va luong du lieu

- Bronze:
  - Ingest file raw TikTok comments theo batch tu `raw_ingest_tiktok_comments_batch.py`.
  - Dedup tai Bronze theo **checksum file content** (khong phai theo business key).
  - Ghi log vao `file_ingestion_log` voi `layer='bronze'`.
- Silver Step 1 (`step_01_transform.py`):
  - Parse file dac thu TikTok (metadata header + CSV comments).
  - Ghi ra Scratch Parquet cho:
    - `tiktok_post_metadata`
    - `tiktok_post_comments`
  - Co validate/filer mot so cot NOT NULL ngay tai Step 1.
- Silver Step 2 (`step_02_clean_load.py`):
  - Clean, parse type, chuan hoa date/metric.
  - Loai bo ban ghi khong hop le.
  - Append vao Iceberg Silver:
    - `silver.silver.tiktok_post_metadata`
    - `silver.silver.tiktok_post_comments`
  - Co tracking theo batch/file.

## 2) Cac diem da thong nhat (quan trong)

- Bronze dedup **chi loai file trung nhau 100% theo byte**.
- Van co the ton tai nhieu file cung `post_url` trong Bronze neu la nhieu lan crawl khac nhau (noi dung thay doi).
- Vi vay, Silver van bat buoc can dedup theo nghiep vu (business-level dedup), khong the chi dua vao Bronze dedup.

## 3) Van de chinh hien tai o Silver

### 3.1 Null va "silent data loss" trong qua trinh clean

- Nhieu parser trong Step 2 tra ve NULL khi khong match pattern (date, number) nhung chua co co che giai thich ly do theo tung dong.
- Data invalid dang bi filter bo truc tiep (drop) thay vi tach sang khu vuc quarantine, gay kho truy vet nguon goc.

### 3.2 Layer giao tiep Bronze -> Silver chua tan dung dung identity file

- Bronze da co checksum file dung.
- Nhung Step 1 Silver dang tao `source_file_checksum` theo scrape timestamp (khong phai checksum noi dung), dan den:
  - Mat lien ket lineage Bronze <-> Silver theo checksum that.
  - Rui ro skip/phan loai sai trong mot so tinh huong bien.

### 3.3 Chien luoc "giu file moi nhat theo post_url" co the lam mat du lieu

- Trong `partition_utils.py`, logic Option A chi giu file moi nhat cho moi `post_url`.
- Gia dinh "file moi hon la day du hon" khong luon dung (vi crawl co the thieu comment do rate limit/snapshot).
- Rui ro: bo qua ban crawl cu nhung day du hon.

### 3.4 Validation tai Step 1 va Step 2 chua dong bo

- Step 1 va Step 2 dang dung tieu chi loc du lieu khac nhau (`isNull` vs empty string + format checks).
- He qua:
  - Scratch co the con du lieu "rac" ma team nghi da sach.
  - Kho debug vi ket qua giua cac tang khong nhat quan.

### 3.5 Rui ro du lieu "partial success"

- Neu ghi posts thanh cong nhung comments that bai/bi skip logic, co kha nang de lai trang thai khong day du theo `post_url`.
- Neu co check skip theo post_url da ton tai, co the bo lo co hoi replay comments.

## 4) Tac dong truc tiep den mo hinh DL

- DL chuoi thoi gian rat nhay voi:
  - Null khong duoc giai thich.
  - Missing month/record do loc sai.
  - Outlier va parser fail lam meo phan phoi feature.
- He qua:
  - Giam so luong sequence hop le de train.
  - Tang risk leakage/instability khi retrain.
  - Khong tin cay khi so sanh model va danh gia nang cap.

## 5) Nguyen nhan goc (root causes)

- Chua co profile data (EDA co he thong) truoc khi dat rule clean.
- Chua co co che "Write-Audit-Publish" (staging + audit + publish) cho Silver.
- Chua co quarantine table de giu ban ghi fail parse/validation.
- Chua dong bo metadata identity giua Bronze va Silver.

## 6) Ke hoach hanh dong de xuat

### P0 - Data correctness (uu tien cao nhat)

1. Dong bo identity file:
   - Silver Step 1 phai dung checksum noi dung that (tu Bronze metadata), khong dung scrape timestamp gia lap.
2. Giam data loss:
   - Bo filter "drop som" o Step 1 (giu raw toi da o Scratch).
3. Xem lai logic chon ban crawl:
   - Khong mac dinh "latest-only" theo `post_url` neu chua xac minh quality.
4. Chot replay-safe:
   - Dam bao co the replay comments ngay ca khi post da ton tai.

### P1 - Observability va quality governance

1. Them parse status:
   - `*_parse_status` cho date/number/text quan trong.
2. Quarantine:
   - Tach ban ghi fail sang bang quarantine thay vi xoa.
3. Profile truoc clean:
   - Chay profile metrics (format distribution, null ratio, invalid sample) truoc khi ap rule.

### P2 - Hieu nang va maintainability

1. Don dep code chien luoc xu ly trung lap (tranh 2 implementation song song de maintain).
2. Toi uu partition/doc du lieu Scratch de tranh scan qua rong.
3. Chuan hoa schema explicit tai Step 1 de tranh schema drift theo batch.

## 7) Quy tac phan hoi phong van (chot quan diem)

- Cau tra loi dung khong phai "chi load roi clean" hoac "chi clean roi load".
- Kien truc dung:
  - Bronze: giu raw + dedup vat ly theo file.
  - Silver: clean theo nghiep vu + quality checks + quarantine.
  - Gold: business-ready features.
- Dieu kien de van hanh ben vung:
  - Co profile/EDA truoc rule clean.
  - Co audit trail de truy vet.
  - Co replay du lieu an toan khi fail.

## 8) Trang thai tong quan hien tai

- Diem manh:
  - Kien truc medallion da dung huong.
  - Bronze ingest va tracking da co nen tang tot.
- Diem can xu ly gap:
  - Data loss ngam o Silver do parse/filter va latest-only strategy.
  - Chua du observability de ly giai null va parser fail theo run.
- Muc tieu ngay:
  - Dong bo checksum lineage, bo filter som, bo sung quarantine + profile.

