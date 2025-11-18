# TikTok Comments Silver Pipeline - REFACTORED

## 📋 OVERVIEW

**Status**: ✅ REFACTORED theo pattern hotels_reviews
**Date**: November 11, 2025
**Pattern**: 2-Step Transformation (Bronze → Scratch → Silver)

---

## 🏗️ ARCHITECTURE

### Pattern: 2-Step Transformation

```
Bronze CSV (special format)
  └─> Step 1: Transform (Bronze → Scratch)
        ├─> Parse 17-line header → Post metadata
        ├─> Parse CSV data (lines 18+) → Comments
        ├─> Validation (NOT NULL checks)
        └─> Write to 2 Scratch Parquet folders
              ├─> s3a://scratch/.../tiktok_post_metadata/run_YYYYMMDD_HHMMSS/
              └─> s3a://scratch/.../tiktok_post_comments/run_YYYYMMDD_HHMMSS/
  
  └─> Step 2: Clean & Load (Scratch → Silver)
        ├─> Clean & Transform
        │     ├─> Posts: Parse dates, convert metrics to INT
        │     └─> Comments: Parse mixed date formats, convert types
        ├─> Calculate row_checksum
        ├─> Deduplicate (LEFT ANTI JOIN)
        └─> APPEND to 2 Silver Iceberg tables
              ├─> silver.silver.tiktok_post_metadata
              └─> silver.silver.tiktok_post_comments
```

---

## 📁 FILE STRUCTURE

```
LakeHousePj/spark/jobs/silver/tiktok_comments/
├── config.py                 # Configuration (paths, columns, partitions)
├── step_01_transform.py      # Bronze → Scratch (parse special CSV format)
├── step_02_clean_load.py     # Scratch → Silver (clean, dedup, load)
└── __init__.py               # Package init
```

---

## 🗂️ SILVER TABLES

### Table 1: `silver.silver.tiktok_post_metadata`

**Purpose**: Store post metadata from 17-line header

**Schema**:
```
post_url                    STRING (PRIMARY KEY)
author                      STRING
author_tag                  STRING
author_url                  STRING
post_date                   DATE (✅ cleaned from DD-MM-YYYY)
post_description            STRING
likes                       INT (✅ cleaned from STRING)
comments_count              INT
saves                       INT
shares                      INT
comments_level1             INT
comments_level2             INT
comments_loaded             INT
comments_displayed_tiktok   INT
comments_difference         INT
crawl_time                  TIMESTAMP (✅ cleaned from String)
scrape_timestamp            STRING
row_checksum                STRING (MD5 of post_url)
ingestion_timestamp         TIMESTAMP
source_file                 STRING
source_file_checksum        STRING
source_file_size_bytes      LONG
```

**Partition**: NONE (small table, 1 post per file)

**Deduplication**: LEFT ANTI JOIN on `post_url` (primary key)

---

### Table 2: `silver.silver.tiktok_post_comments`

**Purpose**: Store comment data from CSV section (lines 18+)

**Schema**:
```
post_url                    STRING (FOREIGN KEY → posts)
stt                         INT (✅ cleaned from STRING)
ten                         STRING (commenter name, trimmed)
tag_ten                     STRING (commenter username, trimmed)
url                         STRING (commenter profile)
comment                     STRING (comment text, trimmed)
comment_date                DATE (✅ cleaned from mixed format)
likes                       INT (✅ cleaned from STRING)
level_comment               STRING ("Yes" = reply, "No" = root)
replied_to_tag_name         STRING (parent commenter tag, trimmed)
number_of_replies           INT (✅ cleaned from STRING)
scrape_timestamp            STRING
row_checksum                STRING (MD5 of post_url+stt+ten+comment+time)
ingestion_timestamp         TIMESTAMP
source_file                 STRING
source_file_checksum        STRING
source_file_size_bytes      LONG
```

**Partition**: BY `post_url` (semantic, co-located queries)

**Deduplication**: LEFT ANTI JOIN on `row_checksum` (5 business columns)

---

## 🔧 DATA CLEANING (Step 2)

### Posts Metadata Cleaning

1. **`post_date`**: String (DD-MM-YYYY) → DateType
   - Parse: "9-6-2025" → 2025-06-09
   - Handle NULL values

2. **`crawl_time`**: String → TimestampType
   - Parse: "Sat Sep 27 2025 00:52:04 GMT+0700" → 2025-09-27 00:52:04
   - Extract timestamp part, ignore timezone

3. **Metrics**: String → IntegerType
   - Convert: likes, comments_count, saves, shares
   - Convert: comments_level1, comments_level2, comments_loaded
   - Safe casting (NULL if invalid)

4. **`ingestion_timestamp`**: Update to current datetime

---

### Comments Data Cleaning

1. **`comment_date`**: Mixed format → DateType
   - **Absolute format**: "31-8-2025" → 2025-08-31
   - **Relative format**: 
     * "6 ngày trước" → scrape_date - 6 days
     * "1 tuần trước" → scrape_date - 7 days
     * "2 tháng trước" → scrape_date - 2 months
   - **Priority**: Use absolute if available, else calculate relative

2. **`stt`**: String → IntegerType
   - Comment sequence number (1, 2, 3, ...)

3. **`likes`, `number_of_replies`**: String → IntegerType
   - Safe casting (NULL if invalid)

4. **Text fields**: Trim whitespace
   - Fields: ten, tag_ten, comment, replied_to_tag_name

5. **`ingestion_timestamp`**: Update to current datetime

---

## 🔐 INGESTION STRATEGY

### Direct APPEND (NO Deduplication)

**Strategy**: Direct APPEND without deduplication checks

**Rationale**:
- ✅ Crawler tool **only scrapes NEW videos**
- ✅ Each video scraped **only once** (never re-scrapes same video)
- ✅ 1 video = 1 CSV file = unique post + unique comments
- ✅ **No duplicates possible** → No need for deduplication

**vs. hotels_reviews**:
- Hotels: Same hotel scraped multiple times → Need LEFT ANTI JOIN
- TikTok: Each video scraped once → Direct APPEND

**row_checksum Purpose**:
- Still calculated for **data lineage tracking**
- Used for **debugging and auditing**
- NOT used for deduplication (not needed)

**Performance Advantage**:
- ⚡ Faster ingestion (no join operations)
- ⚡ Lower memory usage (no existing table reads)
- ⚡ Simpler pipeline (fewer steps)

**Logic**:
```python
# Posts
df_posts_with_checksum.writeTo(SILVER_TABLE_POSTS) \
    .using("iceberg") \
    .append()

# Comments  
df_comments_with_checksum.repartition("post_url") \
    .writeTo(SILVER_TABLE_COMMENTS) \
    .using("iceberg") \
    .append()
```

---

## 📊 SPECIAL CSV FORMAT HANDLING

### File Structure

```
Line 1:  Thời gian cào: Sat Sep 27 2025 00:52:04 GMT+0700 (Indochina Time)
Line 2:  Post URL: https://www.tiktok.com/@khanhvuvn/video/7513852704230296840
Line 3:  Người đăng: Khánh Vũ🇻🇳
Line 4:  Tag người đăng: khanhvuvn
Line 5:  URL người đăng: https://www.tiktok.com/@khanhvuvn
Line 6:  Thời gian đăng: 9-6-2025
Line 7:  Số lượt tym: 4525
Line 8:  Số lượt comment: 2047
Line 9:  Số lượt lưu: 866
Line 10: Số lượt share: 4857
Line 11: Mô tả của bài đăng: "description text..."
Line 12: Số bình luận cấp 1: 857
Line 13: Số bình luận cấp 2: 625
Line 14: Tổng số bình luận thực tế đã load: 1482
Line 15: Số bình luận TikTok hiển thị: 2047
Line 16: Chênh lệch số bình luận: 565
Line 17: (empty or separator)
Line 18: STT,Tên,Tag tên,URL,Comment,Time,Likes,Level Comment,Replied To Tag Name,Number of Replies
Line 19+: CSV data rows...
```

### Parsing Strategy

**Step 1 (Transform)**:
1. Read file as text lines (Spark read.text)
2. Parse lines 1-17 → Extract metadata (key:value pairs)
3. Parse lines 18+ → CSV data (Python csv.DictReader)
   - Handles commas in comment text automatically
   - Uses proper CSV quoting rules
4. Add metadata to each record
5. Write to Scratch Parquet (2 separate folders)

**Challenge Solved**: 
- ✅ Commas in comment text (handled by csv.DictReader)
- ✅ Mixed date formats (handled in Step 2)
- ✅ Vietnamese text (UTF-8 encoding throughout)
- ✅ Nested replies (preserved with level_comment + replied_to_tag_name)

---

## 🚀 EXECUTION

### Step 1: Transform (Bronze → Scratch)

```bash
docker exec -it spark-master spark-submit \
  --master local[*] \
  --conf spark.executor.memory=1536m \
  --conf spark.executor.memoryOverhead=512m \
  /opt/spark/jobs/silver/tiktok_comments/step_01_transform.py
```

**Output**:
- `s3a://scratch/pipeline/silver/tiktok_post_metadata/run_YYYYMMDD_HHMMSS/`
- `s3a://scratch/pipeline/silver/tiktok_post_comments/run_YYYYMMDD_HHMMSS/`

**Features**:
- ✅ Batch processing (50 files per batch)
- ✅ PostgreSQL checksum tracking (skip already processed)
- ✅ File size tracking
- ✅ Validation (NOT NULL checks)
- ✅ Error handling per file

---

### Step 2: Clean & Load (Scratch → Silver)

```bash
docker exec -it spark-master spark-submit \
  --master local[*] \
  --conf spark.executor.memory=1536m \
  --conf spark.executor.memoryOverhead=512m \
  /opt/spark/jobs/silver/tiktok_comments/step_02_clean_load.py
```

**Output**:
- `silver.silver.tiktok_post_metadata` (Iceberg table)
- `silver.silver.tiktok_post_comments` (Iceberg table, partitioned by post_url)

**Features**:
- ✅ Data cleaning (date parsing, type conversion, text trimming)
- ✅ Deduplication (LEFT ANTI JOIN)
- ✅ APPEND mode (incremental loading)
- ✅ Partitioning by post_url (comments only)
- ✅ PostgreSQL logging with ingestion details

---

## 📈 PERFORMANCE OPTIMIZATIONS

### Batch Processing

**Configuration**: `BATCH_SIZE = 50` files per batch

**Rationale**:
- Avoid memory issues with 8000+ files
- Process in manageable chunks
- Reduce Spark driver memory pressure

**Implementation**:
```python
for batch_idx in range(0, unprocessed_count, BATCH_SIZE):
    batch_files = unprocessed[batch_idx:batch_idx + BATCH_SIZE]
    # Process batch...
```

---

### Partitioning Strategy

**Posts**: NO partition
- Small table (1 post per file)
- No query skew
- Simple flat structure

**Comments**: Partition by `post_url`
- Semantic partitioning
- Co-located queries (query by post)
- Balanced distribution (comments per post)

**Avoid**: Partition by date (too many small partitions)

---

### Deduplication Optimization

**Method**: LEFT ANTI JOIN (not IN/EXISTS)

**Advantage**:
- ✅ Spark-optimized operation
- ✅ Broadcast join if small (auto-optimization)
- ✅ Memory-efficient (only checksums/keys loaded)
- ✅ Preserves DataFrame lineage

**vs. IN clause**:
- ❌ IN collects full list to driver (memory issue)
- ❌ Not parallelized
- ❌ Scala Array conversion overhead

---

## 🔍 DATA VALIDATION

### Step 1 Validation (Transform)

**Posts NOT NULL columns**:
- `post_url`

**Comments NOT NULL columns**:
- `post_url`
- `comment`

**Action**: Filter out records with NULL critical values

---

### Step 2 Validation (Clean & Load)

**Date Parsing**:
- Track parse rate (% successfully parsed)
- Log NULL dates after parsing

**Type Conversion**:
- Safe casting (NULL if invalid)
- Preserve original value in logs

**Deduplication**:
- Report duplicate count
- Log to PostgreSQL

---

## 📝 POSTGRESQL TRACKING

### Ingestion Log Structure

```json
{
  "dedup_stats": {
    "posts_new": 150,
    "comments_new": 1481,
    "posts_duplicates": 0,
    "comments_duplicates": 12
  },
  "transformations": {
    "posts": {
      "post_date": "DD-MM-YYYY → DateType",
      "crawl_time": "String → TimestampType",
      "metrics": "String → IntegerType"
    },
    "comments": {
      "comment_date": "Mixed format (absolute/relative) → DateType",
      "stt_likes_replies": "String → IntegerType",
      "text_fields": "Trimmed whitespace"
    }
  }
}
```

**Checksum**: Based on scrape_timestamp from filename
- Format: `tiktok_comments_YYYY-MM-DDTHH-MM-SS.csv`
- Checksum: `YYYYMMDDHHMMSS` (timestamp without separators)

---

## ✅ IMPROVEMENTS vs OLD CODE

### Old Code (transform_tiktok_comments.py)

❌ Single monolithic script
❌ No Scratch layer (Bronze → Silver direct)
❌ No data cleaning (all columns as STRING)
❌ No date parsing (kept as strings)
❌ Mixed parsing + dedup + write in one step
❌ Had deduplication logic (not needed for this use case)
❌ Hard to debug and maintain

### New Code (2-Step Pattern)

✅ Separated concerns (Transform vs Clean)
✅ Scratch layer for intermediate data
✅ Full data cleaning (proper types: DATE, INT, TIMESTAMP)
✅ Date parsing (mixed format handling)
✅ **Direct APPEND (no unnecessary deduplication)**
✅ Validation at each step
✅ Better error handling per file
✅ Follows hotels_reviews pattern (consistent codebase)
✅ **Faster ingestion** (no join operations)

---

## 🎯 NEXT STEPS

1. **Test Step 1**: Transform 3 sample files to Scratch
   ```bash
   docker exec -it spark-master spark-submit \
     /opt/spark/jobs/silver/tiktok_comments/step_01_transform.py
   ```

2. **Verify Scratch Output**: Check Parquet files
   ```bash
   # Posts
   aws s3 ls s3://scratch/pipeline/silver/tiktok_post_metadata/ --recursive
   
   # Comments
   aws s3 ls s3://scratch/pipeline/silver/tiktok_post_comments/ --recursive
   ```

3. **Test Step 2**: Clean & Load to Silver
   ```bash
   docker exec -it spark-master spark-submit \
     /opt/spark/jobs/silver/tiktok_comments/step_02_clean_load.py
   ```

4. **Verify Silver Tables**: Query with DuckDB or Spark SQL
   ```sql
   -- Posts
   SELECT COUNT(*), COUNT(DISTINCT post_url) 
   FROM silver.tiktok_post_metadata;
   
   -- Comments
   SELECT COUNT(*), COUNT(DISTINCT post_url), COUNT(DISTINCT row_checksum)
   FROM silver.tiktok_post_comments;
   ```

5. **Production Run**: Process all 8000+ files
   - Step 1 will batch process (50 files at a time)
   - PostgreSQL tracking prevents re-processing
   - Monitor memory usage

---

## 🔗 RELATED FILES

- **Config**: `silver/tiktok_comments/config.py`
- **Step 1**: `silver/tiktok_comments/step_01_transform.py`
- **Step 2**: `silver/tiktok_comments/step_02_clean_load.py`
- **Utils**:
  - `utils/spark_session.py` (Spark session factory)
  - `utils/iceberg_utils.py` (Table creation)
  - `utils/merge_utils.py` (row_checksum calculation)
  - `utils/file_tracker.py` (PostgreSQL tracking)

---

## ✅ SUMMARY

**REFACTORED**: TikTok Comments pipeline theo pattern hotels_reviews

**KEY FEATURES**:
- ✅ 2-Step Pattern (Bronze → Scratch → Silver)
- ✅ Special CSV format handling (17-line header + CSV data)
- ✅ Data cleaning (dates, types, text trimming)
- ✅ Mixed date format parsing (absolute + relative)
- ✅ LEFT ANTI JOIN deduplication (optimized)
- ✅ Batch processing (50 files/batch)
- ✅ PostgreSQL checksum tracking
- ✅ 2 Silver tables (posts + comments)
- ✅ Partition by post_url (comments only)

**READY TO TEST**: Chạy Step 1 với 3 files mẫu để verify!

---

🎉 **REFACTORING COMPLETED!**
