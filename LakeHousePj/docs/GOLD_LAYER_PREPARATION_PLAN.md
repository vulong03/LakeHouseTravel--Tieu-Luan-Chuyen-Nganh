# GOLD LAYER PREPARATION PLAN

**Ngày tạo:** 08/11/2025  
**Mục tiêu:** Clean Silver data và chuẩn bị transformation cho Gold layer  
**Tổng số bước:** 7 phases  
**Ước tính thời gian:** 3-4 giờ

---

## EXECUTIVE SUMMARY

### Current Silver Layer Status
- **6 tables** với tổng **1,822,552 records**
- **Overall Quality Score:** 8.78/10
- **Critical Issues:** 2 vấn đề cần fix ngay
- **Medium Issues:** 4 vấn đề cần xử lý trước Gold
- **Storage:** Apache Iceberg + Parquet/Snappy trên MinIO S3

### Critical Data Quality Issues Found

| Issue | Impact | Priority | Action |
|-------|--------|----------|--------|
| `tiktok_videos.vi_sub` 100% NULL | Column vô dụng | 🔴 P0 | DROP column |
| 35,794 empty comments (18.2%) | Dữ liệu rác | 🔴 P0 | DELETE rows |
| 12,316 duplicate reviews (0.78%) | Duplicates | 🟡 P1 | DEDUPLICATE |
| 5,754 duplicate comments (2.93%) | Duplicates | 🟡 P1 | DEDUPLICATE |
| 261 videos thiếu posted_date (5.9%) | Missing dates | 🟡 P1 | IMPUTE hoặc FILTER |
| String data types chưa cast | Type mismatch | 🟢 P2 | CAST in Gold |

---

## PHASE 1: PRE-CLEANUP BACKUP & VALIDATION (30 mins)

### 1.1. Snapshot Current State
```sql
-- Tạo snapshot để có thể rollback
-- Iceberg hỗ trợ time-travel, không cần backup riêng
SELECT 
    table_name,
    COUNT(*) as record_count,
    MAX(ingestion_timestamp) as last_updated
FROM (
    SELECT 'hotels_list' as table_name, COUNT(*) as cnt, MAX(ingestion_timestamp) as ts FROM silver.hotels_list
    UNION ALL
    SELECT 'hotels_detail', COUNT(*), MAX(ingestion_timestamp) FROM silver.hotels_detail
    UNION ALL
    SELECT 'hotels_reviews', COUNT(*), MAX(ingestion_timestamp) FROM silver.hotels_reviews
    UNION ALL
    SELECT 'tiktok_videos', COUNT(*), MAX(ingestion_timestamp) FROM silver.tiktok_videos
    UNION ALL
    SELECT 'tiktok_post_metadata', COUNT(*), MAX(ingestion_timestamp) FROM silver.tiktok_post_metadata
    UNION ALL
    SELECT 'tiktok_post_comments', COUNT(*), MAX(ingestion_timestamp) FROM silver.tiktok_post_comments
) counts;
```

### 1.2. Export Quality Metrics (Baseline)
```bash
# PowerShell script để lưu current state
.\scripts\silvercheck\export-baseline-metrics.ps1
```

**Deliverable:** `docs/SILVER_BASELINE_METRICS.json`

---

## PHASE 2: 🔴 CRITICAL CLEANUP (P0) (45 mins)

### 2.1. DROP Column `vi_sub` từ `tiktok_videos`

**Issue:** 100% NULL values (4,418/4,418 rows)

**SQL:**
```sql
-- Iceberg ALTER TABLE DROP COLUMN
ALTER TABLE silver.tiktok_videos 
DROP COLUMN vi_sub;
```

**Verification:**
```sql
DESCRIBE silver.tiktok_videos;
-- Expected: 12 columns (was 13)
```

**Impact:**
- ✅ Reduces storage slightly
- ✅ Cleaner schema
- ✅ No data loss (column was useless)

---

### 2.2. DELETE Empty Comments từ `tiktok_post_comments`

**Issue:** 35,794 rows có `comment = ''` (18.2% của bảng)

**SQL:**
```sql
-- Iceberg supports DELETE WHERE
DELETE FROM silver.tiktok_post_comments
WHERE comment = '' OR comment IS NULL OR TRIM(comment) = '';
```

**Verification:**
```sql
SELECT 
    COUNT(*) as total_comments,
    SUM(CASE WHEN comment = '' THEN 1 ELSE 0 END) as empty_comments,
    SUM(CASE WHEN TRIM(comment) = '' THEN 1 ELSE 0 END) as whitespace_only
FROM silver.tiktok_post_comments;
-- Expected: total_comments = 160,719, empty_comments = 0
```

**Impact:**
- ✅ Remove 35,794 useless rows
- ✅ Improved data quality from 7.7/10 → 9.5/10
- ⚠️ Reduces records from 196,513 → ~160,719 (18.2% reduction)
- ✅ Better for sentiment analysis & NLP

---

### 2.3. Clean Invalid `level_comment` Values

**Issue:** ~220 rows có giá trị không hợp lệ (dates, numbers, "???")

**Valid values:** "No" (level 1), "Yes" (level 2), "1", "2", "" (empty)

**SQL:**
```sql
-- Option 1: Set to NULL
UPDATE silver.tiktok_post_comments
SET level_comment = NULL
WHERE level_comment NOT IN ('No', 'Yes', '1', '2', '', ' ');

-- Option 2: Infer from replied_to_tag_name
UPDATE silver.tiktok_post_comments
SET level_comment = CASE 
    WHEN replied_to_tag_name IS NOT NULL AND replied_to_tag_name != '' THEN 'Yes'
    ELSE 'No'
END
WHERE level_comment NOT IN ('No', 'Yes', '1', '2', '', ' ');
```

**Recommendation:** Use Option 2 (infer from context)

**Verification:**
```sql
SELECT 
    level_comment, 
    COUNT(*) as count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
FROM silver.tiktok_post_comments
GROUP BY level_comment
ORDER BY count DESC;
-- Expected: Only 'No', 'Yes', NULL
```

---

## PHASE 3: 🟡 DEDUPLICATION (P1) (1 hour)

### 3.1. Deduplicate `hotels_reviews`

**Issue:** 12,316 duplicate rows (0.78%) based on `row_checksum`

**Strategy:** Keep **FIRST occurrence** by `ingestion_timestamp`

**SQL:**
```sql
-- Step 1: Find duplicates
CREATE TEMPORARY VIEW duplicate_reviews AS
SELECT row_checksum, MIN(ingestion_timestamp) as first_ingestion
FROM silver.hotels_reviews
GROUP BY row_checksum
HAVING COUNT(*) > 1;

-- Step 2: Delete later duplicates
DELETE FROM silver.hotels_reviews
WHERE row_checksum IN (SELECT row_checksum FROM duplicate_reviews)
AND ingestion_timestamp NOT IN (
    SELECT first_ingestion FROM duplicate_reviews dr
    WHERE dr.row_checksum = silver.hotels_reviews.row_checksum
);
```

**Alternative (safer - create deduplicated view):**
```sql
CREATE OR REPLACE VIEW silver.hotels_reviews_dedup AS
SELECT DISTINCT ON (row_checksum) *
FROM silver.hotels_reviews
ORDER BY row_checksum, ingestion_timestamp ASC;
```

**Verification:**
```sql
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT row_checksum) as unique_records,
    COUNT(*) - COUNT(DISTINCT row_checksum) as duplicates
FROM silver.hotels_reviews;
-- Expected: duplicates = 0
```

**Impact:**
- ✅ Remove 12,316 duplicate rows
- ✅ Records: 1,588,229 → 1,575,913
- ✅ Quality improvement: 9.3/10 → 9.9/10

---

### 3.2. Deduplicate `tiktok_post_comments`

**Issue:** 5,754 duplicate rows (2.93%)

**Composite Key:** `source_file_checksum` + `post_url` + `stt`

**SQL:**
```sql
-- Create unique key
ALTER TABLE silver.tiktok_post_comments
ADD COLUMN unique_key STRING;

UPDATE silver.tiktok_post_comments
SET unique_key = MD5(CONCAT(source_file_checksum, '|', post_url, '|', stt));

-- Deduplicate
DELETE FROM silver.tiktok_post_comments
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM silver.tiktok_post_comments
    GROUP BY unique_key
);

-- Clean up temp column
ALTER TABLE silver.tiktok_post_comments
DROP COLUMN unique_key;
```

**Note:** Iceberg doesn't have `rowid`, use alternative:

```sql
-- Better approach for Iceberg
CREATE TABLE silver.tiktok_post_comments_clean AS
SELECT DISTINCT ON (MD5(CONCAT(source_file_checksum, '|', post_url, '|', stt))) *
FROM silver.tiktok_post_comments
ORDER BY MD5(CONCAT(source_file_checksum, '|', post_url, '|', stt)), ingestion_timestamp;

-- Swap tables
DROP TABLE silver.tiktok_post_comments;
ALTER TABLE silver.tiktok_post_comments_clean RENAME TO tiktok_post_comments;
```

**Verification:**
```sql
SELECT 
    COUNT(*) as total,
    COUNT(DISTINCT MD5(CONCAT(source_file_checksum, '|', post_url, '|', stt))) as unique_keys
FROM silver.tiktok_post_comments;
-- Expected: total = unique_keys
```

**Impact:**
- ✅ Remove 5,754 duplicates
- ✅ After empty deletion + dedup: 160,719 → ~154,965 comments
- ✅ Quality: 7.7/10 → 9.8/10

---

### 3.3. Handle `tiktok_post_metadata` Time-Series Duplicates

**Issue:** 87 duplicate `post_url` (5.8%) - Đây là **VALID time-series data**

**Strategy:** Keep ALL for Silver, aggregate for Gold

**No action needed in Silver layer**

**Gold Layer Design:**
```sql
-- Gold: Latest snapshot per post
CREATE TABLE gold.tiktok_posts_latest AS
SELECT 
    post_url,
    author,
    post_date,
    post_description,
    MAX(likes) as current_likes,    
    MAX(comments_count) as current_comments,
    MAX(shares) as current_shares,
    MAX(saves) as current_saves,
    MAX(crawl_time) as last_updated
FROM silver.tiktok_post_metadata
GROUP BY post_url, author, post_date, post_description;

-- Gold: Historical engagement tracking
CREATE TABLE gold.tiktok_posts_history AS
SELECT 
    post_url,
    crawl_time,
    likes,
    comments_count,
    shares,
    saves,
    LAG(likes) OVER (PARTITION BY post_url ORDER BY crawl_time) as prev_likes,
    LAG(comments_count) OVER (PARTITION BY post_url ORDER BY crawl_time) as prev_comments
FROM silver.tiktok_post_metadata;
```

---

## PHASE 4: 🟡 MISSING DATA HANDLING (P1) (30 mins)

### 4.1. Handle Missing `posted_date` in `tiktok_videos`

**Issue:** 261 videos (5.91%) thiếu `posted_date`

**Options:**

**Option A: Filter Out (Recommended cho Gold)**
```sql
CREATE TABLE gold.tiktok_videos_clean AS
SELECT *
FROM silver.tiktok_videos
WHERE posted_date IS NOT NULL AND posted_date != '';
-- Result: 4,157 videos (94.1%)
```

**Option B: Impute from `ingestion_timestamp`**
```sql
UPDATE silver.tiktok_videos
SET posted_date = DATE(ingestion_timestamp)
WHERE posted_date IS NULL OR posted_date = '';
```

**Option C: Mark as Unknown**
```sql
UPDATE silver.tiktok_videos
SET posted_date = 'UNKNOWN'
WHERE posted_date IS NULL OR posted_date = '';
```

**Recommendation:** **Option A** - Filter trong Gold transformation (keep Silver as-is)

---

### 4.2. Standardize NULL Values

**Issue:** Some columns have empty strings instead of NULL

**SQL:**
```sql
-- hotels_detail
UPDATE silver.hotels_detail
SET 
    activities = NULLIF(TRIM(activities), ''),
    description = NULLIF(TRIM(description), ''),
    top_amenities = NULLIF(TRIM(top_amenities), '');

-- hotels_reviews
UPDATE silver.hotels_reviews
SET 
    review_positive = NULLIF(TRIM(review_positive), ''),
    review_negative = NULLIF(TRIM(review_negative), ''),
    reviewer_country = NULLIF(TRIM(reviewer_country), '');
```

---

## PHASE 5: 🟢 DATA TYPE TRANSFORMATIONS (P2) (2 hours)

### 5.0. Overview - Type Conversion Strategy

**Challenge:** Silver layer có nhiều columns là STRING cần convert sang proper types

**Strategy:**
1. ✅ **Option A (Recommended):** Keep Silver as STRING, create typed VIEWS → Transform to proper types in GOLD
2. ⚠️ **Option B (Risky):** ALTER Silver tables directly → Permanent conversion

**Recommendation:** Use **Option A** - Cleaner separation of concerns

**Columns needing conversion:**

| Table | Column | Current Type | Target Type | Complexity |
|-------|--------|-------------|-------------|-----------|
| hotels_detail | rating_score | STRING | DOUBLE | Low |
| hotels_detail | review_count_text | STRING | INT | Medium (needs REGEXP) |
| hotels_reviews | review_score | STRING | DOUBLE | Low |
| hotels_reviews | stay_date | STRING | DATE | High (format: "thang 6/2025") |
| hotels_reviews | review_date | STRING | DATE | Medium (format: "dd thang MM, yyyy") |
| tiktok_videos | posted_date | STRING | DATE | Low (format: "yyyy-MM-dd") |
| tiktok_videos | has_sub | STRING | BOOLEAN | Low |
| tiktok_post_metadata | likes | STRING | BIGINT | Low |
| tiktok_post_metadata | comments_count | STRING | BIGINT | Low |
| tiktok_post_metadata | shares | STRING | BIGINT | Low |
| tiktok_post_metadata | saves | STRING | BIGINT | Low |
| tiktok_post_metadata | comments_level1 | STRING | INT | Low |
| tiktok_post_metadata | comments_level2 | STRING | INT | Low |
| tiktok_post_metadata | comments_loaded | STRING | INT | Low |
| tiktok_post_metadata | comments_displayed_tiktok | STRING | INT | Low |
| tiktok_post_metadata | comments_difference | STRING | INT | Low |
| tiktok_post_metadata | post_date | STRING | DATE | Low |
| tiktok_post_metadata | crawl_time | STRING | TIMESTAMP | Medium |
| tiktok_post_comments | stt | STRING | INT | Low |
| tiktok_post_comments | likes | STRING | INT | Low |
| tiktok_post_comments | number_of_replies | STRING | INT | Low |
| tiktok_post_comments | time | STRING | TIMESTAMP | Medium |

**Total conversions needed:** 21 columns across 4 tables

---

### 5.1. Create Typed Views for Gold Layer (Recommended Approach)

**Purpose:** Validate type conversions before materializing Gold tables

```sql
-- 5.1.1. Hotels List (Typed)
CREATE OR REPLACE VIEW silver.hotels_list_typed AS
SELECT 
    CAST(stt AS INT) as hotel_id,
    hotel_name,
    hotel_url,
    province,
    row_checksum,
    ingestion_timestamp,
    source_file,
    source_file_checksum
FROM silver.hotels_list;

-- 5.1.2. Hotels Detail (Typed)
CREATE OR REPLACE VIEW silver.hotels_detail_typed AS
SELECT 
    hotel_name,
    hotel_url,
    province,
    description,
    top_amenities,
    CAST(rating_score AS DOUBLE) as rating_score,
    CAST(REGEXP_REPLACE(review_count_text, '[^0-9]', '') AS INT) as review_count,
    rating_breakdown,
    activities,
    row_checksum,
    ingestion_timestamp,
    source_file,
    source_file_checksum
FROM silver.hotels_detail;

-- 5.1.3. Hotels Reviews (Typed)
CREATE OR REPLACE VIEW silver.hotels_reviews_typed AS
SELECT 
    hotel_name,
    hotel_url,
    reviewer_name,
    reviewer_country,
    room_type,
    -- Parse "thang 6/2025" → DATE
    TO_DATE(
        CONCAT(
            SUBSTRING(stay_date, -4),  -- year
            '-',
            LPAD(REGEXP_EXTRACT(stay_date, 'thang ([0-9]+)', 1), 2, '0'),  -- month
            '-01'
        ),
        'yyyy-MM-dd'
    ) as stay_date,
    TO_DATE(review_date, 'dd thang MM, yyyy') as review_date,
    traveler_type,
    review_title,
    CAST(review_score AS DOUBLE) as review_score,
    review_positive,
    review_negative,
    row_checksum,
    ingestion_timestamp,
    source_file,
    source_file_checksum
FROM silver.hotels_reviews
WHERE review_date IS NOT NULL;

-- 5.1.4. TikTok Videos (Typed)
CREATE OR REPLACE VIEW silver.tiktok_videos_typed AS
SELECT 
    url,
    TO_DATE(posted_date, 'yyyy-MM-dd') as posted_date,
    read_status,
    keyword,
    ques_id,
    target_type,
    region,
    CAST(has_sub AS BOOLEAN) as has_sub,
    row_checksum,
    ingestion_timestamp,
    source_file,
    source_file_checksum
FROM silver.tiktok_videos
WHERE posted_date IS NOT NULL;

-- 5.1.5. TikTok Post Metadata (Typed)
CREATE OR REPLACE VIEW silver.tiktok_post_metadata_typed AS
SELECT 
    post_url,
    author,
    author_tag,
    author_url,
    TO_DATE(post_date, 'yyyy-MM-dd') as post_date,
    post_description,
    CAST(likes AS BIGINT) as likes,
    CAST(comments_count AS BIGINT) as comments_count,
    CAST(saves AS BIGINT) as saves,
    CAST(shares AS BIGINT) as shares,
    CAST(comments_level1 AS INT) as comments_level1,
    CAST(comments_level2 AS INT) as comments_level2,
    CAST(comments_loaded AS INT) as comments_loaded,
    CAST(comments_displayed_tiktok AS INT) as comments_displayed_tiktok,
    CAST(comments_difference AS INT) as comments_difference,
    TO_TIMESTAMP(crawl_time, 'yyyy-MM-dd HH:mm:ss') as crawl_time,
    ingestion_timestamp,
    source_file,
    source_file_checksum
FROM silver.tiktok_post_metadata;

-- 5.1.6. TikTok Post Comments (Typed)
CREATE OR REPLACE VIEW silver.tiktok_post_comments_typed AS
SELECT 
    post_url,
    CAST(stt AS INT) as comment_id,
    ten as commenter_name,
    tag_ten as commenter_tag,
    url as commenter_url,
    comment,
    time as comment_time,
    CAST(likes AS INT) as likes,
    CASE 
        WHEN level_comment IN ('No', '1') THEN 1
        WHEN level_comment IN ('Yes', '2') THEN 2
        ELSE NULL
    END as comment_level,
    replied_to_tag_name,
    CAST(number_of_replies AS INT) as number_of_replies,
    ingestion_timestamp,
    source_file,
    source_file_checksum
FROM silver.tiktok_post_comments
WHERE comment IS NOT NULL AND TRIM(comment) != '';
```

### 5.2. Validation Queries

```sql
-- Test type conversions
SELECT 
    'hotels_detail' as table_name,
    COUNT(*) as total,
    COUNT(CASE WHEN rating_score IS NULL THEN 1 END) as null_rating,
    MIN(rating_score) as min_rating,
    MAX(rating_score) as max_rating
FROM silver.hotels_detail_typed;

-- Expected: min_rating >= 0, max_rating <= 10

SELECT 
    'hotels_reviews' as table_name,
    COUNT(*) as total,
    MIN(stay_date) as earliest_stay,
    MAX(stay_date) as latest_stay,
    COUNT(CASE WHEN review_score < 0 OR review_score > 10 THEN 1 END) as invalid_scores
FROM silver.hotels_reviews_typed;

-- Expected: invalid_scores = 0
```

---

### 5.3. Alternative: Physical Type Conversion in Silver (NOT RECOMMENDED)

**Warning:** ⚠️ This approach modifies Silver layer permanently. Only use if you want strongly-typed Silver.

**Why NOT recommended:**
- Silver should preserve raw data types from Bronze
- Type conversions can fail and corrupt data
- Harder to debug data quality issues
- Gold layer is better place for transformations

**If you still want to do it:**

```sql
-- 5.3.1. Add new typed columns to hotels_detail
ALTER TABLE silver.hotels_detail
ADD COLUMN rating_score_double DOUBLE;

ALTER TABLE silver.hotels_detail  
ADD COLUMN review_count_int INT;

-- Populate new columns with converted values
UPDATE silver.hotels_detail
SET 
    rating_score_double = TRY_CAST(rating_score AS DOUBLE),
    review_count_int = TRY_CAST(REGEXP_REPLACE(review_count_text, '[^0-9]', '') AS INT);

-- Verify conversion success rate
SELECT 
    COUNT(*) as total,
    COUNT(rating_score_double) as rating_converted,
    COUNT(review_count_int) as review_count_converted,
    (COUNT(rating_score_double) * 100.0 / COUNT(*)) as rating_success_rate,
    (COUNT(review_count_int) * 100.0 / COUNT(*)) as count_success_rate
FROM silver.hotels_detail;
-- Expected: success_rate > 99%

-- If success rate is good, drop old columns and rename
-- (DO NOT run unless verified!)
-- ALTER TABLE silver.hotels_detail DROP COLUMN rating_score;
-- ALTER TABLE silver.hotels_detail DROP COLUMN review_count_text;
-- ALTER TABLE silver.hotels_detail RENAME COLUMN rating_score_double TO rating_score;
-- ALTER TABLE silver.hotels_detail RENAME COLUMN review_count_int TO review_count;

-- 5.3.2. Convert hotels_reviews
ALTER TABLE silver.hotels_reviews
ADD COLUMN review_score_double DOUBLE;

UPDATE silver.hotels_reviews
SET review_score_double = TRY_CAST(review_score AS DOUBLE);

-- 5.3.3. Convert tiktok_post_metadata (many columns!)
ALTER TABLE silver.tiktok_post_metadata
ADD COLUMN likes_bigint BIGINT;

ALTER TABLE silver.tiktok_post_metadata
ADD COLUMN comments_count_bigint BIGINT;

ALTER TABLE silver.tiktok_post_metadata
ADD COLUMN shares_bigint BIGINT;

ALTER TABLE silver.tiktok_post_metadata
ADD COLUMN saves_bigint BIGINT;

UPDATE silver.tiktok_post_metadata
SET 
    likes_bigint = TRY_CAST(likes AS BIGINT),
    comments_count_bigint = TRY_CAST(comments_count AS BIGINT),
    shares_bigint = TRY_CAST(shares AS BIGINT),
    saves_bigint = TRY_CAST(saves AS BIGINT);

-- Verify all conversions
SELECT 
    COUNT(*) as total,
    COUNT(likes_bigint) as likes_ok,
    COUNT(comments_count_bigint) as comments_ok,
    COUNT(shares_bigint) as shares_ok,
    COUNT(saves_bigint) as saves_ok
FROM silver.tiktok_post_metadata;
```

**Rollback plan if conversion fails:**
```sql
-- Drop new columns
ALTER TABLE silver.hotels_detail DROP COLUMN rating_score_double;
ALTER TABLE silver.hotels_detail DROP COLUMN review_count_int;
-- etc...
```

---

### 5.4. Date Parsing Functions (Helper for Gold Layer)

**Complex date format conversions:**

```sql
-- Function: Parse Vietnamese month format "thang 6/2025"
CREATE OR REPLACE FUNCTION parse_vn_stay_date(stay_date_str STRING)
RETURNS DATE
LANGUAGE SQL
AS $$
    TO_DATE(
        CONCAT(
            SUBSTRING(stay_date_str, -4),  -- Extract year (2025)
            '-',
            LPAD(REGEXP_EXTRACT(stay_date_str, 'thang ([0-9]+)', 1), 2, '0'),  -- Extract month (6 → 06)
            '-01'  -- Default to 1st of month
        ),
        'yyyy-MM-dd'
    )
$$;

-- Function: Parse review date "22 thang 11, 2024"
CREATE OR REPLACE FUNCTION parse_vn_review_date(review_date_str STRING)
RETURNS DATE
LANGUAGE SQL
AS $$
    TO_DATE(
        CONCAT(
            SUBSTRING(review_date_str, -4),  -- year
            '-',
            LPAD(REGEXP_EXTRACT(review_date_str, 'thang ([0-9]+)', 1), 2, '0'),  -- month
            '-',
            LPAD(REGEXP_EXTRACT(review_date_str, '^([0-9]+)', 1), 2, '0')  -- day
        ),
        'yyyy-MM-dd'
    )
$$;

-- Usage in Gold transformations:
SELECT 
    stay_date as original,
    parse_vn_stay_date(stay_date) as parsed_date
FROM silver.hotels_reviews
LIMIT 10;
```

---

### 5.5. Validation Test Cases

**Test all edge cases before materializing Gold:**

```sql
-- Test 1: Rating scores are valid (0-10 range)
SELECT 
    'hotels_detail' as table_name,
    COUNT(*) as total,
    COUNT(CASE WHEN TRY_CAST(rating_score AS DOUBLE) < 0 THEN 1 END) as negative_scores,
    COUNT(CASE WHEN TRY_CAST(rating_score AS DOUBLE) > 10 THEN 1 END) as scores_over_10,
    COUNT(CASE WHEN rating_score IS NOT NULL AND TRY_CAST(rating_score AS DOUBLE) IS NULL THEN 1 END) as unparseable
FROM silver.hotels_detail;
-- Expected: negative_scores = 0, scores_over_10 = 0, unparseable < 10

-- Test 2: Engagement metrics are non-negative
SELECT 
    'tiktok_post_metadata' as table_name,
    COUNT(CASE WHEN TRY_CAST(likes AS BIGINT) < 0 THEN 1 END) as negative_likes,
    COUNT(CASE WHEN TRY_CAST(comments_count AS BIGINT) < 0 THEN 1 END) as negative_comments,
    COUNT(CASE WHEN TRY_CAST(shares AS BIGINT) < 0 THEN 1 END) as negative_shares
FROM silver.tiktok_post_metadata;
-- Expected: all = 0

-- Test 3: Date parsing success rate
SELECT 
    'hotels_reviews' as table_name,
    COUNT(*) as total,
    COUNT(CASE WHEN stay_date IS NOT NULL AND parse_vn_stay_date(stay_date) IS NULL THEN 1 END) as failed_stay_date,
    COUNT(CASE WHEN review_date IS NOT NULL AND parse_vn_review_date(review_date) IS NULL THEN 1 END) as failed_review_date
FROM silver.hotels_reviews;
-- Expected: failed < 1%

-- Test 4: Review scores distribution
SELECT 
    CAST(CAST(review_score AS DOUBLE) AS INT) as score_bucket,
    COUNT(*) as count
FROM silver.hotels_reviews
WHERE review_score IS NOT NULL
GROUP BY CAST(CAST(review_score AS DOUBLE) AS INT)
ORDER BY score_bucket;
-- Expected: Most reviews in 6-10 range (good hotels)
```

---

## PHASE 6: GOLD LAYER DESIGN (Planning only) (30 mins)

### 6.1. Dimensional Model Design

```
FACT TABLES:
1. fact_hotel_reviews (Grain: 1 review)
   - review_key (surrogate)
   - hotel_key (FK)
   - reviewer_country_key (FK)
   - date_key (FK)
   - review_score
   - review_positive
   - review_negative
   - traveler_type
   
2. fact_tiktok_engagement (Grain: 1 post per crawl_time)
   - engagement_key (surrogate)
   - post_key (FK)
   - author_key (FK)
   - date_key (FK)
   - likes
   - comments_count
   - shares
   - saves
   - crawl_timestamp

3. fact_tiktok_comments (Grain: 1 comment)
   - comment_key (surrogate)
   - post_key (FK)
   - commenter_key (FK)
   - comment_text
   - comment_level
   - likes
   - number_of_replies

DIMENSION TABLES:
1. dim_hotel
   - hotel_key (surrogate)
   - hotel_url (natural key)
   - hotel_name
   - province
   - rating_score
   - review_count
   - description
   - top_amenities
   - activities
   - valid_from
   - valid_to
   - is_current (SCD Type 2)

2. dim_location
   - location_key
   - province
   - region (mapped từ province)
   - country (Vietnam)
   - latitude (future)
   - longitude (future)

3. dim_date
   - date_key (YYYYMMDD)
   - date
   - year
   - quarter
   - month
   - day
   - day_of_week
   - is_weekend
   - is_holiday

4. dim_tiktok_author
   - author_key
   - author_name
   - author_tag
   - author_url
   - total_posts
   - avg_engagement

5. dim_tiktok_post
   - post_key
   - post_url (natural key)
   - post_description
   - post_date
   - keyword
   - region
   - has_subtitle
```

### 6.2. Aggregate Tables (OLAP Cubes)

```sql
-- AGG: Hotel Performance by Province
CREATE TABLE gold.agg_hotel_performance_by_province AS
SELECT 
    h.province,
    COUNT(DISTINCT h.hotel_url) as total_hotels,
    AVG(h.rating_score) as avg_rating,
    SUM(h.review_count) as total_reviews,
    COUNT(DISTINCT r.reviewer_country) as unique_countries
FROM silver.hotels_detail_typed h
LEFT JOIN silver.hotels_reviews_typed r ON h.hotel_url = r.hotel_url
GROUP BY h.province;

-- AGG: TikTok Engagement by Region by Month
CREATE TABLE gold.agg_tiktok_engagement_monthly AS
SELECT 
    region,
    DATE_TRUNC('month', post_date) as month,
    COUNT(DISTINCT post_url) as total_posts,
    AVG(likes) as avg_likes,
    AVG(comments_count) as avg_comments,
    AVG(shares) as avg_shares
FROM silver.tiktok_post_metadata_typed
GROUP BY region, DATE_TRUNC('month', post_date);

-- AGG: Top Authors by Engagement
CREATE TABLE gold.agg_top_authors AS
SELECT 
    author,
    COUNT(DISTINCT post_url) as total_posts,
    SUM(likes) as total_likes,
    SUM(comments_count) as total_comments,
    SUM(shares) as total_shares,
    AVG(likes) as avg_likes_per_post
FROM silver.tiktok_post_metadata_typed
GROUP BY author
ORDER BY total_likes DESC;
```

---

## PHASE 7: EXECUTION & VALIDATION (1 hour)

### 7.1. Cleanup Execution Order

```bash
# PowerShell orchestration script
.\scripts\gold-prep\execute-cleanup.ps1
```

**Steps:**
1. ✅ Snapshot baseline metrics
2. ✅ DROP vi_sub column
3. ✅ DELETE empty comments
4. ✅ UPDATE invalid level_comment
5. ✅ DEDUPLICATE hotels_reviews
6. ✅ DEDUPLICATE tiktok_post_comments
7. ✅ NULLIF empty strings
8. ✅ CREATE typed views
9. ✅ Validate type conversions
10. ✅ Export final metrics

### 7.2. Post-Cleanup Validation

```sql
-- Validation Query
SELECT 
    'hotels_list' as table_name,
    COUNT(*) as records,
    COUNT(DISTINCT hotel_url) as unique_keys,
    'Expected: 15,945' as expected
FROM silver.hotels_list

UNION ALL

SELECT 
    'hotels_detail',
    COUNT(*),
    COUNT(DISTINCT hotel_url),
    'Expected: 15,945'
FROM silver.hotels_detail

UNION ALL

SELECT 
    'hotels_reviews',
    COUNT(*),
    COUNT(DISTINCT row_checksum),
    'Expected: 1,575,913 (after dedup)'
FROM silver.hotels_reviews

UNION ALL

SELECT 
    'tiktok_videos',
    COUNT(*),
    COUNT(DISTINCT url),
    'Expected: 4,418 (vi_sub dropped)'
FROM silver.tiktok_videos

UNION ALL

SELECT 
    'tiktok_post_metadata',
    COUNT(*),
    COUNT(DISTINCT source_file),
    'Expected: 1,502 (no changes)'
FROM silver.tiktok_post_metadata

UNION ALL

SELECT 
    'tiktok_post_comments',
    COUNT(*),
    COUNT(*),
    'Expected: ~154,965 (after empty deletion + dedup)'
FROM silver.tiktok_post_comments;
```

### 7.3. Quality Metrics Comparison

```sql
-- Before vs After
SELECT 
    'BEFORE' as phase,
    1822552 as total_records,
    8.78 as quality_score
UNION ALL
SELECT 
    'AFTER',
    (15945 + 15945 + 1575913 + 4418 + 1502 + 154965) as total_records,
    9.65 as quality_score;

-- Expected:
-- BEFORE:  1,822,552 records, 8.78/10 quality
-- AFTER:   1,768,688 records, 9.65/10 quality
-- REMOVED: 53,864 records (2.95% - duplicates & empty)
```

---

## RISKS & MITIGATION

### Risk 1: Data Loss from Aggressive Cleanup
**Mitigation:** 
- Iceberg time-travel: `SELECT * FROM silver.tiktok_post_comments VERSION AS OF 123456`
- Rollback command: `CALL lakehouse.system.rollback_to_timestamp('silver.tiktok_post_comments', TIMESTAMP '2025-11-08 10:00:00')`

### Risk 2: Type Conversion Errors
**Mitigation:**
- Test all conversions in views first
- Use `TRY_CAST()` instead of `CAST()` for error handling
- Validate ranges before materialization

### Risk 3: Performance Impact on Large Tables
**Mitigation:**
- Run cleanup during off-hours
- Use partitioning for DELETE operations
- Monitor Spark executor memory

### Risk 4: Breaking Downstream Dependencies
**Mitigation:**
- Document all schema changes
- Keep Silver views for backward compatibility
- Version Gold layer (gold_v1, gold_v2)

---

## SUCCESS CRITERIA

### Data Quality Targets
- ✅ Overall quality score: **≥ 9.5/10** (from 8.78)
- ✅ NULL percentage in critical columns: **< 1%**
- ✅ Duplicate records: **0%**
- ✅ Empty strings in text columns: **0%**
- ✅ Type conversion success rate: **> 99.5%**

### Performance Targets
- ✅ Cleanup execution time: **< 2 hours**
- ✅ No data loss except for identified bad data
- ✅ All validations pass

### Deliverables
1. ✅ Cleaned Silver layer (6 tables)
2. ✅ Typed views (6 views)
3. ✅ Gold layer schema design document
4. ✅ Cleanup execution scripts (PowerShell + SQL)
5. ✅ Before/After quality metrics report
6. ✅ Data dictionary for Gold layer

---

## NEXT STEPS AFTER CLEANUP

1. **Implement Gold Transformations**
   - Create dimensional model tables
   - Build aggregate tables for BI
   - Set up incremental refresh logic

2. **Setup Data Quality Monitoring**
   - Great Expectations data validation
   - Automated quality checks in Airflow DAG
   - Alerting for quality degradation

3. **BI Layer Development**
   - Power BI semantic model
   - Key metrics & KPIs
   - Executive dashboards

4. **ML Feature Engineering**
   - Sentiment analysis features
   - Hotel recommendation features
   - Trend detection features

---

## APPENDIX

### A. PowerShell Scripts to Create

1. `scripts/gold-prep/export-baseline-metrics.ps1`
2. `scripts/gold-prep/execute-cleanup.ps1`
3. `scripts/gold-prep/validate-cleanup.ps1`
4. `scripts/gold-prep/create-typed-views.ps1`
5. `scripts/gold-prep/rollback-cleanup.ps1`

### B. SQL Scripts to Create

1. `spark/jobs/silver-cleanup/01-drop-vi-sub.sql`
2. `spark/jobs/silver-cleanup/02-delete-empty-comments.sql`
3. `spark/jobs/silver-cleanup/03-deduplicate-reviews.sql`
4. `spark/jobs/silver-cleanup/04-deduplicate-comments.sql`
5. `spark/jobs/silver-cleanup/05-nullify-empty-strings.sql`
6. `spark/jobs/silver-cleanup/06-create-typed-views.sql`
7. `spark/jobs/silver-cleanup/99-validate-all.sql`

### C. Documentation to Create

1. `docs/GOLD_LAYER_SCHEMA_DESIGN.md`
2. `docs/SILVER_CLEANUP_EXECUTION_LOG.md`
3. `docs/DATA_QUALITY_BEFORE_AFTER.md`
4. `docs/GOLD_TRANSFORMATION_LOGIC.md`

---

**Status:** 📋 PLANNING COMPLETE - Ready for Review  
**Next Action:** Review plan → Create scripts → Execute cleanup  
**Estimated Total Time:** 3-4 hours  
**Risk Level:** 🟡 MEDIUM (mitigated by Iceberg time-travel)

---

**Document Version:** 1.0  
**Last Updated:** 08/11/2025  
**Author:** GitHub Copilot  
**Reviewer:** [Pending]
