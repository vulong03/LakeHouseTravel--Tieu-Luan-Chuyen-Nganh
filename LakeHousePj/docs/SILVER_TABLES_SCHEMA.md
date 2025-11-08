# SILVER LAYER - TABLE SCHEMAS DOCUMENTATION# SILVER LAYER - TABLE SCHEMAS DOCUMENTATION



**Generated:** 08/11/2025  **Generated:** 08/11/2025  

**Database:** `silver`  **Database:** `silver`  

**Format:** Apache Iceberg + Parquet (Snappy compression)  **Format:** Apache Iceberg + Parquet (Snappy compression)  

**Total Tables:** 6  **Total Tables:** 6

**Total Records:** 1,822,552

---

---

## Table: `hotels_list`

## 1. Table: `hotels_list`

**Purpose:** Master list of all hotels (minimal info for indexing)  

**Purpose:** Master list of all hotels  **Records:** 15,945  

**Records:** 15,945  **Partition:** `province`

**Partition:** `province`

| Column | Type | Description |

| Column | Type | Description ||--------|------|-------------|

|--------|------|-------------|| `stt` | string | Row number/index |

| `stt` | string | Row number/index || `hotel_name` | string | Hotel name |

| `hotel_name` | string | Hotel name || `hotel_url` | string | Hotel URL (Primary Key) |

| `hotel_url` | string | Hotel URL (Primary Key) || `province` | string | Province/City (Partition Key) |

| `province` | string | Province/City (Partition Key) || `row_checksum` | string | MD5 hash for deduplication |

| `row_checksum` | string | MD5 hash for deduplication || `ingestion_timestamp` | timestamp | When data was ingested to Silver |

| `ingestion_timestamp` | timestamp | When data was ingested to Silver || `source_file` | string | Source CSV file from Bronze layer |

| `source_file` | string | Source CSV file from Bronze || `source_file_checksum` | string | Checksum of source file |

| `source_file_checksum` | string | Checksum of source file |

**Data Quality:** ✅ 10/10 - Perfect quality, 0% NULL

**Quality:** ✅ 10/10 - Perfect, 0% NULL

---

---

## Table: hotels_detail

## 2. Table: `hotels_detail`

| Column | Type | Description |

**Purpose:** Detailed hotel information  |--------|------|-------------|

**Records:** 15,945  | hotel_name | string |  |

**Partition:** `province`| hotel_url | string |  |

| province | string |  |

| Column | Type | Description || description | string |  |

|--------|------|-------------|| top_amenities | string |  |

| `hotel_name` | string | Hotel name || rating_score | string |  |

| `hotel_url` | string | Hotel URL (Primary Key) || review_count_text | string |  |

| `province` | string | Province/City (Partition Key) || rating_breakdown | string |  |

| `description` | string | Hotel description || activities | string |  |

| `top_amenities` | string | Featured amenities || row_checksum | string |  |

| `rating_score` | string | Rating score (cast to DOUBLE in Gold) || source_file | string |  |

| `review_count_text` | string | Number of reviews (text) || source_file_checksum | string |  |

| `rating_breakdown` | string | Rating by criteria || province | string |  |

| `activities` | string | Activities (94% NULL - acceptable) |

| `row_checksum` | string | MD5 hash for deduplication |---

| `ingestion_timestamp` | timestamp | Ingestion timestamp |

| `source_file` | string | Source CSV file |## Table: hotels_reviews

| `source_file_checksum` | string | Source file checksum |

| Column | Type | Description |

**Quality:** ✅ 9.3/10 - Good|--------|------|-------------|

| hotel_name | string |  |

---| hotel_url | string |  |

| reviewer_name | string |  |

## 3. Table: `hotels_reviews`| reviewer_country | string |  |

| room_type | string |  |

**Purpose:** Customer reviews  | stay_date | string |  |

**Records:** 1,588,229 (87% of total data)  | traveler_type | string |  |

**Partition:** None| review_date | string |  |

| review_title | string |  |

| Column | Type | Description || review_score | string |  |

|--------|------|-------------|| review_positive | string |  |

| `hotel_name` | string | Hotel name || review_negative | string |  |

| `hotel_url` | string | Hotel URL (FK to hotels_detail) || row_checksum | string |  |

| `reviewer_name` | string | Reviewer name || source_file | string |  |

| `reviewer_country` | string | Reviewer country || source_file_checksum | string |  |

| `room_type` | string | Room type |

| `stay_date` | string | Stay date (format: "thang 6/2025") |---

| `traveler_type` | string | Traveler type |

| `review_date` | string | Review date |## Table: tiktok_videos

| `review_title` | string | Review title |

| `review_score` | string | Score 0-10 (cast to DOUBLE) || Column | Type | Description |

| `review_positive` | string | Positive text (38.6% NULL) ||--------|------|-------------|

| `review_negative` | string | Negative text (63% NULL) || url | string |  |

| `row_checksum` | string | MD5 hash || posted_date | string |  |

| `ingestion_timestamp` | timestamp | Ingestion timestamp || read_status | string |  |

| `source_file` | string | Source file || keyword | string |  |

| `source_file_checksum` | string | Source checksum || ques_id | string |  |

| target_type | string |  |

**Quality:** ✅ 9.3/10 - Excellent  | region | string |  |

**Duplicates:** 🟡 12,316 (0.78%)| has_sub | string |  |

| vi_sub | string |  |

---| row_checksum | string |  |

| source_file | string |  |

## 4. Table: `tiktok_videos`| source_file_checksum | string |  |

| region | string |  |

**Purpose:** TikTok video metadata  

**Records:** 4,418  ---

**Partition:** `region`

## Table: tiktok_post_metadata

| Column | Type | Description |

|--------|------|-------------|| Column | Type | Description |

| `url` | string | Video URL (Primary Key) ||--------|------|-------------|

| `posted_date` | string | Posted date (5.9% NULL) || post_url | string |  |

| `read_status` | string | Read status || author | string |  |

| `keyword` | string | Search keyword || author_tag | string |  |

| `ques_id` | string | Question ID || author_url | string |  |

| `target_type` | string | Target type || post_date | string |  |

| `region` | string | Region (Partition Key - 8 regions) || post_description | string |  |

| `has_sub` | string | Has subtitle flag || likes | string |  |

| `vi_sub` | string | Vietnamese subtitle (🔴 100% NULL - DROP!) || comments_count | string |  |

| `row_checksum` | string | MD5 hash || saves | string |  |

| `ingestion_timestamp` | timestamp | Ingestion timestamp || shares | string |  |

| `source_file` | string | Source file || comments_level1 | string |  |

| `source_file_checksum` | string | Source checksum || comments_level2 | string |  |

| comments_loaded | string |  |

**Quality:** ⚠️ 7.7/10 - vi_sub 100% NULL  | comments_displayed_tiktok | string |  |

**Action:** DROP vi_sub column| comments_difference | string |  |

| source_file | string |  |

---| source_file_checksum | string |  |



## 5. Table: `tiktok_post_metadata`---



**Purpose:** TikTok post engagement metrics  ## Table: tiktok_post_comments

**Records:** 1,502  

**Partition:** None| Column | Type | Description |

|--------|------|-------------|

| Column | Type | Description || post_url | string |  |

|--------|------|-------------|| stt | string |  |

| `post_url` | string | Post URL (Primary Key) || ten | string |  |

| `author` | string | Author name || tag_ten | string |  |

| `author_tag` | string | Author username || url | string |  |

| `author_url` | string | Author profile URL || comment | string |  |

| `post_date` | string | Post date || likes | string |  |

| `post_description` | string | Post caption || level_comment | string |  |

| `likes` | string | Likes count (cast to BIGINT) || replied_to_tag_name | string |  |

| `comments_count` | string | Comments count (cast to BIGINT) || number_of_replies | string |  |

| `saves` | string | Saves count (cast to BIGINT) || source_file | string |  |

| `shares` | string | Shares count (cast to BIGINT) || source_file_checksum | string |  |

| `comments_level1` | string | Level 1 comments || post_url | string |  |

| `comments_level2` | string | Level 2 comments (replies) |

| `comments_loaded` | string | Comments loaded |---

| `comments_displayed_tiktok` | string | Comments displayed |

| `comments_difference` | string | Comment difference |
| `crawl_time` | string | Crawl timestamp |
| `ingestion_timestamp` | timestamp | Ingestion timestamp |
| `source_file` | string | Source file |
| `source_file_checksum` | string | Source checksum |

**Quality:** ✅ 9.7/10 - Excellent, 0% NULL  
**Duplicates:** ✅ 87 (5.8%) - Valid time-series data

---

## 6. Table: `tiktok_post_comments`

**Purpose:** Comments on TikTok posts  
**Records:** 196,513  
**Partition:** `post_url`

| Column | Type | Description |
|--------|------|-------------|
| `post_url` | string | Post URL (Partition Key, FK) |
| `stt` | string | Comment sequence |
| `ten` | string | Commenter name |
| `tag_ten` | string | Commenter username |
| `url` | string | Commenter profile URL |
| `comment` | string | Comment text (🔴 18.2% empty!) |
| `time` | string | Comment time |
| `likes` | string | Comment likes (cast to INT) |
| `level_comment` | string | Level (No=L1, Yes=L2) |
| `replied_to_tag_name` | string | Replied to username |
| `number_of_replies` | string | Replies count |
| `ingestion_timestamp` | timestamp | Ingestion timestamp |
| `source_file` | string | Source file |
| `source_file_checksum` | string | Source checksum |

**Quality:** ⚠️ 7.7/10 - 35,794 empty comments  
**Duplicates:** 🟡 5,754 (2.93%)  
**Action:** DELETE empty comments

---

## COMMON METADATA COLUMNS

All tables include:

- `row_checksum` (string): MD5 hash for deduplication
- `ingestion_timestamp` (timestamp): Insert time to Silver
- `source_file` (string): Bronze CSV filename
- `source_file_checksum` (string): Source file MD5

---

## DATA TYPE CONVERSIONS NEEDED

| Current | Target | Columns |
|---------|--------|---------|
| string | DOUBLE | rating_score, review_score |
| string | BIGINT | likes, comments_count, shares, saves |
| string | DATE | stay_date, review_date, posted_date, post_date |
| string | TIMESTAMP | time, crawl_time |

---

## PARTITIONING SUMMARY

| Table | Partition Key | # Partitions |
|-------|--------------|--------------|
| hotels_list | province | 63 |
| hotels_detail | province | 63 |
| hotels_reviews | None | N/A |
| tiktok_videos | region | 8 |
| tiktok_post_metadata | None | N/A |
| tiktok_post_comments | post_url | 1,415 |

---

## CRITICAL ACTIONS REQUIRED

1. 🔴 DROP `tiktok_videos.vi_sub` (100% NULL)
2. 🔴 DELETE 35,794 empty comments
3. 🟡 Cast strings to proper types in Gold
4. 🟡 Deduplicate reviews and comments
5. 🟡 Aggregate time-series posts

---

**Last Updated:** 08/11/2025  
**Overall Quality:** 8.78/10
