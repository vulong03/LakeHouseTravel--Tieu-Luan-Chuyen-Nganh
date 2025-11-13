"""
Phân tích CHI TIẾT cấu trúc file TikTok Comments
Mục tiêu: Hiểu rõ format để thiết kế pipeline Silver
"""
import pandas as pd
import json

# Chọn 1 file mẫu có dữ liệu nhiều nhất
sample_file = "D:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/data/raw/tiktok/comments/tiktok_comments_2025-09-26T17-52-04.csv"

print("="*80)
print("🔍 PHÂN TÍCH CHI TIẾT FILE TIKTOK COMMENTS")
print("="*80)
print(f"\nFile mẫu: {sample_file}")

# =============================================================================
# PHẦN 1: METADATA HEADER (17 dòng đầu)
# =============================================================================
print(f"\n" + "="*80)
print("📋 PHẦN 1: METADATA HEADER (17 dòng đầu)")
print("="*80)

with open(sample_file, 'r', encoding='utf-8') as f:
    header_lines = [f.readline().strip() for _ in range(17)]

print("\nNội dung header:")
for i, line in enumerate(header_lines, 1):
    print(f"  {i:2d}. {line}")

# Parse metadata
metadata = {}
for line in header_lines[:15]:  # 15 dòng đầu là metadata key:value
    if ':' in line:
        parts = line.split(':', 1)
        if len(parts) == 2:
            key = parts[0].strip()
            value = parts[1].strip()
            metadata[key] = value

print("\n📊 Metadata đã parse:")
for key, value in metadata.items():
    print(f"   {key}: {value}")

# =============================================================================
# PHẦN 2: CSV DATA (từ dòng 18)
# =============================================================================
print(f"\n" + "="*80)
print("📊 PHẦN 2: CSV DATA (từ dòng 18 trở đi)")
print("="*80)

# Đọc CSV data với proper quoting để handle commas trong text
df = pd.read_csv(sample_file, skiprows=17, encoding='utf-8', 
                 quotechar='"', escapechar='\\', on_bad_lines='skip')

print(f"\n✅ Đã load thành công!")
print(f"   Tổng số records: {len(df):,}")
print(f"   Tổng số columns: {len(df.columns)}")

print(f"\n📋 Tên các cột:")
for i, col in enumerate(df.columns, 1):
    print(f"   {i:2d}. {col}")

print(f"\n📊 Kiểu dữ liệu:")
for col in df.columns:
    print(f"   {col:30s}: {df[col].dtype}")

print(f"\n📈 Thống kê NULL values:")
for col in df.columns:
    null_count = df[col].isna().sum()
    null_pct = null_count / len(df) * 100
    not_null = len(df) - null_count
    print(f"   {col:30s}: NOT NULL={not_null:>5,} | NULL={null_count:>5,} ({null_pct:5.1f}%)")

# =============================================================================
# PHẦN 3: PHÂN TÍCH TỪNG CỘT
# =============================================================================
print(f"\n" + "="*80)
print("🔍 PHẦN 3: PHÂN TÍCH CHI TIẾT TỪNG CỘT")
print("="*80)

# STT (ID)
print(f"\n1. Cột 'STT' (Comment ID):")
print(f"   - Kiểu: {df['STT'].dtype}")
print(f"   - Min: {df['STT'].min()}, Max: {df['STT'].max()}")
print(f"   - Unique: {df['STT'].nunique()} / {len(df)}")
print(f"   - Sample: {list(df['STT'].head(5))}")

# Tên (Commenter Name)
print(f"\n2. Cột 'Tên' (Commenter Name):")
print(f"   - Kiểu: {df['Tên'].dtype}")
print(f"   - Unique: {df['Tên'].nunique()} / {len(df)}")
print(f"   - Top 5 commenters:")
top_commenters = df['Tên'].value_counts().head(5)
for name, count in top_commenters.items():
    print(f"      {name}: {count} comments")

# Tag tên (Username/Handle)
print(f"\n3. Cột 'Tag tên' (Username):")
print(f"   - Kiểu: {df['Tag tên'].dtype}")
print(f"   - Sample: {list(df['Tag tên'].head(5))}")

# URL (Profile URL)
print(f"\n4. Cột 'URL' (Profile URL):")
print(f"   - Kiểu: {df['URL'].dtype}")
print(f"   - Sample: {list(df['URL'].head(3))}")

# Comment (Text)
print(f"\n5. Cột 'Comment' (Comment Text) - QUAN TRỌNG:")
print(f"   - Kiểu: {df['Comment'].dtype}")
print(f"   - Độ dài text:")
df['comment_length'] = df['Comment'].astype(str).str.len()
print(f"      Min: {df['comment_length'].min()}")
print(f"      Max: {df['comment_length'].max()}")
print(f"      Mean: {df['comment_length'].mean():.2f}")
print(f"      Median: {df['comment_length'].median():.0f}")
print(f"   - Empty comments: {(df['Comment'].isna() | (df['Comment'] == '')).sum()}")
print(f"   - Sample comments (first 5):")
for i, comment in enumerate(df['Comment'].head(5), 1):
    preview = str(comment)[:80] + '...' if len(str(comment)) > 80 else str(comment)
    print(f"      {i}. {preview}")

# Time (Comment Time)
print(f"\n6. Cột 'Time' (Comment Time) - QUAN TRỌNG:")
print(f"   - Kiểu: {df['Time'].dtype}")
print(f"   - Unique values: {df['Time'].nunique()}")
print(f"   - Sample values:")
time_samples = df['Time'].value_counts().head(10)
for time, count in time_samples.items():
    print(f"      {time}: {count} comments")
print(f"\n   ⚠️  FORMAT PHÂN TÍCH:")
print(f"      - Có format ngày: DD-MM-YYYY (vd: 31-8-2025)")
print(f"      - Có format tương đối: 'X ngày trước', 'X tuần trước'")
print(f"      → CẦN XỬ LÝ 2 FORMATS!")

# Likes (Like count)
print(f"\n7. Cột 'Likes' (Like Count):")
print(f"   - Kiểu: {df['Likes'].dtype}")
print(f"   - Min: {df['Likes'].min()}, Max: {df['Likes'].max()}")
print(f"   - Mean: {df['Likes'].mean():.2f}")
print(f"   - Zero likes: {(df['Likes'] == 0).sum()} ({(df['Likes'] == 0).sum()/len(df)*100:.1f}%)")
print(f"   - Distribution:")
print(df['Likes'].value_counts().head(10))

# Level Comment (Reply level)
print(f"\n8. Cột 'Level Comment' (Is Reply?):")
print(f"   - Kiểu: {df['Level Comment'].dtype}")
print(f"   - Unique values: {df['Level Comment'].unique()}")
print(f"   - Distribution:")
print(df['Level Comment'].value_counts())

# Replied To Tag Name (Parent comment author)
print(f"\n9. Cột 'Replied To Tag Name' (Parent Author):")
print(f"   - Kiểu: {df['Replied To Tag Name'].dtype}")
print(f"   - NULL/--- count: {((df['Replied To Tag Name'] == '---') | df['Replied To Tag Name'].isna()).sum()}")
print(f"   - Has parent: {((df['Replied To Tag Name'] != '---') & df['Replied To Tag Name'].notna()).sum()}")
print(f"   - Sample values:")
non_null_parents = df[df['Replied To Tag Name'] != '---']['Replied To Tag Name'].head(5)
for val in non_null_parents:
    print(f"      {val}")

# Number of Replies
print(f"\n10. Cột 'Number of Replies' (Reply Count):")
print(f"   - Kiểu: {df['Number of Replies'].dtype}")
print(f"   - Min: {df['Number of Replies'].min()}, Max: {df['Number of Replies'].max()}")
print(f"   - Mean: {df['Number of Replies'].mean():.2f}")
print(f"   - Zero replies: {(df['Number of Replies'] == 0).sum()} ({(df['Number of Replies'] == 0).sum()/len(df)*100:.1f}%)")

# =============================================================================
# PHẦN 4: QUAN HỆ GIỮA CÁC CỘT
# =============================================================================
print(f"\n" + "="*80)
print("🔗 PHẦN 4: QUAN HỆ GIỮA CÁC CỘT")
print("="*80)

# Level Comment vs Replied To Tag Name
print(f"\n1. Quan hệ Level Comment vs Replied To Tag Name:")
print(f"   Level='No' (root comment):")
no_level = df[df['Level Comment'] == 'No']
print(f"      Tổng: {len(no_level)}")
print(f"      Replied To = '---': {(no_level['Replied To Tag Name'] == '---').sum()}")
print(f"   Level='Yes' (reply comment):")
yes_level = df[df['Level Comment'] == 'Yes']
print(f"      Tổng: {len(yes_level)}")
print(f"      Replied To != '---': {(yes_level['Replied To Tag Name'] != '---').sum()}")

# Number of Replies vs Level Comment
print(f"\n2. Number of Replies vs Level Comment:")
print(f"   Root comments (Level='No') with replies > 0: {((df['Level Comment'] == 'No') & (df['Number of Replies'] > 0)).sum()}")
print(f"   Reply comments (Level='Yes') with replies > 0: {((df['Level Comment'] == 'Yes') & (df['Number of Replies'] > 0)).sum()}")

# =============================================================================
# PHẦN 5: DUPLICATES ANALYSIS
# =============================================================================
print(f"\n" + "="*80)
print("🔄 PHẦN 5: DUPLICATES ANALYSIS")
print("="*80)

# Full row duplicates
full_dup = df.duplicated(keep=False).sum()
print(f"\n1. Full row duplicates: {full_dup} ({full_dup/len(df)*100:.2f}%)")

# Duplicates by business logic (STT, Tên, Comment)
key_cols = ['STT', 'Tên', 'Comment']
business_dup = df.duplicated(subset=key_cols, keep=False).sum()
print(f"\n2. Business duplicates (STT, Tên, Comment):")
print(f"   Count: {business_dup} ({business_dup/len(df)*100:.2f}%)")

if business_dup > 0:
    print(f"\n   Sample duplicates:")
    dup_df = df[df.duplicated(subset=key_cols, keep=False)].sort_values(key_cols)
    for i, (key, group) in enumerate(dup_df.groupby(key_cols)):
        if i >= 3:
            break
        print(f"\n   Group {i+1} ({len(group)} copies):")
        print(f"      STT: {key[0]}, Tên: {key[1]}")
        print(f"      Comment: {key[2][:60]}...")

# =============================================================================
# PHẦN 6: SUMMARY & RECOMMENDATIONS
# =============================================================================
print(f"\n" + "="*80)
print("📝 PHẦN 6: TÓM TẮT & KHUYẾN NGHỊ CHO PIPELINE")
print("="*80)

print(f"\n✅ CẤU TRÚC FILE HIỂU RÕ:")
print(f"   1. Header: 17 dòng metadata (thông tin post, người đăng, stats)")
print(f"   2. CSV Data: Từ dòng 18, gồm 10 cột")
print(f"   3. Total records: {len(df):,}")

print(f"\n📊 COLUMNS QUAN TRỌNG (Bronze → Silver):")
print(f"   ✅ STT: Unique ID cho mỗi comment trong 1 post")
print(f"   ✅ Tên: Tên người comment")
print(f"   ✅ Tag tên: Username/handle")
print(f"   ✅ URL: Profile URL")
print(f"   ✅ Comment: Nội dung comment (TEXT - quan trọng nhất)")
print(f"   ✅ Time: Thời gian comment (2 formats: DD-MM-YYYY và 'X ngày trước')")
print(f"   ✅ Likes: Số likes")
print(f"   ✅ Level Comment: No (root) / Yes (reply)")
print(f"   ✅ Replied To Tag Name: Tag người được reply (--- nếu root)")
print(f"   ✅ Number of Replies: Số lượng reply của comment này")

print(f"\n🔧 XỬ LÝ CẦN THIẾT:")
print(f"   1. METADATA EXTRACTION:")
print(f"      - Parse 17 dòng header để lấy thông tin post")
print(f"      - Thêm vào mỗi comment record: post_url, post_author, post_date")
print(f"   2. TIME PARSING (PHỨC TẠP!):")
print(f"      - Format 1: DD-MM-YYYY → DateType")
print(f"      - Format 2: 'X ngày trước', 'X tuần trước' → tính ngày từ scrape_time")
print(f"   3. TEXT CLEANING:")
print(f"      - Trim whitespace")
print(f"      - Handle NULL/empty comments")
print(f"   4. DEDUPLICATION:")
print(f"      - Key: post_url + STT + Tên + Comment")
print(f"      - row_checksum = MD5(post_url, STT, Tên, Comment, Time)")

print(f"\n⚠️  CHALLENGES:")
print(f"   1. Multiple files → cần process tất cả files trong folder")
print(f"   2. Time format phức tạp → cần regex pattern matching")
print(f"   3. Metadata ở header → cần parse riêng rồi join với data")
print(f"   4. Filename chứa scrape_time → cần extract từ filename")

print(f"\n💡 PIPELINE DESIGN:")
print(f"   STEP 1: Bronze Ingestion")
print(f"      - List all CSV files trong folder")
print(f"      - Mỗi file → parse header + CSV data")
print(f"      - Add scrape_time từ filename")
print(f"      - Save to Bronze Iceberg table")
print(f"   ")
print(f"   STEP 2: Bronze → Scratch Transform")
print(f"      - Parse metadata (post info)")
print(f"      - Parse time (2 formats)")
print(f"      - Normalize text")
print(f"      - Flatten structure")
print(f"   ")
print(f"   STEP 3: Scratch → Silver Clean & Load")
print(f"      - Calculate row_checksum")
print(f"      - Deduplicate (LEFT ANTI JOIN)")
print(f"      - Append to Silver")

print(f"\n🎯 PARTITION STRATEGY:")
print(f"   Option 1: Partition by YEAR-MONTH(comment_date)")
print(f"   Option 2: Partition by post_url (nếu có nhiều posts)")
print(f"   Option 3: No partition (nếu data nhỏ)")

print(f"\n" + "="*80)
print(f"✅ PHÂN TÍCH HOÀN TẤT - SẴN SÀNG THIẾT KẾ PIPELINE!")
print(f"="*80)
