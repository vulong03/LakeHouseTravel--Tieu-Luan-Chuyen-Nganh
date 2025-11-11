"""
Deep Analysis of TikTok Comments Data
Investigate structure, quality, duplicates, and challenges
"""
import pandas as pd
import os
from pathlib import Path
import numpy as np

# Path to TikTok comments
comments_dir = "D:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/data/raw/tiktok/comments"

print("="*80)
print("🔍 TIKTOK COMMENTS - DEEP DATA ANALYSIS")
print("="*80)

# Find all CSV files
csv_files = list(Path(comments_dir).glob("*.csv"))
print(f"\n📂 Found {len(csv_files)} CSV files:")
for i, f in enumerate(csv_files, 1):
    size_mb = f.stat().st_size / (1024 * 1024)
    print(f"   {i}. {f.name} ({size_mb:.2f} MB)")

# =============================================================================
# SECTION 1: LOAD ALL FILES
# =============================================================================
print(f"\n" + "="*80)
print(f"📖 SECTION 1: LOADING ALL FILES")
print(f"="*80)

dfs = []
file_info = []

for csv_file in csv_files:
    print(f"\nReading: {csv_file.name}")
    try:
        # TikTok comment files have metadata header (17 lines)
        # Then CSV data starting with: STT,Tên,Tag tên,URL,Comment,...
        df = pd.read_csv(csv_file, skiprows=17)
        dfs.append(df)
        file_info.append({
            'filename': csv_file.name,
            'records': len(df),
            'columns': len(df.columns),
            'size_mb': csv_file.stat().st_size / (1024 * 1024)
        })
        print(f"   ✅ Loaded: {len(df):,} records, {len(df.columns)} columns")
    except Exception as e:
        print(f"   ❌ Error: {e}")

# Combine all dataframes
if dfs:
    df_all = pd.concat(dfs, ignore_index=True)
    print(f"\n📊 COMBINED DATA:")
    print(f"   Total records: {len(df_all):,}")
    print(f"   Total size: {sum(f['size_mb'] for f in file_info):.2f} MB")
else:
    print("❌ No data loaded!")
    exit(1)

# =============================================================================
# SECTION 2: DATA STRUCTURE
# =============================================================================
print(f"\n" + "="*80)
print(f"📋 SECTION 2: DATA STRUCTURE")
print(f"="*80)

print(f"\nColumns ({len(df_all.columns)}):")
for i, col in enumerate(df_all.columns, 1):
    print(f"   {i:2d}. {col}")

print(f"\nData types:")
for col in df_all.columns:
    print(f"   {col:30s} : {df_all[col].dtype}")

print(f"\nSample records (first 3):")
print(df_all.head(3).to_string())

# =============================================================================
# SECTION 3: NULL VALUES ANALYSIS
# =============================================================================
print(f"\n" + "="*80)
print(f"❌ SECTION 3: NULL VALUES ANALYSIS")
print(f"="*80)

print(f"\nNULL values by column:")
print(f"{'Column':<30s} │ {'NOT NULL':>12s} │ {'NULL':>12s} │ {'NULL %':>8s}")
print("─" * 80)

null_summary = []
for col in df_all.columns:
    null_count = df_all[col].isna().sum()
    not_null = len(df_all) - null_count
    null_pct = null_count / len(df_all) * 100
    null_summary.append({
        'column': col,
        'not_null': not_null,
        'null': null_count,
        'null_pct': null_pct
    })
    print(f"{col:<30s} │ {not_null:>12,} │ {null_count:>12,} │ {null_pct:>7.2f}%")

# Identify critical columns (>50% NULL)
critical_nulls = [s for s in null_summary if s['null_pct'] > 50]
if critical_nulls:
    print(f"\n⚠️  CRITICAL: Columns with >50% NULL:")
    for s in critical_nulls:
        print(f"   - {s['column']}: {s['null_pct']:.2f}% NULL")

# =============================================================================
# SECTION 4: DUPLICATE DETECTION
# =============================================================================
print(f"\n" + "="*80)
print(f"🔄 SECTION 4: DUPLICATE DETECTION")
print(f"="*80)

# Full row duplicates
full_dup = df_all.duplicated(keep=False).sum()
print(f"\n1. EXACT FULL ROW DUPLICATES:")
print(f"   Count: {full_dup:,} ({full_dup/len(df_all)*100:.2f}%)")

# Identify key columns for dedup
key_cols = []
for col in df_all.columns:
    if any(keyword in col.lower() for keyword in ['id', 'cid', 'comment_id', 'text', 'author']):
        key_cols.append(col)

print(f"\n2. KEY COLUMNS FOR DEDUPLICATION:")
print(f"   Identified columns: {key_cols}")

if key_cols:
    for col in key_cols:
        unique_count = df_all[col].nunique()
        total_count = df_all[col].notna().sum()
        dup_rate = (total_count - unique_count) / total_count * 100 if total_count > 0 else 0
        print(f"   - {col}: {unique_count:,} unique / {total_count:,} total ({dup_rate:.2f}% duplicate rate)")
    
    # Try dedup on combination of key columns
    if len(key_cols) >= 2:
        print(f"\n3. DEDUPLICATION ON KEY COLUMNS COMBINATION:")
        combo_dup = df_all.duplicated(subset=key_cols, keep=False).sum()
        unique_combo = len(df_all) - combo_dup
        print(f"   Columns used: {key_cols}")
        print(f"   Unique: {unique_combo:,} ({unique_combo/len(df_all)*100:.2f}%)")
        print(f"   Duplicates: {combo_dup:,} ({combo_dup/len(df_all)*100:.2f}%)")
        
        if combo_dup > 0:
            print(f"\n   Sample duplicates (first 5 groups):")
            dup_df = df_all[df_all.duplicated(subset=key_cols, keep=False)].sort_values(key_cols)
            for i, (key, group) in enumerate(dup_df.groupby(key_cols)):
                if i >= 5:
                    break
                print(f"\n   Group {i+1} ({len(group)} copies):")
                for col in key_cols:
                    idx = key_cols.index(col)
                    val = key[idx] if isinstance(key, tuple) else key
                    print(f"      {col}: {val}")

# =============================================================================
# SECTION 5: TEXT ANALYSIS (if comment text exists)
# =============================================================================
print(f"\n" + "="*80)
print(f"📝 SECTION 5: TEXT ANALYSIS")
print(f"="*80)

text_cols = [col for col in df_all.columns if 'text' in col.lower() or 'comment' in col.lower() or 'content' in col.lower()]
print(f"\nText columns found: {text_cols}")

if text_cols:
    for col in text_cols:
        if df_all[col].notna().sum() > 0:
            print(f"\nAnalyzing column: '{col}'")
            
            # Text length statistics
            df_all[f'{col}_length'] = df_all[col].astype(str).str.len()
            print(f"   Length statistics:")
            print(f"      Min: {df_all[f'{col}_length'].min()}")
            print(f"      Max: {df_all[f'{col}_length'].max()}")
            print(f"      Mean: {df_all[f'{col}_length'].mean():.2f}")
            print(f"      Median: {df_all[f'{col}_length'].median():.2f}")
            
            # Empty/very short texts
            empty = (df_all[col].isna()) | (df_all[col].astype(str).str.strip() == '')
            very_short = df_all[f'{col}_length'] < 3
            print(f"   Empty texts: {empty.sum():,} ({empty.sum()/len(df_all)*100:.2f}%)")
            print(f"   Very short (<3 chars): {very_short.sum():,} ({very_short.sum()/len(df_all)*100:.2f}%)")
            
            # Sample texts
            print(f"\n   Sample texts (first 5):")
            for i, text in enumerate(df_all[col].dropna().head(5), 1):
                preview = str(text)[:100] + '...' if len(str(text)) > 100 else str(text)
                print(f"      {i}. {preview}")

# =============================================================================
# SECTION 6: TIMESTAMP/DATE ANALYSIS
# =============================================================================
print(f"\n" + "="*80)
print(f"📅 SECTION 6: TIMESTAMP/DATE ANALYSIS")
print(f"="*80)

date_cols = [col for col in df_all.columns if any(keyword in col.lower() for keyword in ['date', 'time', 'created', 'posted'])]
print(f"\nDate/time columns found: {date_cols}")

if date_cols:
    for col in date_cols:
        print(f"\nAnalyzing column: '{col}'")
        print(f"   Data type: {df_all[col].dtype}")
        print(f"   Sample values (first 5):")
        for i, val in enumerate(df_all[col].dropna().head(5), 1):
            print(f"      {i}. {val}")
        
        # Try to parse as datetime
        try:
            df_all[f'{col}_parsed'] = pd.to_datetime(df_all[col], errors='coerce')
            parsed = df_all[f'{col}_parsed'].notna().sum()
            print(f"   Parsing success: {parsed:,} / {len(df_all):,} ({parsed/len(df_all)*100:.2f}%)")
            
            if parsed > 0:
                print(f"   Date range:")
                print(f"      Min: {df_all[f'{col}_parsed'].min()}")
                print(f"      Max: {df_all[f'{col}_parsed'].max()}")
        except Exception as e:
            print(f"   ⚠️  Could not parse as datetime: {e}")

# =============================================================================
# SECTION 7: NUMERIC COLUMNS (likes, replies, etc.)
# =============================================================================
print(f"\n" + "="*80)
print(f"📊 SECTION 7: NUMERIC COLUMNS ANALYSIS")
print(f"="*80)

numeric_cols = df_all.select_dtypes(include=[np.number]).columns.tolist()
print(f"\nNumeric columns found: {numeric_cols}")

if numeric_cols:
    for col in numeric_cols:
        print(f"\n{col}:")
        print(f"   Count: {df_all[col].notna().sum():,}")
        print(f"   Min: {df_all[col].min()}")
        print(f"   Max: {df_all[col].max()}")
        print(f"   Mean: {df_all[col].mean():.2f}")
        print(f"   Median: {df_all[col].median():.2f}")
        
        # Distribution
        print(f"   Distribution:")
        print(f"      Zero values: {(df_all[col] == 0).sum():,}")
        print(f"      Positive: {(df_all[col] > 0).sum():,}")
        if (df_all[col] < 0).sum() > 0:
            print(f"      Negative: {(df_all[col] < 0).sum():,}")

# =============================================================================
# SECTION 8: RECOMMENDATIONS
# =============================================================================
print(f"\n" + "="*80)
print(f"💡 SECTION 8: PIPELINE RECOMMENDATIONS")
print(f"="*80)

print(f"\n1. DATA QUALITY ISSUES:")
if critical_nulls:
    print(f"   ⚠️  Critical NULL columns (>50%):")
    for s in critical_nulls:
        print(f"      - {s['column']}: {s['null_pct']:.2f}% NULL")
    print(f"   → Decision: Drop these columns OR accept high NULL rate")
else:
    print(f"   ✅ No critical NULL issues")

print(f"\n2. DEDUPLICATION STRATEGY:")
if key_cols:
    print(f"   Recommended dedup columns: {key_cols}")
    if len(key_cols) >= 2:
        print(f"   → Use combination of {len(key_cols)} columns for row_checksum")
else:
    print(f"   ⚠️  No clear dedup columns - use all business columns")

print(f"\n3. PARTITIONING STRATEGY:")
if date_cols:
    print(f"   Date columns available: {date_cols}")
    print(f"   → Recommended: Partition by YEAR-MONTH from {date_cols[0]}")
else:
    print(f"   ⚠️  No date columns - consider no partitioning")

print(f"\n4. DATA SIZE & MEMORY:")
total_mb = sum(f['size_mb'] for f in file_info)
print(f"   Total data size: {total_mb:.2f} MB")
print(f"   Total records: {len(df_all):,}")
if total_mb < 100:
    print(f"   → Small dataset - can process in FULL mode")
elif total_mb < 500:
    print(f"   → Medium dataset - consider BATCH mode if needed")
else:
    print(f"   → Large dataset - BATCH mode recommended")

print(f"\n5. CHALLENGES TO HANDLE:")
challenges = []
if critical_nulls:
    challenges.append("High NULL rate in some columns")
if combo_dup > 0:
    challenges.append(f"Duplicates exist ({combo_dup:,} records)")
if not date_cols:
    challenges.append("No clear timestamp for partitioning")
if text_cols and any(empty.sum() > 0 for col in text_cols if col in df_all.columns):
    challenges.append("Empty/short text comments")

if challenges:
    for i, challenge in enumerate(challenges, 1):
        print(f"   {i}. {challenge}")
else:
    print(f"   ✅ No major challenges detected")

print(f"\n" + "="*80)
print(f"✅ ANALYSIS COMPLETE!")
print(f"="*80)
