"""
Gradio App: Province Tourism Hotness Forecasting
=================================================

Features:
- Input: Chọn tháng bắt đầu và kết thúc (1-12 tháng sau)
- Filter: Chọn vùng miền (Bắc/Trung/Nam)
- Output: Top N tỉnh thành có hotness cao nhất
- Visualization: Line chart hotness trend theo thời gian
"""

import gradio as gr
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import mlflow
import os
import time
from io import BytesIO
from minio import Minio
from minio.error import S3Error

# MinIO configuration
MINIO_CLIENT = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False
)
BUCKET_NAME = "gold"
FORECAST_PREFIX = "ml_forecast/"

# MLflow configuration
MLFLOW_TRACKING_URI = "postgresql://lakehouse_user:lakehouse_pass@postgres:5432/mlflow_db"

# Model configurations
MODELS = {
    "XGBoost (Reduced)": {
        "name": "province_hotness_forecaster_reduced",
        "file_pattern": "province_hotness_forecast_reduced_",
        "description": "XGBoost với 8 features (temporal + lag)"
    },
    "Random Forest": {
        "name": "province_hotness_forecaster_rf",
        "file_pattern": "province_hotness_forecast_rf_",
        "description": "Random Forest với 8 features (temporal + lag)"
    }
}

# Cache configuration - separate cache per model
CACHE_DURATION = 30  # 30 seconds
_forecast_cache = {}

# Debug mode
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

def debug_print(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)

# Region mapping
REGIONS = {
    "Tất cả": None,
    "Đông Bắc": "Northeast",
    "Tây Bắc": "Northwest",
    "Đồng bằng Sông Hồng": "Red_River_Delta",
    "Bắc Trung Bộ": "North_Central_Coast",
    "Tây Nguyên": "Central_Highlands",
    "Duyên hải Nam Trung Bộ": "South_Central_Coast",
    "Đông Nam Bộ": "Southeast",
    "Đồng bằng Sông Cửu Long": "Mekong_Delta"
}


def clear_cache(model_key=None):
    """Clear forecast data cache for specific model or all models"""
    global _forecast_cache
    if model_key:
        if model_key in _forecast_cache:
            del _forecast_cache[model_key]
            print(f"🗑️ Cache cleared for {model_key}")
    else:
        _forecast_cache = {}
        print("🗑️ All caches cleared")


def load_forecast_data(model_key="XGBoost (Reduced)"):
    """Load forecast data from MinIO Parquet files (latest run folder) with caching per model"""
    # Check cache for this model
    now = time.time()
    if model_key in _forecast_cache:
        cache_entry = _forecast_cache[model_key]
        if cache_entry["data"] is not None and (now - cache_entry["timestamp"]) < CACHE_DURATION:
            print(f"📦 Using cached data for {model_key} from run: {cache_entry['run_folder']} (age: {int(now - cache_entry['timestamp'])}s)")
            return cache_entry["data"], None
    
    # Get model config
    model_config = MODELS.get(model_key)
    if not model_config:
        return pd.DataFrame(), f"❌ Model không tồn tại: {model_key}"
    
    file_pattern = model_config["file_pattern"]
    
    try:
        print(f"🔄 Loading fresh data for {model_key} from MinIO...")
        
        # List all objects in forecast directory
        objects = list(MINIO_CLIENT.list_objects(BUCKET_NAME, prefix=FORECAST_PREFIX, recursive=True))
        
        # Get all folders with their last_modified timestamp (filter by model pattern)
        folder_timestamps = {}
        parquet_files_by_folder = {}
        
        for obj in objects:
            if obj.object_name.endswith('.parquet') and file_pattern in obj.object_name:
                # Extract folder name: ml_forecast/province_hotness_forecast_reduced_20241215_073023/part-xxx.parquet
                parts = obj.object_name.split('/')
                if len(parts) >= 3:
                    run_folder = f"{parts[0]}/{parts[1]}/"  # ml_forecast/province_hotness_forecast_reduced_20241215_073023/
                    
                    # Track latest modified time for this folder
                    if run_folder not in folder_timestamps or obj.last_modified > folder_timestamps[run_folder]:
                        folder_timestamps[run_folder] = obj.last_modified
                    
                    # Track files in this folder
                    if run_folder not in parquet_files_by_folder:
                        parquet_files_by_folder[run_folder] = []
                    parquet_files_by_folder[run_folder].append(obj.object_name)
        
        if not folder_timestamps:
            error_msg = f"⚠️ Không tìm thấy file dự báo cho {model_key} trong MinIO!"
            print(error_msg)
            return pd.DataFrame(), error_msg
        
        print(f"📂 Found {len(folder_timestamps)} run folders:")
        for folder, timestamp in sorted(folder_timestamps.items(), key=lambda x: x[1], reverse=True):
            print(f"   - {folder} (modified: {timestamp})")
        
        # Get latest run folder by last_modified time (not by name)
        latest_run_folder = max(folder_timestamps.items(), key=lambda x: x[1])[0]
        latest_timestamp = folder_timestamps[latest_run_folder]
        
        print(f"🌟 LATEST RUN: {latest_run_folder} (modified: {latest_timestamp})")
        
        # Load ALL parquet files from latest run folder
        df_list = []
        files_in_latest_run = parquet_files_by_folder[latest_run_folder]
        
        print(f"📦 Loading {len(files_in_latest_run)} parquet files from latest run...")
        
        for parquet_file in files_in_latest_run:
            try:
                response = MINIO_CLIENT.get_object(BUCKET_NAME, parquet_file)
                df_part = pd.read_parquet(BytesIO(response.read()))
                df_list.append(df_part)
                print(f"   ✅ Loaded {parquet_file}: {len(df_part)} rows")
            except Exception as e:
                print(f"   ⚠️ Failed to load {parquet_file}: {str(e)}")
                continue
        
        if not df_list:
            error_msg = "⚠️ Không thể đọc dữ liệu từ parquet files!"
            print(error_msg)
            return pd.DataFrame(), error_msg
        
        # Concatenate all dataframes
        df = pd.concat(df_list, ignore_index=True)
        
        print(f"✅ Total loaded: {len(df)} rows from {len(df_list)} files")
        print(f"   - Unique provinces: {df['province_name'].nunique()}")
        print(f"   - Unique regions: {df['region'].nunique()}")
        print(f"   - Date range: {df['forecast_date'].min()} to {df['forecast_date'].max()}")
        
        # Update cache for this model
        _forecast_cache[model_key] = {
            "data": df,
            "timestamp": now,
            "run_folder": latest_run_folder
        }
        
        print(f"✅ Cache updated for {model_key}: {latest_run_folder} at {datetime.now().strftime('%H:%M:%S')}")
        
        return df, None
        
    except S3Error as e:
        error_msg = f"❌ Lỗi kết nối MinIO: {str(e)}"
        print(error_msg)
        return pd.DataFrame(), error_msg
    except Exception as e:
        error_msg = f"❌ Lỗi load dữ liệu: {str(e)}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), error_msg
    
def get_model_info(model_key="XGBoost (Reduced)"):
    """Get latest model information from MLflow"""
    try:
        model_config = MODELS.get(model_key)
        if not model_config:
            return None
        
        model_name = model_config["name"]
        
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = mlflow.MlflowClient()
        
        # Get latest version
        model_versions = client.search_model_versions(f"name='{model_name}'")
        if model_versions:
            latest_version = max(model_versions, key=lambda x: int(x.version))
            run = client.get_run(latest_version.run_id)
            
            return {
                "model_key": model_key,
                "model_name": model_name,
                "version": latest_version.version,
                "run_id": latest_version.run_id,
                "test_rmse": run.data.metrics.get("test_rmse", "N/A"),
                "test_mae": run.data.metrics.get("test_mae", "N/A"),
                "test_r2": run.data.metrics.get("test_r2", "N/A"),
                "trained_at": datetime.fromtimestamp(run.info.start_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
            }
    except Exception as e:
        print(f"Error getting model info for {model_key}: {e}")
        return None


def recommend_provinces(model_key, start_month, end_month, region, top_n):
    """
    Recommend top provinces based on forecasted hotness
    """
    # Convert top_n to integer (handle "All" case)
    if top_n == "All":
        top_n_int = None  # Will use all provinces
    else:
        top_n_int = int(top_n)
    
    # Convert MM/YYYY to year_month integer (YYYYMM)
    month_mapping = {
        "10/2025": 202510, "11/2025": 202511, "12/2025": 202512,
        "01/2026": 202601, "02/2026": 202602, "03/2026": 202603,
        "04/2026": 202604, "05/2026": 202605, "06/2026": 202606,
        "07/2026": 202607, "08/2026": 202608, "09/2026": 202609
    }
    
    start_year_month = month_mapping[start_month]
    end_year_month = month_mapping[end_month]
    
    # Validate range
    if start_year_month > end_year_month:
        error_html = """
<div style="background: #ff4444; padding: 20px; border-radius: 8px; color: white; box-shadow: 0 4px 15px rgba(0,0,0,0.2);">
    <h3 style="margin-top: 0;">❌ Lỗi chọn tháng</h3>
    <p>Tháng bắt đầu phải nhỏ hơn hoặc bằng tháng kết thúc!</p>
</div>
        """
        return pd.DataFrame(), None, error_html
    
    # Load forecast data for selected model
    df, load_error = load_forecast_data(model_key)
    
    if load_error:
        error_html = f"""
<div style="background: #ff4444; padding: 20px; border-radius: 8px; color: white; box-shadow: 0 4px 15px rgba(0,0,0,0.2);">
    <h3 style="margin-top: 0;">❌ Lỗi hệ thống</h3>
    <p>{load_error}</p>
    <p style="margin-top: 15px; font-size: 14px;">Vui lòng kiểm tra kết nối MinIO hoặc chạy lại forecast job!</p>
</div>
        """
        return pd.DataFrame(), None, error_html
    
    if df.empty:
        return pd.DataFrame(), None, "⚠️ Không có dữ liệu forecast!"
    
    print(f"\n🔍 Available horizon_months: {sorted(df['horizon_month'].unique())}")
    print(f"🔍 Available year_months: {sorted(df['year_month'].unique())}")
    
    # Show horizon to year_month mapping and detect duplicates
    horizon_mapping = df[['horizon_month', 'year_month']].drop_duplicates().sort_values('horizon_month')
    print(f"🔍 Horizon → Year_Month mapping:")
    
    year_month_counts = {}
    for _, row in horizon_mapping.iterrows():
        ym = row['year_month']
        h = row['horizon_month']
        print(f"   Horizon {h} → {ym}")
        
        if ym not in year_month_counts:
            year_month_counts[ym] = []
        year_month_counts[ym].append(h)
    
    # Check for duplicate mappings
    duplicates = {ym: horizons for ym, horizons in year_month_counts.items() if len(horizons) > 1}
    if duplicates:
        print(f"⚠️ WARNING: Duplicate year_month mappings detected!")
        for ym, horizons in duplicates.items():
            print(f"   {ym} is mapped from horizons: {horizons}")
    
    # Validate requested months are available
    available_year_months = set(df['year_month'].unique())
    
    # Generate valid YYYYMM sequence with proper month arithmetic
    requested_year_months = []
    current = start_year_month
    while current <= end_year_month:
        requested_year_months.append(current)
        year = current // 100
        month = current % 100
        if month == 12:
            current = (year + 1) * 100 + 1
        else:
            current = current + 1
    requested_year_months = set(requested_year_months)
    
    missing_year_months = requested_year_months - available_year_months
    
    if missing_year_months:
        missing_str = ", ".join([f"{ym//100}-{ym%100:02d}" for ym in sorted(missing_year_months)])
        available_str = ", ".join([f"{ym//100}-{ym%100:02d}" for ym in sorted(available_year_months)])
        warning_html = f"""
<div style="background: linear-gradient(135deg, #ff6b6b 0%, #ee5a6f 100%); padding: 20px; border-radius: 12px; color: white; box-shadow: 0 4px 15px rgba(0,0,0,0.2);">
    <h3 style="margin-top: 0; font-size: 20px;">⚠️ Thiếu dữ liệu cho một số tháng</h3>
    <p style="margin: 10px 0; font-size: 16px;"><strong>Tháng bị thiếu:</strong> {missing_str}</p>
    <p style="margin: 10px 0; font-size: 16px;"><strong>Tháng có sẵn:</strong> {available_str}</p>
    <p style="margin-top: 15px; font-size: 14px; opacity: 0.9;">💡 Vui lòng chọn khoảng thời gian khác hoặc chạy lại forecast job để cập nhật dữ liệu!</p>
</div>
        """
        return pd.DataFrame(), None, warning_html
    
    # Filter by year_month range
    df_filtered = df[(df['year_month'] >= start_year_month) & (df['year_month'] <= end_year_month)]
    
    debug_print(f"🔍 After horizon filter: {len(df_filtered)} rows")
    debug_print(f"🔍 Filtered year_months: {sorted(df_filtered['year_month'].unique())}")
    
    # Filter by region
    if region != "Tất cả":
        region_code = REGIONS[region]
        df_filtered = df_filtered[df_filtered['region'] == region_code]
    
    if df_filtered.empty:
        empty_html = f"""
<div style="background: linear-gradient(135deg, #ffd43b 0%, #ffa733 100%); padding: 20px; border-radius: 12px; color: #333; box-shadow: 0 4px 15px rgba(0,0,0,0.2);">
    <h3 style="margin-top: 0; font-size: 20px;">⚠️ Không có dữ liệu phù hợp</h3>
    <p style="margin: 10px 0;"><strong>Vùng:</strong> {region}</p>
    <p style="margin: 10px 0;"><strong>Khoảng thời gian:</strong> {start_month} đến {end_month}</p>
    <p style="margin-top: 15px; font-size: 14px;">💡 Vui lòng thử lại với bộ lọc khác!</p>
</div>
        """
        return pd.DataFrame(), None, empty_html
    
    # Calculate average hotness for each province
    province_avg = df_filtered.groupby(['province_sk', 'province_name', 'region']).agg({
        'predicted_hotness': 'mean'
    }).reset_index()
    
    province_avg = province_avg.sort_values('predicted_hotness', ascending=False)
    
    # Apply top_n filter if specified
    if top_n_int is not None:
        province_avg = province_avg.head(top_n_int)
    
    province_avg['rank'] = range(1, len(province_avg) + 1)
    province_avg['predicted_hotness'] = province_avg['predicted_hotness'].round(4)
    
    # Create result table
    result_df = province_avg[['rank', 'province_name', 'region', 'predicted_hotness']].copy()
    result_df.columns = ['Hạng', 'Tỉnh/Thành', 'Vùng', 'Hotness Score']
    
    # Create time series chart for top provinces
    top_provinces = province_avg['province_sk'].tolist()
    df_chart = df_filtered[df_filtered['province_sk'].isin(top_provinces)].copy()
    
    # Pre-sort for better performance
    df_chart = df_chart.sort_values(['province_sk', 'horizon_month'])
    
    # Map region codes to Vietnamese
    region_map = {
        'Northeast': 'Đông Bắc',
        'Northwest': 'Tây Bắc',
        'Red_River_Delta': 'Đồng bằng Sông Hồng',
        'North_Central_Coast': 'Bắc Trung Bộ',
        'Central_Highlands': 'Tây Nguyên',
        'South_Central_Coast': 'Duyên hải Nam Trung Bộ',
        'Southeast': 'Đông Nam Bộ',
        'Mekong_Delta': 'Đồng bằng Sông Cửu Long'
    }
    df_chart['region_vn'] = df_chart['region'].map(region_map)
    
    # Use year_month (YYYYMM) instead of forecast_date
    df_chart['date'] = pd.to_datetime(df_chart['year_month'].astype(str), format='%Y%m')
    df_chart['month_label'] = df_chart['date'].dt.strftime('%m/%Y')
    
    # Sort by date to ensure correct plotting order
    df_chart = df_chart.sort_values(['province_name', 'date'])
    
    debug_print(f"🔍 Chart data range: {df_chart['date'].min()} to {df_chart['date'].max()}")
    debug_print(f"🔍 Unique dates in chart: {sorted(df_chart['date'].unique())}")
    
    # Create interactive line chart
    fig = go.Figure()
    
    colors = px.colors.qualitative.Set2
    
    for idx, (province_sk, province_name) in enumerate(zip(province_avg['province_sk'], province_avg['province_name'])):
        # Get data for this province and ensure it's sorted by date
        province_data = df_chart[df_chart['province_sk'] == province_sk].sort_values('date').reset_index(drop=True)
        
        fig.add_trace(go.Scatter(
            x=province_data['date'],
            y=province_data['predicted_hotness'],
            mode='lines+markers',
            name=province_name,
            line=dict(color=colors[idx % len(colors)], width=3),
            marker=dict(size=8),
            hovertemplate=f'<b>{province_name}</b><br>' +
                         'Tháng: %{x|%m/%Y}<br>' +
                         'Hotness: %{y:.4f}<br>' +
                         '<extra></extra>'
        ))
    
    fig.update_layout(
        title={
            'text': f' Xu hướng Hotness Score ({start_month} - {end_month})',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 20, 'family': 'Arial, sans-serif'}
        },
        xaxis_title='Thời gian',
        yaxis_title='Hotness Score',
        hovermode='x unified',
        template='plotly_white',
        height=550,
        plot_bgcolor='rgba(240, 247, 255, 0.5)',
        paper_bgcolor='white',
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.98,
            xanchor="left",
            x=1.02,
            bgcolor="rgba(255, 255, 255, 0.9)",
            bordercolor="lightgray",
            borderwidth=1
        ),
        margin=dict(l=60, r=180, t=80, b=60)
    )
    
    # Format x-axis to show month/year properly
    fig.update_xaxes(
        tickformat='%m/%Y',
        dtick="M1",
        tickangle=-45
    )
    
    # Check for missing provinces
    num_provinces_in_data = df['province_sk'].nunique()
    num_provinces_filtered = df_filtered['province_sk'].nunique()
    
    # Calculate number of months in range
    year_months_in_range = list(range(start_year_month, end_year_month + 1))
    # Handle year overflow (e.g., 202512 -> 202601)
    adjusted_range = []
    for ym in year_months_in_range:
        year = ym // 100
        month = ym % 100
        if month > 12:
            month = month - 12
            year += 1
        adjusted_range.append(year * 100 + month)
    
    num_months_selected = len(set(adjusted_range))
    num_actual_months = df_filtered['year_month'].nunique()  # Số tháng THỰC TẾ sau khi filter
    
    missing_province_warning = ""
    if num_provinces_in_data < 62:
        missing_count = 62 - num_provinces_in_data
        missing_province_warning = f"""
<div style="background: #ffd43b; padding: 15px; margin-top: 10px; border-radius: 8px; color: #333; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
    ⚠️ <strong>Lưu ý:</strong> Thiếu dữ liệu cho {missing_count} tỉnh (do không đủ dữ liệu lịch sử để dự báo)
</div>
        """
    
    # Model info message
    model_info = get_model_info(model_key)
    if model_info:
        # Calculate actual date range from filtered data
        unique_months = sorted(df_filtered['year_month'].unique())
        if len(unique_months) > 0:
            start_date = pd.to_datetime(str(unique_months[0]), format='%Y%m')
            end_date = pd.to_datetime(str(unique_months[-1]), format='%Y%m')
            date_range_str = f"{start_date.strftime('%m/%Y')} - {end_date.strftime('%m/%Y')}"
        else:
            date_range_str = "N/A"
        
        # Warning if actual months != selected months (due to duplicate horizon mapping)
        month_warning = ""
        if num_actual_months != num_months_selected:
            month_warning = f"""
<div style="background: #ff9800; padding: 12px; margin-top: 10px; border-radius: 8px; color: white; font-size: 14px;">
    ⚠️ <strong>Cảnh báo:</strong> Chọn {num_months_selected} tháng nhưng chỉ có {num_actual_months} tháng dữ liệu thực tế (do lỗi horizon mapping trong forecast job)
</div>
            """
        
        info_msg = f"""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 12px; color: white; box-shadow: 0 4px 15px rgba(0,0,0,0.2);">
    <h3 style="margin-top: 0; font-size: 18px;">🤖 Thông tin Model: {model_key}</h3>
    <p style="margin: 8px 0;"><strong>Model Registry:</strong> {model_info['model_name']} <span style="background: rgba(255,255,255,0.3); padding: 3px 8px; border-radius: 5px;">v{model_info['version']}</span></p>
    <p style="margin: 8px 0;"><strong>Độ chính xác:</strong> RMSE={model_info['test_rmse']:.4f} | MAE={model_info['test_mae']:.4f} | R²={model_info['test_r2']:.4f}</p>
    <p style="margin: 8px 0;"><strong>Trained:</strong> {model_info['trained_at']}</p>
    <p style="margin: 8px 0;"><strong>Dữ liệu gốc:</strong> {num_provinces_in_data}/62 tỉnh × 12 tháng = {len(df)} predictions</p>
    <p style="margin: 8px 0;"><strong>Đã lọc:</strong> {num_provinces_filtered} tỉnh × {num_actual_months} tháng thực tế (chọn {num_months_selected}) = {len(df_filtered)} predictions | {date_range_str}</p>
    <p style="margin: 8px 0;"><strong>Hiển thị:</strong> Top {len(province_avg)} tỉnh</p>
</div>
{month_warning}
{missing_province_warning}
        """
    else:
        # Calculate actual date range from filtered data
        unique_months = sorted(df_filtered['year_month'].unique())
        if len(unique_months) > 0:
            start_date = pd.to_datetime(str(unique_months[0]), format='%Y%m')
            end_date = pd.to_datetime(str(unique_months[-1]), format='%Y%m')
            date_range_str = f"{start_date.strftime('%m/%Y')} - {end_date.strftime('%m/%Y')}"
        else:
            date_range_str = "N/A"
        
        # Warning if actual months != selected months
        month_warning = ""
        if num_actual_months != num_months_selected:
            month_warning = f"""
<div style="background: #ff9800; padding: 12px; margin-top: 10px; border-radius: 8px; color: white; font-size: 14px;">
    ⚠️ <strong>Cảnh báo:</strong> Chọn {num_months_selected} tháng nhưng chỉ có {num_actual_months} tháng dữ liệu thực tế (do lỗi horizon mapping trong forecast job)
</div>
            """
        
        info_msg = f"""
<div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); padding: 20px; border-radius: 12px; color: white; box-shadow: 0 4px 15px rgba(0,0,0,0.2);">
    <p style="margin: 0;">✅ Đã tạo {len(province_avg)} recommendations | {num_provinces_filtered} tỉnh × {num_actual_months} tháng thực tế (chọn {num_months_selected}) = {len(df_filtered)} predictions | {date_range_str}</p>
</div>
{month_warning}
{missing_province_warning}
        """
    
    return result_df, fig, info_msg

def create_interface():
    """Create Gradio interface"""
    
    # Custom CSS for better styling
    custom_css = """
    .gradio-container {
        max-width: 1400px !important;
    }
    .header-section {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 40px;
        border-radius: 16px;
        color: white;
        margin-bottom: 30px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
    }
    .header-section h1 {
        margin: 0 0 15px 0;
        font-size: 2.5em;
        font-weight: 700;
    }
    .header-section p {
        font-size: 1.1em;
        opacity: 0.95;
        margin: 5px 0;
    }
    .info-box {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 20px;
        border-radius: 12px;
        color: white;
        margin-top: 20px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.15);
    }
    .dataframe {
        font-size: 14px !important;
    }
    """
    
    with gr.Blocks(title=" Tourism Hotness Forecast", theme=gr.themes.Soft(), css=custom_css) as app:
        # Header
        gr.HTML(
            """
            <div class="header-section">
                <h1>HỆ THỐNG DỰ ĐOÁN XU HƯỚNG DU LỊCH VIỆT NAM</h1>
            </div>
            """
        )
        
        with gr.Row(equal_height=True):
            # Left panel - Filters
            with gr.Column(scale=1):
                gr.HTML('<div class="filter-card">')
                gr.Markdown("##  Bộ lọc tìm kiếm")
                
                # Model selection
                model_selector = gr.Dropdown(
                    choices=list(MODELS.keys()),
                    value="XGBoost (Reduced)",
                    label="🤖 Chọn Model",
                    info="So sánh giữa các thuật toán ML"
                )
                
                search_btn = gr.Button(" Tìm kiếm", variant="primary", size="sm")
                
                # Dynamic month choices: Load from actual forecast data
                month_choices = ["10/2025", "11/2025", "12/2025", 
                                 "01/2026", "02/2026", "03/2026", 
                                 "04/2026", "05/2026", "06/2026", 
                                 "07/2026", "08/2026", "09/2026"]
                
                start_month = gr.Dropdown(
                    choices=month_choices,
                    value="10/2025",
                    label="📅 Tháng bắt đầu",
                    info="Chọn tháng bắt đầu dự báo"
                )
                
                end_month = gr.Dropdown(
                    choices=month_choices,
                    value="03/2026",
                    label="📅 Tháng kết thúc",
                    info="Chọn tháng kết thúc dự báo"
                )
                
                region = gr.Dropdown(
                    choices=list(REGIONS.keys()),
                    value="Tất cả",
                    label="🗺️ Vùng miền",
                    info="Chọn khu vực du lịch yêu thích"
                )
                
                top_n = gr.Dropdown(
                    choices=["5", "10", "15", "20", "All"],
                    value="10",
                    label="🏆 Số lượng Top N",
                    info="Hiển thị bao nhiêu điểm đến?"
                )
                
                gr.HTML('</div>')
                
                # Info box
                info_msg = gr.HTML(
                    """
                    <div style="background: linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%); 
                                padding: 20px; border-radius: 12px; margin-top: 20px; 
                                box-shadow: 0 4px 15px rgba(0,0,0,0.1);">
                        <p style="margin: 0; color: #333; font-weight: 500;">
                            ℹ️ Nhấn <strong>'Tìm kiếm'</strong> để xem kết quả dự báo
                        </p>
                    </div>
                    """
                )
            
            # Right panel - Results
            with gr.Column(scale=2):
                gr.HTML('<div class="result-card">')
                gr.Markdown("## Kết quả Dự báo")
                
                result_table = gr.Dataframe(
                    headers=["Hạng", "Tỉnh/Thành", "Vùng", "Hotness Score"],
                    datatype=["number", "str", "str", "number"],
                    wrap=True,
                    interactive=False,
                    row_count=10,
                    column_widths=["10%", "35%", "35%", "20%"]
                )
                
                result_chart = gr.Plot(label="📈 Biểu đồ xu hướng theo thời gian")
                
                gr.HTML('</div>')
        
        # Helper function to update end_month choices based on start_month
        def update_end_month_choices(selected_start):
            start_idx = month_choices.index(selected_start)
            available_end_months = month_choices[start_idx:]
            return gr.Dropdown(choices=available_end_months, value=available_end_months[0])
        
        # Event handlers
        start_month.change(
            fn=update_end_month_choices,
            inputs=[start_month],
            outputs=[end_month]
        )
        
        search_btn.click(
            fn=recommend_provinces,
            inputs=[model_selector, start_month, end_month, region, top_n],
            outputs=[result_table, result_chart, info_msg]
        )
        
        # Footer explanation
        gr.HTML(
            """
            <div style="background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%); 
                        padding: 30px; border-radius: 16px; margin-top: 30px; 
                        box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
                <h3 style="margin-top: 0; color: #333;">Giải thích Hotness Score</h3>
                <p style="color: #555; line-height: 1.8; margin: 10px 0;">
                    <strong>Hotness Score (0-1)</strong> được tính từ <strong>5 nhóm chỉ số chính</strong> theo cấu trúc phân cấp:
                </p>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 20px 0;">
                    <div style="background: white; padding: 15px; border-radius: 8px; border-left: 4px solid #4CAF50;">
                         <strong>Base Volume (25%)</strong><br>
                        <span style="color: #666; font-size: 0.9em;">
                            • Posts: 60%<br>
                            • Comments: 40%
                        </span>
                    </div>
                    <div style="background: white; padding: 15px; border-radius: 8px; border-left: 4px solid #2196F3;">
                         <strong>Engagement (35%)</strong><br>
                        <span style="color: #666; font-size: 0.9em;">
                            • Post Likes: 45%<br>
                            • Post Saves: 35%<br>
                            • Comment Likes: 20%
                        </span>
                    </div>
                    <div style="background: white; padding: 15px; border-radius: 8px; border-left: 4px solid #FF9800;">
                         <strong>Sentiment (20%)</strong><br>
                        <span style="color: #666; font-size: 0.9em;">
                            • Positive Ratio: 50%<br>
                            • Avg Sentiment: 30%<br>
                            • Negative Ratio: 20%
                        </span>
                    </div>
                    <div style="background: white; padding: 15px; border-radius: 8px; border-left: 4px solid #E91E63;">
                         <strong>Emoji Vibe (5%)</strong><br>
                        <span style="color: #666; font-size: 0.9em;">
                            • Total Emojis: 50%<br>
                            • Emoji Sentiment: 50%
                        </span>
                    </div>
                    <div style="background: white; padding: 15px; border-radius: 8px; border-left: 4px solid #9C27B0; grid-column: 1 / -1;">
                         <strong>NLP Richness (15%)</strong><br>
                        <span style="color: #666; font-size: 0.9em;">
                            • Avg Words per Comment: 45%<br>
                            • Unique Word Ratio: 45%<br>
                            • Exclamation Ratio: 10%
                        </span>
                    </div>
                </div>
                <div style="background: rgba(103, 126, 234, 0.1); padding: 15px; border-radius: 8px; margin-top: 20px;">
                    <p style="color: #555; line-height: 1.8; margin: 0;">
                        <strong>🤖 Models có sẵn:</strong><br>
                        • <strong>XGBoost (Reduced):</strong> Gradient Boosting với 8 features (temporal + lag)<br>
                        • <strong>Random Forest:</strong> Bagging ensemble với 8 features (temporal + lag)<br>
                        <strong>Dự đoán:</strong> Recursive autoregressive strategy (12 tháng ahead)<br>
                        <strong>So sánh:</strong> Chọn model khác nhau để so sánh độ chính xác và kết quả dự báo
                    </p>
                </div>
            </div>
            """
        )
    
    return app


if __name__ == "__main__":
    app = create_interface()
    app.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
        show_error=True
    )