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
MODEL_NAME = "province_hotness_forecaster"

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


def load_forecast_data():
    """Load forecast data from MinIO Parquet files (latest run folder)"""
    try:
        # List all objects in forecast directory
        objects = MINIO_CLIENT.list_objects(BUCKET_NAME, prefix=FORECAST_PREFIX, recursive=True)
        
        # Get all parquet files with their folder paths
        parquet_files = []
        for obj in objects:
            if obj.object_name.endswith('.parquet'):
                parquet_files.append(obj.object_name)
        
        if not parquet_files:
            print("⚠️ No forecast files found!")
            return pd.DataFrame()
        
        print(f"📂 Found {len(parquet_files)} parquet files")
        
        # Extract unique run folders (format: ml_forecast/YYYYMMDD_HHMMSS/)
        run_folders = set()
        for file_path in parquet_files:
            # Extract folder name: ml_forecast/20241204_160932/part-xxx.parquet
            parts = file_path.split('/')
            if len(parts) >= 3:
                run_folder = f"{parts[0]}/{parts[1]}/"  # ml_forecast/20241204_160932/
                run_folders.add(run_folder)
        
        if not run_folders:
            print("⚠️ No valid run folders found!")
            return pd.DataFrame()
        
        # Sort folders by timestamp (descending) and get latest
        latest_run_folder = sorted(run_folders, reverse=True)[0]
        print(f"📁 Loading from latest run: {latest_run_folder}")
        
        # Load ALL parquet files from latest run folder
        df_list = []
        files_in_latest_run = [f for f in parquet_files if f.startswith(latest_run_folder)]
        
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
            print("⚠️ No data loaded from parquet files!")
            return pd.DataFrame()
        
        # Concatenate all dataframes
        df = pd.concat(df_list, ignore_index=True)
        
        print(f"✅ Total loaded: {len(df)} rows from {len(df_list)} files")
        print(f"   - Unique provinces: {df['province_name'].nunique()}")
        print(f"   - Unique regions: {df['region'].nunique()}")
        print(f"   - Date range: {df['forecast_date'].min()} to {df['forecast_date'].max()}")
        
        return df
        
    except S3Error as e:
        print(f"❌ MinIO error: {str(e)}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error loading forecast data: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    
def get_model_info():
    """Get latest model information from MLflow"""
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = mlflow.MlflowClient()
        
        # Get latest version
        model_versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        if model_versions:
            latest_version = max(model_versions, key=lambda x: int(x.version))
            run = client.get_run(latest_version.run_id)
            
            return {
                "version": latest_version.version,
                "run_id": latest_version.run_id,
                "test_rmse": run.data.metrics.get("test_rmse", "N/A"),
                "test_mae": run.data.metrics.get("test_mae", "N/A"),
                "test_r2": run.data.metrics.get("test_r2", "N/A"),
                "trained_at": datetime.fromtimestamp(run.info.start_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
            }
    except Exception as e:
        print(f"Error getting model info: {e}")
        return None


def recommend_provinces(start_month, end_month, region, top_n):
    """
    Recommend top provinces based on forecasted hotness
    
    Args:
        start_month: Starting horizon month (1-12)
        end_month: Ending horizon month (1-12)
        region: Region filter (North/Central/South or None)
        top_n: Number of top provinces to return
    
    Returns:
        DataFrame with recommendations and plotly figure
    """
    # Load forecast data
    df = load_forecast_data()
    
    if df.empty:
        return pd.DataFrame(), None, "⚠️ Không thể load dữ liệu forecast!"
    
    # Filter by horizon months
    df_filtered = df[(df['horizon_month'] >= start_month) & (df['horizon_month'] <= end_month)]
    
    # Filter by region
    if region != "Tất cả":
        region_code = REGIONS[region]
        df_filtered = df_filtered[df_filtered['region'] == region_code]
    
    if df_filtered.empty:
        return pd.DataFrame(), None, "⚠️ Không có dữ liệu phù hợp với bộ lọc!"
    
    # Calculate average hotness for each province
    province_avg = df_filtered.groupby(['province_sk', 'province_name', 'region']).agg({
        'predicted_hotness': 'mean'
    }).reset_index()
    
    province_avg = province_avg.sort_values('predicted_hotness', ascending=False).head(top_n)
    province_avg['rank'] = range(1, len(province_avg) + 1)
    province_avg['predicted_hotness'] = province_avg['predicted_hotness'].round(4)
    
    # Create result table
    result_df = province_avg[['rank', 'province_name', 'region', 'predicted_hotness']].copy()
    result_df.columns = ['Hạng', 'Tỉnh/Thành', 'Vùng', 'Hotness Score']
    
    # Create time series chart for top provinces
    top_provinces = province_avg['province_sk'].tolist()
    df_chart = df_filtered[df_filtered['province_sk'].isin(top_provinces)]
    
    # IMPORTANT: Sort by horizon_month to ensure correct chronological order
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
    
    # Convert year_month to datetime format for proper display
    df_chart['date'] = pd.to_datetime(df_chart['year_month'].astype(str), format='%Y%m')
    df_chart['month_label'] = df_chart['date'].dt.strftime('%m/%Y')
    
    # Sort by date to ensure correct plotting order
    df_chart = df_chart.sort_values('date')
    
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
            'text': f'📈 Xu hướng Hotness Score (Tháng {start_month}-{end_month} sau)',
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
    
    # Model info message with better formatting
    model_info = get_model_info()
    if model_info:
        info_msg = f"""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 12px; color: white; box-shadow: 0 4px 15px rgba(0,0,0,0.2);">
    <h3 style="margin-top: 0; font-size: 18px;">✅ Thông tin Model</h3>
    <p style="margin: 8px 0;"><strong>Model:</strong> {MODEL_NAME} <span style="background: rgba(255,255,255,0.3); padding: 3px 8px; border-radius: 5px;">v{model_info['version']}</span></p>
    <p style="margin: 8px 0;"><strong>📊 Độ chính xác:</strong> RMSE={model_info['test_rmse']:.4f} | MAE={model_info['test_mae']:.4f} | R²={model_info['test_r2']:.4f}</p>
    <p style="margin: 8px 0;"><strong>🕒 Trained:</strong> {model_info['trained_at']}</p>
    <p style="margin: 8px 0;"><strong>📈 Dữ liệu:</strong> {len(df_filtered)} predictions ({len(province_avg)} tỉnh × {end_month - start_month + 1} tháng)</p>
</div>
        """
    else:
        info_msg = f"""
<div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); padding: 20px; border-radius: 12px; color: white; box-shadow: 0 4px 15px rgba(0,0,0,0.2);">
    <p style="margin: 0;">✅ Đã tạo {len(province_avg)} recommendations từ {len(df_filtered)} predictions</p>
</div>
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
    .filter-card {
        background: white;
        padding: 25px;
        border-radius: 12px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        border: 1px solid #e5e7eb;
    }
    .result-card {
        background: white;
        padding: 25px;
        border-radius: 12px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        border: 1px solid #e5e7eb;
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
    
    with gr.Blocks(title="🏖️ Tourism Hotness Forecast", theme=gr.themes.Soft(), css=custom_css) as app:
        # Header
        gr.HTML(
            """
            <div class="header-section">
                <h1>🏖️ Hệ thống Dự báo Xu hướng Du lịch Việt Nam</h1>
                <p>🤖 Powered by <strong>XGBoost Time-Series Forecasting</strong> | 📊 Data from TikTok & Booking.com</p>
                <p style="font-size: 0.95em; margin-top: 10px;">💡 Dự đoán điểm đến hot nhất từ 1-12 tháng trong tương lai dựa trên AI và Big Data</p>
            </div>
            """
        )
        
        with gr.Row(equal_height=True):
            # Left panel - Filters
            with gr.Column(scale=1):
                gr.HTML('<div class="filter-card">')
                gr.Markdown("### ⚙️ Bộ lọc tìm kiếm")
                
                start_month = gr.Slider(
                    minimum=1, 
                    maximum=12, 
                    value=1, 
                    step=1,
                    label="📅 Tháng bắt đầu (horizon)",
                    info="Từ tháng thứ mấy tính từ hiện tại?"
                )
                
                end_month = gr.Slider(
                    minimum=1, 
                    maximum=12, 
                    value=3, 
                    step=1,
                    label="📅 Tháng kết thúc (horizon)",
                    info="Đến tháng thứ mấy tính từ hiện tại?"
                )
                
                region = gr.Dropdown(
                    choices=list(REGIONS.keys()),
                    value="Tất cả",
                    label="🗺️ Vùng miền",
                    info="Chọn khu vực du lịch yêu thích"
                )
                
                top_n = gr.Slider(
                    minimum=5,
                    maximum=20,
                    value=10,
                    step=1,
                    label="🏆 Số lượng Top N",
                    info="Hiển thị bao nhiêu điểm đến?"
                )
                
                search_btn = gr.Button("🔍 Tìm kiếm ngay", variant="primary", size="lg", scale=2)
                
                gr.HTML('</div>')
                
                # Info box
                info_msg = gr.HTML(
                    """
                    <div style="background: linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%); 
                                padding: 20px; border-radius: 12px; margin-top: 20px; 
                                box-shadow: 0 4px 15px rgba(0,0,0,0.1);">
                        <p style="margin: 0; color: #333; font-weight: 500;">
                            ℹ️ Nhấn <strong>'Tìm kiếm ngay'</strong> để xem kết quả dự báo
                        </p>
                    </div>
                    """
                )
            
            # Right panel - Results
            with gr.Column(scale=2):
                gr.HTML('<div class="result-card">')
                gr.Markdown("### 📊 Kết quả Dự báo")
                
                result_table = gr.Dataframe(
                    label="🏆 Top Tỉnh/Thành có Hotness Score cao nhất",
                    headers=["Hạng", "Tỉnh/Thành", "Vùng", "Hotness Score"],
                    datatype=["number", "str", "str", "number"],
                    wrap=True,
                    interactive=False,
                    row_count=10,
                    column_widths=["10%", "35%", "35%", "20%"]
                )
                
                result_chart = gr.Plot(label="📈 Biểu đồ xu hướng theo thời gian")
                
                gr.HTML('</div>')
        
        # Event handler
        search_btn.click(
            fn=recommend_provinces,
            inputs=[start_month, end_month, region, top_n],
            outputs=[result_table, result_chart, info_msg]
        )
        
        # Footer explanation
        gr.HTML(
            """
            <div style="background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%); 
                        padding: 30px; border-radius: 16px; margin-top: 30px; 
                        box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
                <h3 style="margin-top: 0; color: #333;">📖 Giải thích Hotness Score</h3>
                <p style="color: #555; line-height: 1.8; margin: 10px 0;">
                    <strong>Hotness Score (0-1)</strong> được tính từ 14 metrics quan trọng:
                </p>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 20px 0;">
                    <div style="background: white; padding: 15px; border-radius: 8px;">
                        📝 <strong>Volume (20%)</strong><br>
                        <span style="color: #666; font-size: 0.9em;">Số lượng comments, posts</span>
                    </div>
                    <div style="background: white; padding: 15px; border-radius: 8px;">
                        👍 <strong>Post Engagement (25%)</strong><br>
                        <span style="color: #666; font-size: 0.9em;">Likes, shares, saves</span>
                    </div>
                    <div style="background: white; padding: 15px; border-radius: 8px;">
                        💬 <strong>Comment Engagement (10%)</strong><br>
                        <span style="color: #666; font-size: 0.9em;">Comment likes</span>
                    </div>
                    <div style="background: white; padding: 15px; border-radius: 8px;">
                        😊 <strong>Sentiment (30%)</strong><br>
                        <span style="color: #666; font-size: 0.9em;">Tích cực, tiêu cực, trung bình, emoji</span>
                    </div>
                    <div style="background: white; padding: 15px; border-radius: 8px; grid-column: 1 / -1;">
                        📚 <strong>NLP Richness (15%)</strong><br>
                        <span style="color: #666; font-size: 0.9em;">Độ dài, từ vựng, cảm xúc</span>
                    </div>
                </div>
                <p style="color: #555; line-height: 1.8; margin: 15px 0;">
                    <strong>🤖 Mô hình:</strong> XGBoost Regressor với 12 features (temporal + lag + current metrics)<br>
                    <strong>🔮 Forecast Strategy:</strong> Recursive autoregressive (12 tháng ahead)
                </p>
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