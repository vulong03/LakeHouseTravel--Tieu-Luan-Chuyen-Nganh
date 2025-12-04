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
    """Load forecast data from MinIO Parquet files"""
    try:
        # List all parquet files in the forecast folder
        objects = MINIO_CLIENT.list_objects(BUCKET_NAME, prefix=FORECAST_PREFIX, recursive=True)
        
        # Find the latest parquet file
        parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
        
        if not parquet_files:
            print("No forecast parquet files found in MinIO")
            return pd.DataFrame()
        
        # Sort by filename (timestamp in filename) and get the latest
        latest_file = sorted(parquet_files)[-1]
        print(f"Loading forecast data from: {latest_file}")
        
        # Download the parquet file
        response = MINIO_CLIENT.get_object(BUCKET_NAME, latest_file)
        parquet_data = response.read()
        response.close()
        response.release_conn()
        
        # Read parquet data into pandas DataFrame
        df = pd.read_parquet(BytesIO(parquet_data))
        
        print(f"✓ Loaded {len(df)} forecast records from MinIO")
        return df
        
    except S3Error as e:
        print(f"MinIO S3 Error loading forecast data: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error loading forecast data: {e}")
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
    
    # Create interactive line chart
    fig = go.Figure()
    
    colors = px.colors.qualitative.Set2
    
    for idx, (province_sk, province_name) in enumerate(zip(province_avg['province_sk'], province_avg['province_name'])):
        province_data = df_chart[df_chart['province_sk'] == province_sk].sort_values('horizon_month')
        
        fig.add_trace(go.Scatter(
            x=province_data['year_month'],
            y=province_data['predicted_hotness'],
            mode='lines+markers',
            name=province_name,
            line=dict(color=colors[idx % len(colors)], width=2),
            marker=dict(size=8),
            hovertemplate=f'<b>{province_name}</b><br>' +
                         'Tháng: %{x}<br>' +
                         'Hotness: %{y:.4f}<br>' +
                         '<extra></extra>'
        ))
    
    fig.update_layout(
        title=f'Xu hướng Hotness Score (Tháng {start_month}-{end_month} sau)',
        xaxis_title='Tháng',
        yaxis_title='Hotness Score',
        hovermode='x unified',
        template='plotly_white',
        height=500,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        )
    )
    
    # Add range selector
    fig.update_xaxes(
        rangeslider_visible=False,
        rangeselector=dict(
            buttons=list([
                dict(count=3, label="3 tháng", step="month", stepmode="backward"),
                dict(count=6, label="6 tháng", step="month", stepmode="backward"),
                dict(step="all", label="Tất cả")
            ])
        )
    )
    
    # Model info message
    model_info = get_model_info()
    if model_info:
        info_msg = f"""
✅ **Dự báo từ Model:** {MODEL_NAME} v{model_info['version']}
📊 **Độ chính xác:** RMSE={model_info['test_rmse']:.4f}, MAE={model_info['test_mae']:.4f}, R²={model_info['test_r2']:.4f}
🕒 **Trained:** {model_info['trained_at']}
📈 **Dữ liệu:** {len(df_filtered)} predictions ({len(province_avg)} tỉnh × {end_month - start_month + 1} tháng)
        """
    else:
        info_msg = f"✅ Đã tạo {len(province_avg)} recommendations từ {len(df_filtered)} predictions"
    
    return result_df, fig, info_msg


def create_interface():
    """Create Gradio interface"""
    
    with gr.Blocks(title="Province Tourism Hotness Forecasting", theme=gr.themes.Soft()) as app:
        gr.Markdown(
            """
            # 🏖️ Hệ thống Dự báo Xu hướng Du lịch Tỉnh thành
            
            Dựa trên mô hình **XGBoost Time-Series Forecasting** với dữ liệu TikTok & Booking.com
            
            ### 📌 Cách sử dụng:
            1. Chọn **khoảng thời gian** bạn muốn đi du lịch (1-12 tháng sau)
            2. Chọn **vùng miền** (hoặc để "Tất cả")
            3. Chọn số lượng **Top N** tỉnh thành muốn xem
            4. Nhấn **"Tìm kiếm"** để xem kết quả!
            """
        )
        
        with gr.Row():
            with gr.Column(scale=1):
                gr.Markdown("### ⚙️ Bộ lọc")
                
                start_month = gr.Slider(
                    minimum=1, 
                    maximum=12, 
                    value=1, 
                    step=1,
                    label="Tháng bắt đầu (horizon)",
                    info="Tháng thứ mấy tính từ hiện tại"
                )
                
                end_month = gr.Slider(
                    minimum=1, 
                    maximum=12, 
                    value=3, 
                    step=1,
                    label="Tháng kết thúc (horizon)",
                    info="Tháng thứ mấy tính từ hiện tại"
                )
                
                region = gr.Dropdown(
                    choices=list(REGIONS.keys()),
                    value="Tất cả",
                    label="Vùng miền",
                    info="Chọn khu vực du lịch"
                )
                
                top_n = gr.Slider(
                    minimum=5,
                    maximum=20,
                    value=10,
                    step=1,
                    label="Số lượng Top N",
                    info="Hiển thị bao nhiêu tỉnh thành"
                )
                
                search_btn = gr.Button("🔍 Tìm kiếm", variant="primary", size="lg")
                
                info_msg = gr.Markdown("ℹ️ Nhấn 'Tìm kiếm' để xem kết quả")
            
            with gr.Column(scale=2):
                gr.Markdown("### 📊 Kết quả Dự báo")
                
                result_table = gr.Dataframe(
                    label="Top N Tỉnh/Thành có Hotness cao nhất",
                    headers=["Hạng", "Tỉnh/Thành", "Vùng", "Hotness Score"],
                    datatype=["number", "str", "str", "number"],
                    wrap=True
                )
                
                result_chart = gr.Plot(label="Xu hướng Hotness theo thời gian")
        
        # Event handler
        search_btn.click(
            fn=recommend_provinces,
            inputs=[start_month, end_month, region, top_n],
            outputs=[result_table, result_chart, info_msg]
        )
        
        gr.Markdown(
            """
            ---
            ### 📖 Giải thích Hotness Score
            
            **Hotness Score** (0-1) được tính từ 14 metrics:
            - 📝 **Volume** (20%): Số lượng comments, posts
            - 👍 **Post Engagement** (25%): Likes, shares, saves
            - 💬 **Comment Engagement** (10%): Comment likes
            - 😊 **Sentiment** (30%): Tích cực, tiêu cực, trung bình, emoji
            - 📚 **NLP Richness** (15%): Độ dài, từ vựng, cảm xúc
            
            **Mô hình:** XGBoost Regressor với 12 features (temporal + lag + current metrics)
            
            **Forecast Strategy:** Recursive autoregressive (12 tháng ahead)
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
