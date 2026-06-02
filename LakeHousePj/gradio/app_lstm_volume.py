"""
Tourism Trend Forecasting — LSTM Hotel Volume Dashboard
========================================================

Giao diện Gradio cho mô hình LSTM v3 dự báo lượng đặt phòng (hotel_review_volume).
Target: hotel_review_volume (log-normalized, expm1 để hiển thị số thực)

Tabs:
  Tab 1: Dự báo 12 tháng tới — top tỉnh theo lượng đặt phòng dự báo
  Tab 2: So sánh tỉnh — biểu đồ xu hướng dự báo theo thời gian
  Tab 3: Phân tích Traveler Type — couple/family/business/solo ratio theo tỉnh & tháng
  Tab 4: Thông tin Model & Metrics

Data source: MinIO s3a://gold/ml_forecast/province_hotel_volume_forecast_lstm_*
Model registry: MLflow — province_hotel_volume_forecaster_lstm
"""

import gradio as gr
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import mlflow
import os
import time
from io import BytesIO
from minio import Minio
from minio.error import S3Error

# ============================================================
# Configuration
# ============================================================

MINIO_CLIENT = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False
)
BUCKET_NAME = "gold"
FORECAST_PREFIX = "ml_forecast/"
FEATURES_PREFIX = "ml_training/"          # dl_features.parquet (for traveler type tab)
LSTM_FILE_PATTERN = "province_hotel_volume_forecast_lstm_"

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
LSTM_MODEL_NAME = "province_hotel_volume_forecaster_lstm"

CACHE_DURATION = 60   # seconds
_forecast_cache = {"data": None, "timestamp": 0, "run_folder": ""}
_features_cache = {"data": None, "timestamp": 0}

REGION_MAP_VI = {
    "Northeast":           "Đông Bắc",
    "Northwest":           "Tây Bắc",
    "Red_River_Delta":     "Đồng bằng Sông Hồng",
    "North_Central_Coast": "Bắc Trung Bộ",
    "Central_Highlands":   "Tây Nguyên",
    "South_Central_Coast": "Duyên hải Nam Trung Bộ",
    "Southeast":           "Đông Nam Bộ",
    "Mekong_Delta":        "Đồng bằng Sông Cửu Long",
}
REGIONS_VI = {"Tất cả": None, **{v: k for k, v in REGION_MAP_VI.items()}}

MONTH_CHOICES = [
    "10/2025", "11/2025", "12/2025",
    "01/2026", "02/2026", "03/2026",
    "04/2026", "05/2026", "06/2026",
    "07/2026", "08/2026", "09/2026",
]

def _ym_int(month_str: str) -> int:
    """Convert 'MM/YYYY' → YYYYMM int."""
    m, y = month_str.split("/")
    return int(y) * 100 + int(m)

def _ym_label(ym_int: int) -> str:
    return f"{ym_int % 100:02d}/{ym_int // 100}"


# ============================================================
# Data Loading
# ============================================================

def load_forecast_data():
    """Load latest LSTM forecast parquet from MinIO, with caching."""
    global _forecast_cache
    now = time.time()
    if _forecast_cache["data"] is not None and (now - _forecast_cache["timestamp"]) < CACHE_DURATION:
        return _forecast_cache["data"], None

    try:
        objects = list(MINIO_CLIENT.list_objects(BUCKET_NAME, prefix=FORECAST_PREFIX, recursive=True))
        folder_ts, files_by_folder = {}, {}

        for obj in objects:
            if obj.object_name.endswith(".parquet") and LSTM_FILE_PATTERN in obj.object_name:
                parts = obj.object_name.split("/")
                if len(parts) >= 3:
                    folder = f"{parts[0]}/{parts[1]}/"
                    if folder not in folder_ts or obj.last_modified > folder_ts[folder]:
                        folder_ts[folder] = obj.last_modified
                    files_by_folder.setdefault(folder, []).append(obj.object_name)

        if not folder_ts:
            return pd.DataFrame(), "⚠️ Không tìm thấy file dự báo LSTM trong MinIO. Hãy chạy train_lstm_forecast.py trước!"

        latest = max(folder_ts, key=folder_ts.get)
        dfs = []
        for fpath in files_by_folder[latest]:
            resp = MINIO_CLIENT.get_object(BUCKET_NAME, fpath)
            dfs.append(pd.read_parquet(BytesIO(resp.read())))

        df = pd.concat(dfs, ignore_index=True)
        df["region_vi"] = df["region"].map(REGION_MAP_VI).fillna(df["region"])
        df["date"] = pd.to_datetime(df["year_month"].astype(str), format="%Y%m")
        df["month_label"] = df["date"].dt.strftime("%m/%Y")

        _forecast_cache.update({"data": df, "timestamp": now, "run_folder": latest})
        print(f"✅ Loaded {len(df)} LSTM forecast rows from {latest}")
        return df, None

    except S3Error as e:
        return pd.DataFrame(), f"❌ Lỗi kết nối MinIO: {e}"
    except Exception as e:
        import traceback; traceback.print_exc()
        return pd.DataFrame(), f"❌ Lỗi load dữ liệu: {e}"


def load_features_data():
    """Load dl_features parquet (for traveler type analysis) from MinIO."""
    global _features_cache
    now = time.time()
    if _features_cache["data"] is not None and (now - _features_cache["timestamp"]) < CACHE_DURATION:
        return _features_cache["data"], None

    try:
        objects = list(MINIO_CLIENT.list_objects(BUCKET_NAME, prefix=FEATURES_PREFIX, recursive=True))
        parquet_files = [o.object_name for o in objects if o.object_name.endswith(".parquet")]
        if not parquet_files:
            return pd.DataFrame(), "⚠️ Không tìm thấy dl_features.parquet trong MinIO."

        dfs = []
        for fpath in parquet_files:
            resp = MINIO_CLIENT.get_object(BUCKET_NAME, fpath)
            dfs.append(pd.read_parquet(BytesIO(resp.read())))

        df = pd.concat(dfs, ignore_index=True)
        df["region_vi"] = df["region"].map(REGION_MAP_VI).fillna(df.get("region", ""))
        _features_cache.update({"data": df, "timestamp": now})
        print(f"✅ Loaded {len(df)} feature rows for traveler analysis")
        return df, None

    except Exception as e:
        import traceback; traceback.print_exc()
        return pd.DataFrame(), f"❌ Lỗi load features: {e}"


def get_model_info():
    """Fetch LSTM model info from MLflow registry."""
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = mlflow.MlflowClient()
        versions = client.search_model_versions(f"name='{LSTM_MODEL_NAME}'")
        if not versions:
            return None
        latest = max(versions, key=lambda v: int(v.version))
        run = client.get_run(latest.run_id)
        m = run.data.metrics
        p = run.data.params
        return {
            "version": latest.version,
            "run_id": latest.run_id,
            "trained_at": datetime.fromtimestamp(run.info.start_time / 1000).strftime("%Y-%m-%d %H:%M"),
            "train_rmse": m.get("train_rmse"), "train_r2": m.get("train_r2"),
            "test_rmse":  m.get("test_rmse"),  "test_r2":  m.get("test_r2"),
            "test_mae":   m.get("test_mae"),    "test_mape_actual": m.get("test_mape_actual"),
            "best_val_loss": m.get("best_val_loss"),
            "epochs_trained": m.get("epochs_trained"),
            "num_features": p.get("num_features"),
            "sequence_length": p.get("sequence_length"),
            "hidden_size": p.get("hidden_size"),
        }
    except Exception as e:
        print(f"MLflow error: {e}")
        return None


# ============================================================
# Tab 1 — Top Province Forecast
# ============================================================

def tab1_top_provinces(start_month, end_month, region_vi, top_n, sort_by):
    df, err = load_forecast_data()
    if err:
        return pd.DataFrame(), None, _error_html(err)
    if df.empty:
        return pd.DataFrame(), None, _warn_html("Không có dữ liệu dự báo.")

    s_ym, e_ym = _ym_int(start_month), _ym_int(end_month)
    if s_ym > e_ym:
        return pd.DataFrame(), None, _error_html("Tháng bắt đầu phải ≤ tháng kết thúc!")

    fdf = df[(df["year_month"] >= s_ym) & (df["year_month"] <= e_ym)].copy()
    if region_vi != "Tất cả":
        fdf = fdf[fdf["region_vi"] == region_vi]

    if fdf.empty:
        return pd.DataFrame(), None, _warn_html("Không có dữ liệu cho khoảng thời gian/vùng đã chọn.")

    # Aggregate
    agg = fdf.groupby(["province_sk", "province_name", "region_vi"]).agg(
        avg_volume=("predicted_hotel_volume_actual", "mean"),
        total_volume=("predicted_hotel_volume_actual", "sum"),
        avg_growth=("predicted_growth_pct", "mean"),
        peak_month=("predicted_hotel_volume_actual", "idxmax"),
    ).reset_index()

    sort_col = "avg_volume" if sort_by == "Lượng đặt phòng TB/tháng" else "avg_growth"
    agg = agg.sort_values(sort_col, ascending=False).head(int(top_n)).reset_index(drop=True)
    agg["rank"] = range(1, len(agg) + 1)

    # Map peak month index → month label
    peak_labels = []
    for idx_val in agg["peak_month"]:
        try:
            peak_labels.append(fdf.loc[idx_val, "month_label"])
        except Exception:
            peak_labels.append("—")
    agg["peak_month_label"] = peak_labels

    result_df = agg[["rank", "province_name", "region_vi",
                      "avg_volume", "total_volume", "avg_growth", "peak_month_label"]].copy()
    result_df.columns = ["#", "Tỉnh/Thành", "Vùng",
                         "TB Lượt/Tháng", "Tổng Lượt (12T)", "Tăng trưởng TB %", "Tháng Đỉnh"]
    result_df["TB Lượt/Tháng"] = result_df["TB Lượt/Tháng"].round(0).astype(int)
    result_df["Tổng Lượt (12T)"] = result_df["Tổng Lượt (12T)"].round(0).astype(int)
    result_df["Tăng trưởng TB %"] = result_df["Tăng trưởng TB %"].round(1)

    # Bar chart
    colors = px.colors.qualitative.Vivid
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=agg["province_name"],
        y=agg["avg_volume"],
        marker=dict(
            color=agg["avg_volume"],
            colorscale="Teal",
            showscale=True,
            colorbar=dict(title="Lượt/tháng"),
        ),
        text=agg["avg_volume"].round(0).astype(int),
        textposition="outside",
        hovertemplate=(
            "<b>%{x}</b><br>"
            "TB Lượt/Tháng: %{y:,.0f}<br>"
            "<extra></extra>"
        ),
    ))
    fig.update_layout(
        title={"text": f"🏆 Top {len(agg)} Tỉnh — Lượt Đặt Phòng Dự Báo ({start_month}→{end_month})",
               "x": 0.5, "xanchor": "center", "font": {"size": 18}},
        xaxis=dict(title="", tickangle=-35),
        yaxis=dict(title="Lượt review/tháng (dự báo)"),
        template="plotly_white",
        height=480,
        plot_bgcolor="rgba(248,250,255,0.8)",
    )

    n_prov = df["province_sk"].nunique()
    info = _info_html(
        f"Đã lọc {n_prov} tỉnh · {fdf['year_month'].nunique()} tháng · "
        f"Hiển thị top {len(agg)} ({sort_by})"
    )
    return result_df, fig, info


# ============================================================
# Tab 2 — Province Comparison (time series)
# ============================================================

def tab2_compare(province_list, metric):
    df, err = load_forecast_data()
    if err:
        return None, _error_html(err)
    if df.empty or not province_list:
        return None, _warn_html("Chưa chọn tỉnh nào.")

    fdf = df[df["province_name"].isin(province_list)].copy()
    fdf = fdf.sort_values(["province_name", "date"])

    col = "predicted_hotel_volume_actual" if metric == "Lượt đặt phòng (actual)" else "predicted_growth_pct"
    ylab = "Lượt review (dự báo)" if metric == "Lượt đặt phòng (actual)" else "Tăng trưởng % so với tháng trước"

    fig = go.Figure()
    palette = px.colors.qualitative.Bold
    for i, prov in enumerate(province_list):
        pdata = fdf[fdf["province_name"] == prov].sort_values("date")
        fig.add_trace(go.Scatter(
            x=pdata["date"],
            y=pdata[col],
            mode="lines+markers",
            name=prov,
            line=dict(width=3, color=palette[i % len(palette)]),
            marker=dict(size=9, symbol="circle"),
            hovertemplate=(
                f"<b>{prov}</b><br>"
                "Tháng: %{x|%m/%Y}<br>"
                f"{ylab}: %{{y:,.1f}}<br>"
                "<extra></extra>"
            )
        ))

    fig.update_layout(
        title={"text": f"📈 So sánh Dự báo: {metric}", "x": 0.5, "xanchor": "center", "font": {"size": 18}},
        xaxis=dict(title="Thời gian", tickformat="%m/%Y", dtick="M1", tickangle=-40),
        yaxis=dict(title=ylab),
        hovermode="x unified",
        template="plotly_white",
        height=520,
        legend=dict(orientation="v", xanchor="left", x=1.02, yanchor="top", y=0.98,
                    bgcolor="rgba(255,255,255,0.9)", bordercolor="#ddd", borderwidth=1),
        margin=dict(r=200),
        plot_bgcolor="rgba(248,250,255,0.8)",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(200,220,240,0.5)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(200,220,240,0.5)")

    info = _info_html(f"So sánh {len(province_list)} tỉnh · 12 tháng dự báo · LSTM v3")
    return fig, info


# ============================================================
# Tab 3 — Traveler Type Analysis
# ============================================================

def tab3_traveler(province_name, year_filter):
    df_feat, err = load_features_data()
    if err:
        return None, None, _error_html(err)
    if df_feat.empty:
        return None, None, _warn_html("Không có dữ liệu đặc trưng.")

    fdf = df_feat[df_feat["province_name"] == province_name].copy()
    if year_filter != "Tất cả":
        fdf = fdf[fdf["year"] == int(year_filter)]

    if fdf.empty:
        return None, None, _warn_html(f"Không có dữ liệu cho {province_name}.")

    fdf = fdf.sort_values("year_month")
    fdf["date"] = pd.to_datetime(fdf["year_month"].astype(str), format="%Y%m")

    # --- Chart 1: Stacked area traveler type ---
    fig1 = go.Figure()
    colors_map = {"couple_ratio": "#4ECDC4", "family_ratio": "#FF6B6B",
                  "business_ratio": "#45B7D1", "solo_ratio": "#FFA07A"}
    labels_map = {"couple_ratio": "Cặp đôi", "family_ratio": "Gia đình",
                  "business_ratio": "Công tác", "solo_ratio": "Một mình"}
    for col in ["couple_ratio", "family_ratio", "business_ratio", "solo_ratio"]:
        fig1.add_trace(go.Scatter(
            x=fdf["date"],
            y=(fdf[col] * 100).round(1),
            mode="lines",
            name=labels_map[col],
            stackgroup="one",
            line=dict(width=0.5, color=colors_map[col]),
            fillcolor=colors_map[col],
            hovertemplate=f"<b>{labels_map[col]}</b>: %{{y:.1f}}%<br>Tháng: %{{x|%m/%Y}}<extra></extra>",
        ))

    fig1.update_layout(
        title={"text": f"👥 Phân bổ Loại Du khách — {province_name}", "x": 0.5, "xanchor": "center"},
        xaxis=dict(tickformat="%m/%Y", dtick="M1", tickangle=-40),
        yaxis=dict(title="Tỷ lệ (%)", range=[0, 100]),
        hovermode="x unified",
        template="plotly_white",
        height=400,
        legend=dict(orientation="h", y=-0.2),
        plot_bgcolor="rgba(248,250,255,0.8)",
    )

    # --- Chart 2: Hotel review volume bar ---
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(
        x=fdf["date"],
        y=fdf["hotel_review_volume"],
        marker=dict(color=fdf["hotel_review_volume"], colorscale="Blues", showscale=False),
        name="Lượt review",
        hovertemplate="Tháng: %{x|%m/%Y}<br>Lượt review: %{y:,}<extra></extra>",
    ))
    if "hotness_score" in fdf.columns:
        fig2.add_trace(go.Scatter(
            x=fdf["date"],
            y=fdf["hotness_score"] * fdf["hotel_review_volume"].max(),
            mode="lines+markers",
            name="Hotness (scaled)",
            yaxis="y2",
            line=dict(color="#FF6B6B", width=2, dash="dot"),
            marker=dict(size=6),
            hovertemplate="Hotness Score: %{customdata:.3f}<extra></extra>",
            customdata=fdf["hotness_score"],
        ))

    fig2.update_layout(
        title={"text": f"📊 Lượt Đặt Phòng Thực tế & Hotness — {province_name}", "x": 0.5, "xanchor": "center"},
        xaxis=dict(tickformat="%m/%Y", dtick="M1", tickangle=-40),
        yaxis=dict(title="Lượt review/tháng"),
        yaxis2=dict(title="Hotness Score", overlaying="y", side="right", showgrid=False),
        template="plotly_white",
        height=400,
        legend=dict(orientation="h", y=-0.2),
        plot_bgcolor="rgba(248,250,255,0.8)",
    )

    # Summary stats
    summary = fdf[["couple_ratio", "family_ratio", "business_ratio", "solo_ratio"]].mean() * 100
    info = f"""
<div style="background: linear-gradient(135deg, #4ECDC4, #45B7D1); padding: 18px; border-radius: 12px; color: white;">
  <h3 style="margin:0 0 10px">📍 {province_name} — Tổng kết</h3>
  <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; text-align: center;">
    <div><div style="font-size:1.8em; font-weight:800">{summary['couple_ratio']:.1f}%</div><div>Cặp đôi</div></div>
    <div><div style="font-size:1.8em; font-weight:800">{summary['family_ratio']:.1f}%</div><div>Gia đình</div></div>
    <div><div style="font-size:1.8em; font-weight:800">{summary['business_ratio']:.1f}%</div><div>Công tác</div></div>
    <div><div style="font-size:1.8em; font-weight:800">{summary['solo_ratio']:.1f}%</div><div>Một mình</div></div>
  </div>
  <p style="margin:10px 0 0; font-size:0.9em; opacity:0.9">{len(fdf)} tháng dữ liệu | Avg review/tháng: {fdf['hotel_review_volume'].mean():.0f}</p>
</div>
"""
    return fig1, fig2, info


# ============================================================
# Tab 4 — Model Info
# ============================================================

def tab4_model_info():
    df, err = load_forecast_data()
    info = get_model_info()

    forecast_stats = ""
    if not err and not df.empty:
        top10 = (df.groupby("province_name")["predicted_hotel_volume_actual"]
                   .mean().sort_values(ascending=False).head(10))
        forecast_stats = "".join(
            f'<tr><td style="padding:6px 14px">{i+1}</td>'
            f'<td style="padding:6px 14px"><b>{name}</b></td>'
            f'<td style="padding:6px 14px; text-align:right">{val:,.0f}</td></tr>'
            for i, (name, val) in enumerate(top10.items())
        )

    if info:
        def _fmt(v):
            return f"{v:.4f}" if v is not None else "—"

        html = f"""
<div style="font-family: 'Inter', sans-serif; max-width: 900px; margin: 0 auto;">

  <!-- Model Header -->
  <div style="background: linear-gradient(135deg, #667eea, #764ba2); border-radius: 16px; padding: 28px; color: white; margin-bottom: 20px; box-shadow: 0 8px 30px rgba(102,126,234,0.35);">
    <h2 style="margin: 0 0 8px">🧠 LSTM Deep Learning Model</h2>
    <p style="margin:0; opacity:0.9; font-size:1.1em">province_hotel_volume_forecaster_lstm · Version {info['version']}</p>
    <p style="margin:8px 0 0; opacity:0.75; font-size:0.9em">Trained: {info['trained_at']} · Run ID: {info['run_id'][:8]}…</p>
  </div>

  <!-- Metrics grid -->
  <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; margin-bottom: 20px;">
    <div style="background:#f0fdf4; border-left:4px solid #22c55e; border-radius:10px; padding:18px;">
      <div style="font-size:0.85em; color:#666; margin-bottom:6px;">TRAIN SET</div>
      <div style="font-size:1.4em; font-weight:700; color:#15803d">R² = {_fmt(info['train_r2'])}</div>
      <div style="color:#444; margin-top:4px">RMSE = {_fmt(info['train_rmse'])}</div>
    </div>
    <div style="background:#eff6ff; border-left:4px solid #3b82f6; border-radius:10px; padding:18px;">
      <div style="font-size:0.85em; color:#666; margin-bottom:6px;">TEST SET (hold-out 30%)</div>
      <div style="font-size:1.4em; font-weight:700; color:#1d4ed8">R² = {_fmt(info['test_r2'])}</div>
      <div style="color:#444; margin-top:4px">RMSE = {_fmt(info['test_rmse'])} · MAE = {_fmt(info['test_mae'])}</div>
    </div>
  </div>

  <!-- Architecture -->
  <div style="background:#fafafa; border:1px solid #e5e7eb; border-radius:12px; padding:20px; margin-bottom:20px;">
    <h3 style="margin:0 0 14px; color:#374151">⚙️ Kiến trúc & Hyperparameters</h3>
    <table style="width:100%; border-collapse:collapse; font-size:0.95em;">
      <tr style="background:#f3f4f6"><td style="padding:8px 14px; font-weight:600">Kiến trúc</td><td style="padding:8px 14px">LSTM + LayerNorm + Temporal Attention + FC(16→1)</td></tr>
      <tr><td style="padding:8px 14px; font-weight:600">Target Variable</td><td style="padding:8px 14px">hotel_review_volume (log1p normalized, expm1 output)</td></tr>
      <tr style="background:#f3f4f6"><td style="padding:8px 14px; font-weight:600">Số features</td><td style="padding:8px 14px">{info.get('num_features', '36')} (temporal + lag + volume + NLP + hotel)</td></tr>
      <tr><td style="padding:8px 14px; font-weight:600">Sequence length</td><td style="padding:8px 14px">{info.get('sequence_length', '4')} tháng</td></tr>
      <tr style="background:#f3f4f6"><td style="padding:8px 14px; font-weight:600">Hidden size</td><td style="padding:8px 14px">{info.get('hidden_size', '32')}</td></tr>
      <tr><td style="padding:8px 14px; font-weight:600">Loss function</td><td style="padding:8px 14px">HuberLoss (delta=0.5) — robust to outliers</td></tr>
      <tr style="background:#f3f4f6"><td style="padding:8px 14px; font-weight:600">Epochs trained</td><td style="padding:8px 14px">{info.get('epochs_trained', '—')} (early stopping patience=20)</td></tr>
      <tr><td style="padding:8px 14px; font-weight:600">Best val loss</td><td style="padding:8px 14px">{_fmt(info.get('best_val_loss'))}</td></tr>
    </table>
  </div>

  <!-- Top 10 provinces -->
  <div style="background:#fafafa; border:1px solid #e5e7eb; border-radius:12px; padding:20px;">
    <h3 style="margin:0 0 14px; color:#374151">🏆 Top 10 Tỉnh — Lượt Đặt Phòng Dự Báo TB/Tháng</h3>
    <table style="width:100%; border-collapse:collapse; font-size:0.95em;">
      <thead>
        <tr style="background:#667eea; color:white;">
          <th style="padding:8px 14px; text-align:left">#</th>
          <th style="padding:8px 14px; text-align:left">Tỉnh/Thành</th>
          <th style="padding:8px 14px; text-align:right">TB Lượt/Tháng</th>
        </tr>
      </thead>
      <tbody>{forecast_stats}</tbody>
    </table>
  </div>
</div>
"""
    else:
        html = _warn_html("Không thể kết nối MLflow để lấy thông tin model. Kiểm tra MLflow server.")

    return html


# ============================================================
# Helper HTML builders
# ============================================================

def _error_html(msg):
    return f'<div style="background:#fee2e2;border-left:4px solid #ef4444;padding:16px;border-radius:8px;color:#7f1d1d"><b>❌ Lỗi</b><br>{msg}</div>'

def _warn_html(msg):
    return f'<div style="background:#fef9c3;border-left:4px solid #eab308;padding:16px;border-radius:8px;color:#713f12"><b>⚠️ Cảnh báo</b><br>{msg}</div>'

def _info_html(msg):
    return f'<div style="background:linear-gradient(135deg,#667eea,#764ba2);padding:14px 20px;border-radius:10px;color:white;font-weight:500">ℹ️ {msg}</div>'


# ============================================================
# Dynamic dropdowns helpers
# ============================================================

def _get_provinces():
    df, _ = load_forecast_data()
    if df is None or df.empty:
        return []
    names = [str(x) for x in df["province_name"].dropna().unique() if x is not None and str(x).strip() != ""]
    return sorted(names)

def _get_feature_provinces():
    df, _ = load_features_data()
    if df is None or df.empty:
        return []
    names = [str(x) for x in df["province_name"].dropna().unique() if x is not None and str(x).strip() != ""]
    return sorted(names)

def _get_feature_years():
    df, _ = load_features_data()
    if df is None or df.empty:
        return ["Tất cả"]
    years = [str(int(float(x))) for x in df["year"].dropna().unique() if x is not None]
    return ["Tất cả"] + sorted(list(set(years)), reverse=True)


# ============================================================
# Gradio Interface
# ============================================================

def create_app():
    provinces = _get_provinces()
    feat_provinces = _get_feature_provinces()
    feat_years = _get_feature_years()

    css = """
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    * { font-family: 'Inter', sans-serif !important; }
    .gradio-container { max-width: 1500px !important; }
    .header-hero {
        background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 40%, #134e80 100%);
        border-radius: 20px;
        padding: 48px 40px;
        color: white;
        text-align: center;
        margin-bottom: 24px;
        position: relative;
        overflow: hidden;
    }
    .header-hero::before {
        content: '';
        position: absolute; inset: 0;
        background: radial-gradient(ellipse at 70% 50%, rgba(99,179,237,0.15) 0%, transparent 70%);
    }
    .header-hero h1 { font-size: 2.6em; font-weight: 800; margin: 0 0 8px; letter-spacing: -0.5px; }
    .header-hero .sub { font-size: 1.1em; opacity: 0.8; margin: 0; }
    .header-hero .badges { margin-top: 18px; display: flex; gap: 10px; justify-content: center; flex-wrap: wrap; }
    .header-hero .badge {
        background: rgba(255,255,255,0.15); border: 1px solid rgba(255,255,255,0.3);
        padding: 5px 14px; border-radius: 20px; font-size: 0.85em; font-weight: 600;
        backdrop-filter: blur(4px);
    }
    .filter-panel { background: #f8fafc; border-radius: 14px; padding: 20px; border: 1px solid #e2e8f0; }
    button.primary { background: linear-gradient(135deg, #667eea, #764ba2) !important; border: none !important; }
    """

    with gr.Blocks(title="🏖️ LSTM Tourism Forecast — Vietnam", theme=gr.themes.Soft(), css=css) as app:

        # ─── Hero Header ───────────────────────────────────────────────────
        gr.HTML("""
        <div class="header-hero">
            <h1>🏖️ VIETNAM TOURISM FORECAST</h1>
            <p class="sub">Dự báo Lượt Đặt Phòng Khách sạn 12 tháng tới · Powered by LSTM Deep Learning</p>
            <div class="badges">
                <span class="badge">🧠 LSTM + Attention</span>
                <span class="badge">📊 1.55M Reviews</span>
                <span class="badge">🗓️ 62 Tỉnh · 12 Tháng</span>
                <span class="badge">🎯 R² = 0.81 (Test)</span>
                <span class="badge">⚡ Iceberg Lakehouse</span>
            </div>
        </div>
        """)

        with gr.Tabs():

            # ══════════════════════════════════════════════
            # TAB 1: TOP PROVINCES
            # ══════════════════════════════════════════════
            with gr.Tab("🏆 Top Tỉnh Dự Báo"):
                with gr.Row():
                    with gr.Column(scale=1, min_width=280):
                        gr.Markdown("### ⚙️ Bộ lọc")
                        with gr.Group(elem_classes="filter-panel"):
                            start_m = gr.Dropdown(MONTH_CHOICES, value="10/2025", label="Tháng bắt đầu")
                            end_m   = gr.Dropdown(MONTH_CHOICES, value="09/2026", label="Tháng kết thúc")
                            region_dd = gr.Dropdown(list(REGIONS_VI.keys()), value="Tất cả", label="Vùng địa lý")
                            top_n_dd  = gr.Dropdown(["5", "10", "15", "20", "30"], value="10", label="Hiển thị Top N")
                            sort_dd   = gr.Dropdown(
                                ["Lượng đặt phòng TB/tháng", "Tăng trưởng TB %"],
                                value="Lượng đặt phòng TB/tháng",
                                label="Sắp xếp theo"
                            )
                            btn1 = gr.Button("🔍 Xem kết quả", variant="primary", size="lg")

                    with gr.Column(scale=3):
                        info1 = gr.HTML()
                        chart1 = gr.Plot()
                        table1 = gr.Dataframe(
                            headers=["#", "Tỉnh/Thành", "Vùng", "TB Lượt/Tháng",
                                     "Tổng Lượt (12T)", "Tăng trưởng TB %", "Tháng Đỉnh"],
                            wrap=True,
                        )

                btn1.click(tab1_top_provinces,
                           inputs=[start_m, end_m, region_dd, top_n_dd, sort_dd],
                           outputs=[table1, chart1, info1])

            # ══════════════════════════════════════════════
            # TAB 2: PROVINCE COMPARISON
            # ══════════════════════════════════════════════
            with gr.Tab("📈 So sánh Tỉnh"):
                with gr.Row():
                    with gr.Column(scale=1, min_width=280):
                        gr.Markdown("### ⚙️ Chọn tỉnh")
                        with gr.Group(elem_classes="filter-panel"):
                            prov_check = gr.Dropdown(
                                provinces,
                                value=["Đà Nẵng", "Hà Nội", "Hồ Chí Minh"] if provinces else [],
                                multiselect=True,
                                label="Chọn tỉnh (tối đa 8)",
                                max_choices=8,
                            )
                            metric_dd = gr.Dropdown(
                                ["Lượt đặt phòng (actual)", "Tăng trưởng % (so tháng trước)"],
                                value="Lượt đặt phòng (actual)",
                                label="Chỉ số hiển thị"
                            )
                            btn2 = gr.Button("📊 So sánh", variant="primary", size="lg")

                    with gr.Column(scale=3):
                        info2  = gr.HTML()
                        chart2 = gr.Plot()

                btn2.click(tab2_compare,
                           inputs=[prov_check, metric_dd],
                           outputs=[chart2, info2])

            # ══════════════════════════════════════════════
            # TAB 3: TRAVELER TYPE ANALYSIS
            # ══════════════════════════════════════════════
            with gr.Tab("👥 Phân tích Du khách"):
                with gr.Row():
                    with gr.Column(scale=1, min_width=280):
                        gr.Markdown("### ⚙️ Chọn tỉnh & Năm")
                        with gr.Group(elem_classes="filter-panel"):
                            feat_prov_dd = gr.Dropdown(
                                feat_provinces,
                                value=feat_provinces[0] if feat_provinces else None,
                                label="Tỉnh/Thành phố"
                            )
                            feat_year_dd = gr.Dropdown(feat_years, value="Tất cả", label="Năm")
                            btn3 = gr.Button("🔍 Phân tích", variant="primary", size="lg")
                        gr.Markdown("""
**Giải thích:**
- **Cặp đôi**: Du lịch theo cặp, thường có rating cao
- **Gia đình**: Mùa hè tăng mạnh, cần tiện ích gia đình
- **Công tác**: Ổn định cả năm, ít nhạy cảm với mùa vụ
- **Một mình**: Xu hướng tăng sau 2022 (solo travel)
""")

                    with gr.Column(scale=3):
                        info3   = gr.HTML()
                        chart3a = gr.Plot(label="Phân bổ Loại Du khách theo Tháng")
                        chart3b = gr.Plot(label="Lượt Review & Hotness Score theo Tháng")

                btn3.click(tab3_traveler,
                           inputs=[feat_prov_dd, feat_year_dd],
                           outputs=[chart3a, chart3b, info3])

            # ══════════════════════════════════════════════
            # TAB 4: MODEL INFO
            # ══════════════════════════════════════════════
            with gr.Tab("🧠 Thông tin Model"):
                with gr.Row():
                    refresh_btn = gr.Button("🔄 Refresh từ MLflow", variant="secondary")
                model_html = gr.HTML(value=tab4_model_info())
                refresh_btn.click(tab4_model_info, inputs=[], outputs=[model_html])

        # Footer
        gr.HTML("""
        <div style="text-align:center; padding:20px; color:#94a3b8; font-size:0.85em; margin-top:10px;">
            Tourism Analytics · LSTM Hotel Volume Forecaster v3 · Built with PySpark + Iceberg + MLflow + Gradio
        </div>
        """)

    return app


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    print("🚀 Starting LSTM Tourism Forecast Dashboard...")
    app = create_app()
    app.launch(
        server_name="0.0.0.0",
        server_port=int(os.getenv("GRADIO_PORT", 7860)),
        share=False,
        show_error=True,
    )
