"""
Tourism Trend Forecasting — LSTM Hotel Volume Dashboard
========================================================

Gradio interface for the LSTM v5 model predicting reservation volume (hotel_review_volume).
Target: hotel_review_volume (log-normalized, expm1 used for actual display scaling)

Tabs:
  Tab 1: 🏖️ Seasonal Recommendations — Seasonal & themed destination ranking
  Tab 2: 🏆 Forecast Ranking — Top provinces by forecasted booking volume
  Tab 3: 📈 Compare Provinces — Historical & forecasted trends over time
  Tab 4: 👥 Traveler Demographics — Couple/family/business/solo ratios per province
  Tab 5: 🏨 Hotel Segments — Search hotels by customer segments via K-Means (K=3)
  Tab 6: 🧠 Model Specs — Deep learning architecture & MLflow metrics
"""

import os
import time
import sys
from datetime import datetime
from io import BytesIO
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import gradio as gr
from minio import Minio
from minio.error import S3Error
import mlflow

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
FORECAST_PREFIX = "dl_forecast/"
FEATURES_PREFIX = "dl_training/dl_features.parquet"          # dl_features.parquet (for traveler type & nlp avg)
LSTM_FILE_PATTERN = "province_hotel_volume_forecast_lstm_"

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
LSTM_MODEL_NAME = "province_hotel_volume_forecaster_lstm_v5"

CACHE_DURATION = 60   # seconds
_forecast_cache = {"data": None, "timestamp": 0, "run_folder": ""}
_features_cache = {"data": None, "timestamp": 0}

REGION_MAP_EN = {
    "Northeast":           "Northeast",
    "Northwest":           "Northwest",
    "Red_River_Delta":     "Red River Delta",
    "North_Central_Coast": "North Central Coast",
    "Central_Highlands":   "Central Highlands",
    "South_Central_Coast": "South Central Coast",
    "Southeast":           "Southeast",
    "Mekong_Delta":        "Mekong Delta",
}
REGIONS_EN = {"All Regions": None, **{v: k for k, v in REGION_MAP_EN.items()}}

MONTH_CHOICES = [
    "10/2025", "11/2025", "12/2025",
    "01/2026", "02/2026", "03/2026",
    "04/2026", "05/2026", "06/2026",
    "07/2026", "08/2026", "09/2026",
]

# Exact coastal province names as represented in the database
BEACH_PROVINCES = {
    "Đà Nẵng", "Khánh Hòa", "Kiên Giang", "Bà Rịa Vũng Tàu", "Bình Thuận", 
    "Quảng Ninh", "Hải Phòng", "Bình Định", "Phú Yên", "Quảng Nam", 
    "Thừa Thiên Huế", "Ninh Thuận", "Thanh Hóa", "Nghệ An", "Quảng Bình",
    "Bến Tre", "Trà Vinh", "Sóc Trăng", "Bạc Liêu", "Cà Mau",
    "Quảng Ngãi", "Quảng Trị", "Hà Tĩnh", "Tiền Giang", "Nam Định", "Thái Bình"
}

def _ym_int(month_str: str) -> int:
    """Convert 'MM/YYYY' → YYYYMM int."""
    m, y = month_str.split("/")
    return int(y) * 100 + int(m)

def _ym_label(ym_int: int) -> str:
    return f"{ym_int % 100:02d}/{ym_int // 100}"


def _get_months_from_start_end(start_month: str, end_month: str) -> list:
    """Return all months between start_month and end_month (inclusive) from MONTH_CHOICES."""
    if not start_month or not end_month:
        return ["05/2026", "06/2026", "07/2026", "08/2026"]
    try:
        if start_month not in MONTH_CHOICES or end_month not in MONTH_CHOICES:
            return ["05/2026", "06/2026", "07/2026", "08/2026"]
        idx_start = MONTH_CHOICES.index(start_month)
        idx_end   = MONTH_CHOICES.index(end_month)
        if idx_start > idx_end:
            idx_start, idx_end = idx_end, idx_start
        return MONTH_CHOICES[idx_start : idx_end + 1]
    except Exception as e:
        print(f"Error parsing month range: {e}")
        return ["05/2026", "06/2026", "07/2026", "08/2026"]

# ============================================================================
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
            return pd.DataFrame(), "⚠️ No LSTM forecast files found in MinIO. Please run train_lstm_forecast.py first!"

        latest = max(folder_ts, key=folder_ts.get)
        dfs = []
        for fpath in files_by_folder[latest]:
            resp = MINIO_CLIENT.get_object(BUCKET_NAME, fpath)
            dfs.append(pd.read_parquet(BytesIO(resp.read())))

        df = pd.concat(dfs, ignore_index=True)
        df["region_vi"] = df["region"].map(REGION_MAP_EN).fillna(df["region"])
        df["date"] = pd.to_datetime(df["year_month"].astype(float).astype(int).astype(str), format="%Y%m")
        df["month_label"] = df["date"].dt.strftime("%m/%Y")

        _forecast_cache.update({"data": df, "timestamp": now, "run_folder": latest})
        print(f"✅ Loaded {len(df)} LSTM forecast rows from {latest}")
        return df, None

    except S3Error as e:
        return pd.DataFrame(), f"❌ MinIO connection error: {e}"
    except Exception as e:
        import traceback; traceback.print_exc()
        return pd.DataFrame(), f"❌ Data loading error: {e}"


def load_features_data():
    """Load dl_features parquet (for traveler type analysis and aspect weights) from MinIO."""
    global _features_cache
    now = time.time()
    if _features_cache["data"] is not None and (now - _features_cache["timestamp"]) < CACHE_DURATION:
        return _features_cache["data"], None

    try:
        objects = list(MINIO_CLIENT.list_objects(BUCKET_NAME, prefix=FEATURES_PREFIX, recursive=True))
        parquet_files = [o.object_name for o in objects if o.object_name.endswith(".parquet")]
        if not parquet_files:
            return pd.DataFrame(), "⚠️ dl_features.parquet not found in MinIO."

        dfs = []
        for fpath in parquet_files:
            resp = MINIO_CLIENT.get_object(BUCKET_NAME, fpath)
            dfs.append(pd.read_parquet(BytesIO(resp.read())))

        df = pd.concat(dfs, ignore_index=True)
        df["region_vi"] = df["region"].map(REGION_MAP_EN).fillna(df.get("region", ""))
        _features_cache.update({"data": df, "timestamp": now})
        print(f"✅ Loaded {len(df)} feature rows for traveler analysis")
        return df, None

    except Exception as e:
        import traceback; traceback.print_exc()
        return pd.DataFrame(), f"❌ Features loading error: {e}"


def load_average_aspects():
    """Load aspects from features and compute historical averages per province."""
    df_feat, err = load_features_data()
    if err or df_feat.empty:
        return pd.DataFrame(), err or "Empty dataset"

    aspect_cols = [
        "avg_aspect_scenery", "avg_aspect_food", "avg_aspect_price",
        "avg_aspect_service", "avg_aspect_accommodation"
    ]
    existing = [c for c in aspect_cols if c in df_feat.columns]
    if not existing:
        return pd.DataFrame(), "⚠️ PhoBERT aspect columns not found in dl_features."

    # Group by province_sk to get baseline characteristics
    df_avg = df_feat.groupby("province_sk")[existing].mean().reset_index()
    return df_avg, None


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
# Tab 1 — Seasonal & Themed Recommendations
# ============================================================

def tab1_seasonal_recommend(start_month, end_month, selected_theme, selected_region, top_n):
    """Generate recommendations based on predicted volume and PhoBERT aspect scores over multiple months."""
    df_forecast, err = load_forecast_data()
    if err:
        return pd.DataFrame(), None, _error_html(err), _error_html(err)
    if df_forecast.empty:
        return pd.DataFrame(), None, _warn_html("No forecast data available."), _warn_html("No data.")

    # Build list of months from start/end dropdowns
    selected_months = _get_months_from_start_end(start_month, end_month)
    if not selected_months:
        return pd.DataFrame(), None, _warn_html("Please select at least one month."), _warn_html("No month selected.")

    month_ints = [_ym_int(m) for m in selected_months if m]
    if not month_ints:
        return pd.DataFrame(), None, _warn_html("No valid months selected."), _warn_html("No valid months.")

    # Filter by selected months
    df_filtered = df_forecast[df_forecast["year_month"].isin(month_ints)].copy()
    if selected_region != "Tất cả" and selected_region != "All Regions":
        df_filtered = df_filtered[df_filtered["region_vi"] == selected_region]

    if df_filtered.empty:
        return pd.DataFrame(), None, _warn_html("No forecast data found matching selected criteria."), _warn_html("No data.")

    # Fetch aspect scores
    df_aspects, _ = load_average_aspects()
    if not df_aspects.empty:
        df_filtered = df_filtered.merge(df_aspects, on="province_sk", how="left")

    # Set default values for aspects if missing
    aspect_cols = [
        "avg_aspect_scenery", "avg_aspect_food", "avg_aspect_price",
        "avg_aspect_service", "avg_aspect_accommodation"
    ]
    for c in aspect_cols:
        if c in df_filtered.columns:
            df_filtered[c] = df_filtered[c].fillna(0.5)
        else:
            df_filtered[c] = 0.5

    # ── Aggregate first to get per-province averages ──
    agg_dict = {
        "predicted_hotel_volume_actual": "mean",
    }
    for c in aspect_cols:
        if c in df_filtered.columns:
            agg_dict[c] = "first"

    df_m = df_filtered.groupby(["province_sk", "province_name", "region_vi"]).agg(agg_dict).reset_index()

    # ── Normalize volume to [0, 1] để tránh bias thành phố lớn ──
    v_min, v_max = df_m["predicted_hotel_volume_actual"].min(), df_m["predicted_hotel_volume_actual"].max()
    if v_max > v_min:
        df_m["volume_norm"] = (df_m["predicted_hotel_volume_actual"] - v_min) / (v_max - v_min)
    else:
        df_m["volume_norm"] = 1.0

    # ── Compute recommendation score ──
    theme_col_map = {
        "🏞️ Scenery":       "avg_aspect_scenery",
        "🍲 Food":           "avg_aspect_food",
        "💰 Price":          "avg_aspect_price",
        "🛎️ Service":        "avg_aspect_service",
        "🏨 Accommodation":  "avg_aspect_accommodation",
        "All": None
    }
    aspect_col = theme_col_map.get(selected_theme)

    if aspect_col and aspect_col in df_m.columns:
        # Normalize aspect về [0,1] (dữ liệu PhoBERT thường đã trong khoảng 0–1, nhưng chuẩn hoá thêm cho chắc)
        a_min, a_max = df_m[aspect_col].min(), df_m[aspect_col].max()
        if a_max > a_min:
            df_m["aspect_norm"] = (df_m[aspect_col] - a_min) / (a_max - a_min)
        else:
            df_m["aspect_norm"] = 1.0
        # Trọng số: 40% volume, 60% aspect → chất lượng trải nghiệm quyết định hơn khối lượng
        df_m["recommendation_score"] = 0.4 * df_m["volume_norm"] + 0.6 * df_m["aspect_norm"]
        theme_label = selected_theme
    else:
        # "All" → rank thuần theo volume (normalized)
        df_m["recommendation_score"] = df_m["volume_norm"]
        theme_label = "Overall Demand"

    # Scale rec_index về 0–100
    max_score = df_m["recommendation_score"].max()
    if max_score > 0:
        df_m["rec_index"] = (df_m["recommendation_score"] / max_score) * 100
    else:
        df_m["rec_index"] = 0.0

    df_m = df_m.sort_values("rec_index", ascending=False).head(int(top_n)).reset_index(drop=True)
    df_m["rank"] = range(1, len(df_m) + 1)

    # 1. Create Plotly Bar Chart (Dark Theme)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_m["province_name"],
        y=df_m["rec_index"].round(1),
        marker=dict(
            color=df_m["rec_index"],
            colorscale="Viridis",
            showscale=True,
            colorbar=dict(title="Recommendation Score"),
        ),
        text=df_m["rec_index"].round(1),
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>Score: %{y:.1f}/100<br><extra></extra>",
    ))
    
    # Dạng range ngắn gọn: "MM/YYYY → MM/YYYY" thay vì liệt kê từng tháng
    if len(selected_months) > 1:
        months_range_str = f"{selected_months[0]} → {selected_months[-1]}"
    else:
        months_range_str = selected_months[0] if selected_months else ""

    fig.update_layout(
        title={"text": f"🎯 Top Destination Recommendations — Criteria: {theme_label} ({months_range_str})",
               "x": 0.5, "xanchor": "center", "font": {"size": 18, "color": "#f1f5f9"}},
        xaxis=dict(title="", tickangle=-30, tickfont=dict(color="#94a3b8")),
        yaxis=dict(title="Recommendation Score (0-100)", range=[0, 115], tickfont=dict(color="#94a3b8")),
        template="plotly_dark",
        height=420,
        plot_bgcolor="#111827",
        paper_bgcolor="#111827",
    )

    # 2. Generate Premium HTML cards (Dark Theme)
    cards_html = f"""
    <div style="margin-top: 25px; padding-bottom: 5px; border-bottom: 2px solid #1f2937; margin-bottom: 20px;">
        <h3 style="margin: 0; font-size: 1.5em; font-weight: 700; color: #f1f5f9; display: flex; align-items: center; gap: 8px;">
            🏖️ Special Travel Recommendations for {months_range_str}
        </h3>
    </div>
    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(310px, 1fr)); gap: 24px;">
    """

    rank_colors = {
        1: ("#fbbf24", "rgba(251, 191, 36, 0.15)", "#fbbf24", "linear-gradient(135deg, #1e1b4b 0%, #1e293b 100%)"), # Gold
        2: ("#94a3b8", "rgba(148, 163, 184, 0.15)", "#cbd5e1", "linear-gradient(135deg, #0f172a 0%, #1e293b 100%)"), # Silver
        3: ("#b45309", "rgba(180, 83, 9, 0.15)", "#f97316", "linear-gradient(135deg, #1e1b4b 0%, #1c1917 100%)")  # Bronze
    }

    # Determine representative season details based on selected months
    summer_months_in_sel = [m % 100 for m in month_ints if (m % 100) in [5, 6, 7, 8]]
    winter_months_in_sel = [m % 100 for m in month_ints if (m % 100) in [11, 12, 1]]
    spring_months_in_sel = [m % 100 for m in month_ints if (m % 100) in [2, 3, 4]]
    
    if len(summer_months_in_sel) >= len(winter_months_in_sel) and len(summer_months_in_sel) >= len(spring_months_in_sel):
        rep_season = "summer"
    elif len(winter_months_in_sel) >= len(summer_months_in_sel) and len(winter_months_in_sel) >= len(spring_months_in_sel):
        rep_season = "winter"
    elif len(spring_months_in_sel) >= len(summer_months_in_sel) and len(spring_months_in_sel) >= len(winter_months_in_sel):
        rep_season = "spring"
    else:
        rep_season = "autumn"

    for i, row in df_m.iterrows():
        rank = row["rank"]
        prov_name = row["province_name"]
        region = row["region_vi"]
        rec_idx = row["rec_index"]
        pred_vol = row["predicted_hotel_volume_actual"]

        border_color, badge_bg, text_color, card_bg = rank_colors.get(rank, ("#4f46e5", "rgba(79, 70, 229, 0.15)", "#818cf8", "#111827"))

        # Check coastal and mountainous characteristics
        is_beach = prov_name in BEACH_PROVINCES
        is_highland = prov_name in {"Lâm Đồng", "Lào Cai", "Hà Giang", "Sơn La", "Yên Bái", "Lai Châu", "Điện Biên", "Cao Bằng", "Lạng Sơn"}
        
        season_badge = ""
        season_desc = ""
        
        if rep_season == "summer":
            if is_beach:
                season_badge = "🏖️ Summer - Beach Travel"
                season_desc = f"{prov_name} is welcoming a high volume of visitors. Highly suitable for swimming, water sports, and beach resorts."
            else:
                season_badge = "☀️ Summer - Sightseeing & Entertainment"
                season_desc = f"{prov_name} is in its bustling summer phase. Ideal for indoor sightseeing, diverse culinary exploration, and active summer entertainment."
        elif rep_season == "winter":
            if is_highland:
                season_badge = "❄️ Winter - Highland Cloud Hunting"
                season_desc = f"The cold weather in {prov_name} is perfect for mountain cloud hunting, viewing misty landscapes, winter flowers (wild sunflowers/apricots), and warm local delicacies."
            else:
                season_badge = "🍁 Winter - Urban Tourism & Relaxation"
                season_desc = f"The cool and pleasant year-end weather in {prov_name} is ideal for walking, festival shopping, and gathering with friends."
        elif rep_season == "spring":
            season_badge = "🌸 Spring - Festivals & Scenic Tours"
            season_desc = f"This is when {prov_name} hosts many traditional early-year festivals, suitable for temple sightseeing and spring heritage exploration."
        else:
            season_badge = "🍂 Autumn - Moderate & Romantic"
            season_desc = f"The climate in {prov_name} is cool, pleasant, and beautiful, suitable for outdoor picnics or wellness recovery retreat tours."

        # Aspect details
        aspect_info = ""
        if selected_theme != "Tất cả" and selected_theme != "All":
            val = row[theme_col_map[selected_theme]]
            if val > 0.05:
                badge_color = "#10b981"  # Emerald Green for positive
                val_str = f"+{val:.2f}"
            elif val < -0.05:
                badge_color = "#f43f5e"  # Rose Red for negative
                val_str = f"{val:.2f}"
            else:
                badge_color = "#64748b"  # Slate Gray for neutral
                val_str = f"{val:.2f}"
            aspect_info = f'<span style="background:{badge_color}; color:white; padding:3px 8px; border-radius:12px; font-size:0.8em; font-weight:600; margin-left:6px;">Net Sentiment: {val_str}</span>'

        cards_html += f"""
        <div style="background: {card_bg}; border-radius: 18px; border: 1px solid #1f2937; box-shadow: 0 4px 20px rgba(0,0,0,0.15); padding: 24px; position: relative; overflow: hidden;">
            <div style="position: absolute; top: 0; left: 0; height: 100%; width: 6px; background: {border_color};"></div>
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px;">
                <span style="background: {badge_bg}; color: {text_color}; padding: 4px 12px; border-radius: 20px; font-size: 0.85em; font-weight: 700; border: 1px solid {border_color}33;">
                    Rank {rank}
                </span>
                <span style="font-size: 0.9em; color: #94a3b8; font-weight: 600;">{region}</span>
            </div>
            <h4 style="margin: 0 0 10px 0; font-size: 1.4em; font-weight: 800; color: #f8fafc;">{prov_name}</h4>
            <div style="margin-bottom: 16px;">
                <span style="background: #1f2937; color: #cbd5e1; padding: 4px 10px; border-radius: 12px; font-size: 0.8em; font-weight: 600;">{season_badge}</span>
                {aspect_info}
            </div>
            <div style="background: #0f172a; border-radius: 12px; padding: 14px; margin-bottom: 16px; border: 1px solid #1f2937;">
                <div style="display: flex; justify-content: space-between; font-size: 0.92em; margin-bottom: 6px;">
                    <span style="color: #94a3b8;">Avg Recommendation Index:</span>
                    <strong style="color: #38bdf8; font-size: 1.1em;">{rec_idx:.1f} / 100</strong>
                </div>
                <div style="display: flex; justify-content: space-between; font-size: 0.92em;">
                    <span style="color: #94a3b8;">Avg Est. Bookings/Month:</span>
                    <strong style="color: #f1f5f9;">{pred_vol:,.0f} bookings</strong>
                </div>
            </div>
            <p style="margin: 0; font-size: 0.88em; color: #94a3b8; line-height: 1.6; font-weight: 450;">{season_desc}</p>
        </div>
        """

    cards_html += "</div>"

    info_html = _info_html(
        f"Travel recommendations for: {months_range_str} · Ranked by: {theme_label} · Showing Top {len(df_m)} best destinations."
    )

    table_df = df_m[["rank", "province_name", "region_vi", "rec_index", "predicted_hotel_volume_actual"]].copy()
    table_df.columns = ["Rank", "Province", "Region", "Recommendation Score (100)", "Avg Est. Bookings/Month"]
    table_df["Avg Est. Bookings/Month"] = table_df["Avg Est. Bookings/Month"].round(0).astype(int)
    table_df["Recommendation Score (100)"] = table_df["Recommendation Score (100)"].round(1)

    return table_df, fig, cards_html, info_html


# ============================================================
# Tab 2 — Top Province Forecast
# ============================================================

def tab2_top_provinces(start_month, end_month, region_vi, top_n, sort_by):
    df, err = load_forecast_data()
    if err:
        return pd.DataFrame(), None, None, _error_html(err)
    if df.empty:
        return pd.DataFrame(), None, None, _warn_html("No forecast data available.")

    s_ym, e_ym = _ym_int(start_month), _ym_int(end_month)
    if s_ym > e_ym:
        return pd.DataFrame(), None, None, _error_html("Start month must be before or equal to end month!")

    fdf = df[(df["year_month"] >= s_ym) & (df["year_month"] <= e_ym)].copy()
    if region_vi != "Tất cả" and region_vi != "All Regions":
        fdf = fdf[fdf["region_vi"] == region_vi]

    if fdf.empty:
        return pd.DataFrame(), None, None, _warn_html("No data available for the selected range/region.")

    # Aggregate
    agg = fdf.groupby(["province_sk", "province_name", "region_vi"]).agg(
        avg_volume=("predicted_hotel_volume_actual", "mean"),
        total_volume=("predicted_hotel_volume_actual", "sum"),
        avg_growth=("predicted_growth_pct", "mean"),
        peak_month=("predicted_hotel_volume_actual", "idxmax"),
    ).reset_index()

    sort_col = "avg_volume" if sort_by == "Avg Est. Bookings/Month" else "avg_growth"
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
    result_df.columns = ["Rank", "Province", "Region",
                         "Avg Est. Bookings/Month", "Total Est. Bookings (Period)", "Avg Growth %", "Peak Month"]
    result_df["Avg Est. Bookings/Month"] = result_df["Avg Est. Bookings/Month"].round(0).astype(int)
    result_df["Total Est. Bookings (Period)"] = result_df["Total Est. Bookings (Period)"].round(0).astype(int)
    result_df["Avg Growth %"] = result_df["Avg Growth %"].round(1)

    # 1. Booking Volume Bar Chart
    fig_vol = go.Figure()
    fig_vol.add_trace(go.Bar(
        x=agg["province_name"],
        y=agg["avg_volume"],
        marker=dict(
            color=agg["avg_volume"],
            colorscale="Teal",
            showscale=True,
            colorbar=dict(title="Bookings/Mo", thickness=15),
        ),
        text=agg["avg_volume"].round(0).astype(int),
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>Est. Bookings/Month: %{y:,.0f}<br><extra></extra>",
    ))
    fig_vol.update_layout(
        title={"text": f"📊 Est. Bookings/Month ({start_month} to {end_month})",
               "x": 0.5, "xanchor": "center", "font": {"size": 15, "color": "#f1f5f9"}},
        xaxis=dict(title="", tickangle=-35, tickfont=dict(color="#94a3b8")),
        yaxis=dict(title="Est. Bookings / Month", tickfont=dict(color="#94a3b8")),
        template="plotly_dark",
        height=420,
        margin=dict(t=50, b=80, l=50, r=10),
        plot_bgcolor="#111827",
        paper_bgcolor="#111827",
    )

    # 2. Growth Rate Bar Chart
    fig_growth = go.Figure()
    fig_growth.add_trace(go.Bar(
        x=agg["province_name"],
        y=agg["avg_growth"],
        marker=dict(
            color=agg["avg_growth"],
            colorscale="Electric",
            showscale=True,
            colorbar=dict(title="Growth %", thickness=15),
        ),
        text=agg["avg_growth"].round(1).apply(lambda x: f"{x:.1f}%"),
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>Avg Growth: %{y:.1f}%<br><extra></extra>",
    ))
    fig_growth.update_layout(
        title={"text": f"📈 Avg Growth % ({start_month} to {end_month})",
               "x": 0.5, "xanchor": "center", "font": {"size": 15, "color": "#f1f5f9"}},
        xaxis=dict(title="", tickangle=-35, tickfont=dict(color="#94a3b8")),
        yaxis=dict(title="Growth Rate %", tickfont=dict(color="#94a3b8")),
        template="plotly_dark",
        height=420,
        margin=dict(t=50, b=80, l=50, r=10),
        plot_bgcolor="#111827",
        paper_bgcolor="#111827",
    )

    n_prov = df["province_sk"].nunique()
    info = _info_html(
        f"Filtered {n_prov} provinces · {fdf['year_month'].nunique()} months · "
        f"Showing Top {len(agg)} (sorted by {sort_by})"
    )
    return result_df, fig_vol, fig_growth, info


# ============================================================
# Tab 3 — Province Comparison (time series)
# ============================================================

def tab2_compare(province_list, metric):
    df, err = load_forecast_data()
    if err:
        return None, _error_html(err)
    if df.empty or not province_list:
        return None, _warn_html("No provinces selected.")

    fdf = df[df["province_name"].isin(province_list)].copy()
    fdf = fdf.sort_values(["province_name", "date"])

    col = "predicted_hotel_volume_actual" if metric == "Forecasted Reviews (Actual)" else "predicted_growth_pct"
    ylab = "Forecasted Reviews" if metric == "Forecasted Reviews (Actual)" else "Growth Rate % vs Previous Month"

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
                "Month: %{x|%m/%Y}<br>"
                f"{ylab}: %{{y:,.1f}}<br>"
                "<extra></extra>"
            )
        ))

    fig.update_layout(
        title={"text": f"📈 Forecast Comparison: {metric}", "x": 0.5, "xanchor": "center", "font": {"size": 18, "color": "#f1f5f9"}},
        xaxis=dict(title="Timeline", tickformat="%m/%Y", dtick="M1", tickangle=-40, tickfont=dict(color="#94a3b8")),
        yaxis=dict(title=ylab, tickfont=dict(color="#94a3b8")),
        hovermode="x unified",
        template="plotly_dark",
        height=520,
        legend=dict(orientation="v", xanchor="left", x=1.02, yanchor="top", y=0.98,
                    bgcolor="rgba(17,24,39,0.9)", bordercolor="#334155", borderwidth=1),
        margin=dict(r=200),
        plot_bgcolor="#111827",
        paper_bgcolor="#111827",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(51,65,85,0.4)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(51,65,85,0.4)")

    info = _info_html(f"Comparing {len(province_list)} provinces · 12-Month Forecast · LSTM Model v5")
    return fig, info


# ============================================================
# Tab 4 — Traveler Type Analysis
# ============================================================

def tab3_traveler(province_name, year_filter):
    df_feat, err = load_features_data()
    if err:
        return None, None, _error_html(err)
    if df_feat.empty:
        return None, None, _warn_html("No feature data available.")

    fdf = df_feat[df_feat["province_name"] == province_name].copy()
    if year_filter != "Tất cả" and year_filter != "All":
        fdf = fdf[fdf["year"] == int(year_filter)]

    if fdf.empty:
        return None, None, _warn_html(f"No data available for {province_name}.")

    fdf = fdf.sort_values("year_month")
    fdf["date"] = pd.to_datetime(fdf["year_month"].astype(float).astype(int).astype(str), format="%Y%m")

    # Rescale the 4 traveler ratios so they sum to 100% for each month
    ratio_cols = ["couple_ratio", "family_ratio", "business_ratio", "solo_ratio"]
    row_sum = fdf[ratio_cols].sum(axis=1)
    row_sum = row_sum.replace(0, 1.0) # Avoid division by zero
    for col in ratio_cols:
        fdf[col] = fdf[col] / row_sum

    # --- Chart 1: Stacked area traveler type (Dark Theme) ---
    fig1 = go.Figure()
    colors_map = {"couple_ratio": "#4ECDC4", "family_ratio": "#FF6B6B",
                  "business_ratio": "#45B7D1", "solo_ratio": "#FFA07A"}
    labels_map = {"couple_ratio": "Couple", "family_ratio": "Family",
                  "business_ratio": "Group", "solo_ratio": "Solo"}
    for col in ["couple_ratio", "family_ratio", "business_ratio", "solo_ratio"]:
        fig1.add_trace(go.Scatter(
            x=fdf["date"],
            y=(fdf[col] * 100).round(1),
            mode="lines",
            name=labels_map[col],
            stackgroup="one",
            line=dict(width=0.5, color=colors_map[col]),
            fillcolor=colors_map[col],
            hovertemplate=f"<b>{labels_map[col]}</b>: %{{y:.1f}}%<br>Month: %{{x|%m/%Y}}<extra></extra>",
        ))

    fig1.update_layout(
        title={"text": f"👥 Traveler Type Distribution — {province_name}", "x": 0.5, "xanchor": "center", "font": {"color": "#f1f5f9"}},
        xaxis=dict(tickformat="%m/%Y", dtick="M1", tickangle=-40, tickfont=dict(color="#94a3b8")),
        yaxis=dict(title="Percentage Ratio (%)", range=[0, 100], tickfont=dict(color="#94a3b8")),
        hovermode="x unified",
        template="plotly_dark",
        height=380,
        legend=dict(orientation="h", y=-0.2),
        plot_bgcolor="#111827",
        paper_bgcolor="#111827",
    )

    # --- Chart 2: Hotel volume bar (Dark Theme) ---
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(
        x=fdf["date"],
        y=fdf["hotel_review_volume"],
        marker=dict(color=fdf["hotel_review_volume"], colorscale="Blues", showscale=False),
        name="Est. Bookings",
        hovertemplate="Month: %{x|%m/%Y}<br>Est. Bookings: %{y:,.0f}<extra></extra>",
    ))

    fig2.update_layout(
        title={"text": f"📊 Est. Bookings — {province_name}", "x": 0.5, "xanchor": "center", "font": {"color": "#f1f5f9"}},
        xaxis=dict(tickformat="%m/%Y", dtick="M1", tickangle=-40, tickfont=dict(color="#94a3b8")),
        yaxis=dict(title="Est. Bookings/Month", tickfont=dict(color="#94a3b8")),
        template="plotly_dark",
        height=380,
        legend=dict(orientation="h", y=-0.2),
        plot_bgcolor="#111827",
        paper_bgcolor="#111827",
    )

    # Summary stats
    fdf_valid = fdf[fdf[ratio_cols].sum(axis=1) > 0]
    if not fdf_valid.empty:
        summary = fdf_valid[ratio_cols].mean() * 100
    else:
        summary = fdf[ratio_cols].mean() * 100
    info = f"""
<div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 18px; border-radius: 12px; color: #f1f5f9; border: 1px solid #475569;">
  <h3 style="margin:0 0 10px; color:#38bdf8;">📍 {province_name} — Summary Stats</h3>
  <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; text-align: center;">
    <div><div style="font-size:1.8em; font-weight:800; color:#4ecdc4;">{summary['couple_ratio']:.1f}%</div><div>Couple</div></div>
    <div><div style="font-size:1.8em; font-weight:800; color:#ff6b6b;">{summary['family_ratio']:.1f}%</div><div>Family</div></div>
    <div><div style="font-size:1.8em; font-weight:800; color:#45b7d1;">{summary['business_ratio']:.1f}%</div><div>Group</div></div>
    <div><div style="font-size:1.8em; font-weight:800; color:#ffa07a;">{summary['solo_ratio']:.1f}%</div><div>Solo</div></div>
  </div>
  <p style="margin:10px 0 0; font-size:0.9em; opacity:0.9; color:#94a3b8;">{len(fdf)} months of data | Avg Est. Bookings/month: {fdf['hotel_review_volume'].mean():,.0f}</p>
</div>
"""
    return fig1, fig2, info


# ============================================================
# Tab 5 — Model Info
# ============================================================

def tab4_model_info():
    df, err = load_forecast_data()
    info = get_model_info()

    forecast_stats = ""
    if not err and not df.empty:
        top10 = (df.groupby("province_name")["predicted_hotel_volume_actual"]
                   .mean().sort_values(ascending=False).head(10))
        forecast_stats = "".join(
            f'<tr><td style="padding:8px 14px">{i+1}</td>'
            f'<td style="padding:8px 14px"><b>{name}</b></td>'
            f'<td style="padding:8px 14px; text-align:right">{val:,.0f}</td></tr>'
            for i, (name, val) in enumerate(top10.items())
        )

    if info:
        def _fmt(v):
            return f"{v:.4f}" if v is not None else "—"

        html = f"""
<div style="font-family: 'Inter', sans-serif; max-width: 900px; margin: 0 auto; color: #f1f5f9;">

  <!-- Model Header -->
  <div style="background: linear-gradient(135deg, #0f172a, #1e293b); border-radius: 16px; padding: 28px; color: white; margin-bottom: 20px; box-shadow: 0 8px 30px rgba(0,0,0,0.3); border: 1px solid #312e81;">
    <h2 style="margin: 0 0 8px; font-weight: 800; color: #38bdf8;">🧠 LSTM Deep Learning Model (v5)</h2>
    <p style="margin:0; opacity:0.9; font-size:1.1em">Forecast Model Name: {LSTM_MODEL_NAME} · Version {info['version']}</p>
    <p style="margin:8px 0 0; opacity:0.75; font-size:0.9em">Trained: {info['trained_at']} · Run ID: {info['run_id'][:12]}…</p>
  </div>

  <!-- Metrics grid -->
  <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; margin-bottom: 20px;">
    <div style="background:rgba(34, 197, 94, 0.1); border-left:4px solid #22c55e; border-radius:10px; padding:18px; border-top:1px solid rgba(34,197,94,0.15); border-right:1px solid rgba(34,197,94,0.15); border-bottom:1px solid rgba(34,197,94,0.15);">
      <div style="font-size:0.85em; color:#22c55e; font-weight:700; margin-bottom:6px;">TRAIN SET</div>
      <div style="font-size:1.4em; font-weight:700; color:#4ade80">R² = {_fmt(info['train_r2'])}</div>
      <div style="color:#a7f3d0; margin-top:4px; font-size:0.9em;">RMSE = {_fmt(info['train_rmse'])}</div>
    </div>
    <div style="background:rgba(59, 130, 246, 0.1); border-left:4px solid #3b82f6; border-radius:10px; padding:18px; border-top:1px solid rgba(59,130,246,0.15); border-right:1px solid rgba(59,130,246,0.15); border-bottom:1px solid rgba(59,130,246,0.15);">
      <div style="font-size:0.85em; color:#3b82f6; font-weight:700; margin-bottom:6px;">TEST SET (hold-out 12.5%)</div>
      <div style="font-size:1.4em; font-weight:700; color:#60a5fa">R² = {_fmt(info['test_r2'])}</div>
      <div style="color:#bfdbfe; margin-top:4px; font-size:0.9em;">RMSE = {_fmt(info['test_rmse'])} · MAE = {_fmt(info['test_mae'])}</div>
    </div>
  </div>

  <!-- Architecture -->
  <div style="background:#111827; border:1px solid #1f2937; border-radius:12px; padding:20px; margin-bottom:20px;">
    <h3 style="margin:0 0 14px; color:#38bdf8; font-weight: 700;">⚙️ Model Architecture & Hyperparameters</h3>
    <table style="width:100%; border-collapse:collapse; font-size:0.95em; color:#cbd5e1;">
      <tr style="background:#1f2937"><td style="padding:8px 14px; font-weight:600">Model Architecture</td><td style="padding:8px 14px">LSTM + LayerNorm + Temporal Attention + Dense (Multi-features)</td></tr>
      <tr><td style="padding:8px 14px; font-weight:600">Target Variable (Log-normalized)</td><td style="padding:8px 14px">hotel_volume (hotel_review_volume, Log1p scaled, Expm1 output)</td></tr>
      <tr style="background:#1f2937"><td style="padding:8px 14px; font-weight:600">Number of Features</td><td style="padding:8px 14px">{info.get('num_features', '50')} (includes Hotel & Hotness volume/lags, TikTok engagement, NLP & PhoBERT aspects)</td></tr>
      <tr><td style="padding:8px 14px; font-weight:600">Input Sequence Length</td><td style="padding:8px 14px">{info.get('sequence_length', '3')} historical months</td></tr>
      <tr style="background:#1f2937"><td style="padding:8px 14px; font-weight:600">Hidden Size</td><td style="padding:8px 14px">{info.get('hidden_size', '48')} units</td></tr>
      <tr><td style="padding:8px 14px; font-weight:600">Loss Function</td><td style="padding:8px 14px">HybridLoss (70% Huber + 30% SMAPE) — directly optimizes for MAPE reduction</td></tr>
      <tr style="background:#1f2937"><td style="padding:8px 14px; font-weight:600">Actual Epochs Run</td><td style="padding:8px 14px">{info.get('epochs_trained', '—')} (Early Stopping patience=25)</td></tr>
      <tr><td style="padding:8px 14px; font-weight:600">Best Validation Loss</td><td style="padding:8px 14px">{_fmt(info.get('best_val_loss'))}</td></tr>
    </table>
  </div>

  <!-- Top 10 provinces -->
  <div style="background:#111827; border:1px solid #1f2937; border-radius:12px; padding:20px;">
    <h3 style="margin:0 0 14px; color:#38bdf8; font-weight: 700;">🏆 Top 10 Provinces — Avg Forecasted Est. Bookings/Month</h3>
    <table style="width:100%; border-collapse:collapse; font-size:0.95em; color:#cbd5e1;">
      <thead>
        <tr style="background:#312e81; color:white; border-bottom: 2px solid #4f46e5;">
          <th style="padding:8px 14px; text-align:left; border-top-left-radius: 8px;">#</th>
          <th style="padding:8px 14px; text-align:left">Province Name</th>
          <th style="padding:8px 14px; text-align:right; border-top-right-radius: 8px;">Avg Est. Bookings/Month</th>
        </tr>
      </thead>
      <tbody>{forecast_stats}</tbody>
    </table>
  </div>
</div>
"""
    else:
        html = _warn_html("Could not connect to MLflow to retrieve model info. Please check the MLflow server status.")

    return html


# ============================================================
# Helper HTML builders
# ============================================================

def _error_html(msg):
    return f'<div style="background:rgba(239, 68, 68, 0.15);border-left:4px solid #ef4444;padding:16px;border-radius:8px;color:#fca5a5;border: 1px solid rgba(239, 68, 68, 0.2)"><b>❌ System Error</b><br>{msg}</div>'

def _warn_html(msg):
    return f'<div style="background:rgba(234, 179, 8, 0.15);border-left:4px solid #eab308;padding:16px;border-radius:8px;color:#fde047;border: 1px solid rgba(234, 179, 8, 0.2)"><b>⚠️ Note</b><br>{msg}</div>'

def _info_html(msg):
    return f'<div style="background:linear-gradient(135deg,#1e1b4b,#312e81);padding:14px 20px;border-radius:10px;color:#cbd5e1;font-weight:600;box-shadow: 0 4px 10px rgba(0,0,0,0.3); border: 1px solid #4f46e5;">ℹ️ {msg}</div>'


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
        return ["All"]
    years = [str(int(float(x))) for x in df["year"].dropna().unique() if x is not None]
    return ["All"] + sorted(list(set(years)), reverse=True)


# ============================================================
# Gradio Interface Overhaul (Dark Theme)
# ============================================================

def create_app():
    provinces = _get_provinces()
    feat_provinces = _get_feature_provinces()
    feat_years = _get_feature_years()

    SEASON_MONTHS = {
        "summer": ["05/2026", "06/2026", "07/2026", "08/2026"],
        "winter": ["11/2025", "12/2025", "01/2026"],
        "spring": ["02/2026", "03/2026", "04/2026"],
    }

    # ─── Season button toggle callbacks ───
    def toggle_summer(current_season):
        if current_season == "summer":
            # Bỏ chọn → reset về all months
            return ("none",
                    gr.update(choices=MONTH_CHOICES, value="10/2025"),
                    gr.update(choices=MONTH_CHOICES, value="09/2026"),
                    gr.update(variant="secondary"), gr.update(variant="secondary"), gr.update(variant="secondary"))
        else:
            months = SEASON_MONTHS["summer"]
            return ("summer",
                    gr.update(choices=months, value=months[0]),
                    gr.update(choices=months, value=months[-1]),
                    gr.update(variant="primary"), gr.update(variant="secondary"), gr.update(variant="secondary"))

    def toggle_winter(current_season):
        if current_season == "winter":
            return ("none",
                    gr.update(choices=MONTH_CHOICES, value="10/2025"),
                    gr.update(choices=MONTH_CHOICES, value="09/2026"),
                    gr.update(variant="secondary"), gr.update(variant="secondary"), gr.update(variant="secondary"))
        else:
            months = SEASON_MONTHS["winter"]
            return ("winter",
                    gr.update(choices=months, value=months[0]),
                    gr.update(choices=months, value=months[-1]),
                    gr.update(variant="secondary"), gr.update(variant="primary"), gr.update(variant="secondary"))

    def toggle_spring(current_season):
        if current_season == "spring":
            return ("none",
                    gr.update(choices=MONTH_CHOICES, value="10/2025"),
                    gr.update(choices=MONTH_CHOICES, value="09/2026"),
                    gr.update(variant="secondary"), gr.update(variant="secondary"), gr.update(variant="secondary"))
        else:
            months = SEASON_MONTHS["spring"]
            return ("spring",
                    gr.update(choices=months, value=months[0]),
                    gr.update(choices=months, value=months[-1]),
                    gr.update(variant="secondary"), gr.update(variant="secondary"), gr.update(variant="primary"))

    # High-fidelity Slate-Dark Theme CSS
    css = """
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');
    * { font-family: 'Outfit', 'Inter', sans-serif !important; }
    
    /* Force Custom Xanh Đen (Dark Navy) Variable System for native elements */
    :root, .dark {
        --body-background-fill: #0b0f19 !important;
        --background-fill-primary: #0b0f19 !important;
        --background-fill-secondary: #0f172a !important;
        
        --block-background-fill: #111827 !important;
        --block-border-color: #1f2937 !important;
        --block-border-width: 1px !important;
        
        --input-background-fill: #1f2937 !important;
        --input-border-color: #374151 !important;
        --input-text-color: #f1f5f9 !important;
        
        --button-primary-background-fill: linear-gradient(135deg, #4f46e5, #6366f1) !important;
        --button-primary-background-fill-hover: linear-gradient(135deg, #4338ca, #4f46e5) !important;
        --button-primary-text-color: #ffffff !important;
        
        --button-secondary-background-fill: #1f2937 !important;
        --button-secondary-background-fill-hover: #374151 !important;
        --button-secondary-text-color: #f1f5f9 !important;
        
        --neutral-50: #f8fafc !important;
        --neutral-100: #f1f5f9 !important;
        --neutral-200: #e2e8f0 !important;
        --neutral-300: #cbd5e1 !important;
        --neutral-400: #94a3b8 !important;
        --neutral-500: #64748b !important;
        --neutral-600: #475569 !important;
        --neutral-700: #334155 !important;
        --neutral-800: #1f2937 !important;
        --neutral-900: #111827 !important;
        --neutral-950: #0b0f19 !important;
        
        --body-text-color: #f1f5f9 !important;
        --block-title-text-color: #f8fafc !important;
        --block-label-text-color: #94a3b8 !important;
    }
    
    body, .gradio-container {
        background-color: #0b0f19 !important;
        color: #f1f5f9 !important;
    }
    
    /* Header Card (Indigo to Slate Gradient) */
    .header-hero {
        background: linear-gradient(135deg, #1e1b4b 0%, #312e81 40%, #1e1b4b 100%) !important;
        border-radius: 24px;
        padding: 50px 40px;
        color: white;
        text-align: center;
        margin-bottom: 30px;
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.4);
        border: 1px solid #1f2937;
        position: relative;
        overflow: hidden;
    }
    .header-hero::before {
        content: '';
        position: absolute; inset: 0;
        background: radial-gradient(circle at 80% 50%, rgba(99, 102, 241, 0.15) 0%, transparent 60%);
    }
    .header-hero h1 { font-size: 3em; font-weight: 800; margin: 0 0 10px; letter-spacing: -1px; text-shadow: 0 2px 10px rgba(0,0,0,0.2); }
    .header-hero .sub { font-size: 1.25em; opacity: 0.9; margin: 0; font-weight: 400; }
    .header-hero .badges { margin-top: 20px; display: flex; gap: 12px; justify-content: center; flex-wrap: wrap; }
    .header-hero .badge {
        background: rgba(255, 255, 255, 0.08); border: 1px solid rgba(255, 255, 255, 0.18);
        padding: 6px 16px; border-radius: 30px; font-size: 0.88em; font-weight: 600;
        backdrop-filter: blur(8px);
    }
    
    /* Control panels & custom container styles */
    .filter-panel, .gr-tabs {
        background: #111827 !important;
        border-radius: 16px;
        padding: 24px;
        border: 1px solid #1f2937 !important;
        box-shadow: 0 4px 25px rgba(0,0,0,0.15);
    }
    .filter-title {
        font-size: 1.20em;
        font-weight: 700;
        color: #f1f5f9;
        margin-bottom: 15px;
        border-bottom: 2px solid #1f2937;
        padding-bottom: 8px;
    }
    
    /* Button overrides */
    button.primary {
        background: linear-gradient(135deg, #4f46e5, #6366f1) !important;
        border: none !important;
        color: white !important;
        font-weight: 700 !important;
        box-shadow: 0 4px 12px rgba(79, 70, 229, 0.25) !important;
        transition: all 0.2s;
    }
    button.primary:hover {
        background: linear-gradient(135deg, #4338ca, #4f46e5) !important;
        transform: translateY(-1px);
        box-shadow: 0 6px 16px rgba(79, 70, 229, 0.35) !important;
    }
    
    /* Tabs custom navigation */
    .gr-tabs {
        background: #111827 !important;
        border-radius: 16px;
        padding: 10px;
        border: 1px solid #1f2937 !important;
    }
    .tab-nav {
        border-bottom: 1px solid #1f2937 !important;
    }
    .tab-nav button {
        color: #94a3b8 !important;
        font-weight: 600 !important;
        padding: 12px 24px !important;
        transition: all 0.15s;
    }
    .tab-nav button:hover {
        color: #e2e8f0 !important;
    }
    .tab-nav button.selected {
        color: #f1f5f9 !important;
        border-bottom: 2px solid #4f46e5 !important;
        background: #1f2937 !important;
        border-radius: 8px 8px 0 0 !important;
    }
    
    /* Dataframe layout */
    .dataframe {
        background-color: #111827 !important;
        color: #f1f5f9 !important;
        border-color: #1f2937 !important;
        font-size: 0.92em !important;
    }
    .dataframe th {
        background-color: #1f2937 !important;
        color: #f1f5f9 !important;
        border-bottom: 2px solid #374151 !important;
    }
    .dataframe td {
        border-bottom: 1px solid #1f2937 !important;
    }
    """


    with gr.Blocks(title="🏖️ LSTM Tourism Forecast — Vietnam", theme=gr.themes.Soft(), css=css, js="() => document.documentElement.classList.add('dark')") as app:
        # Force dark mode immediately using JS inside layout
        gr.HTML("<script>document.documentElement.classList.add('dark');</script>")

        # ─── Hero Header ───
        gr.HTML("""
        <div class="header-hero">
            <h1>🏖️ VIETNAM TOURISM RECOMMENDATION SYSTEM</h1>
            <p class="sub">Intelligent seasonal recommendation & tourism volume forecasting using LSTM Deep Learning</p>
            <div class="badges">
                <span class="badge">🧠 LSTM v5 + Attention</span>
                <span class="badge">💬 PhoBERT Multi-task Sentiment</span>
                <span class="badge">🏖️ Seasonal Destination Tips</span>
                <span class="badge">🏨 Hotel Customer Segments</span>
                <span class="badge">📦 MinIO & Iceberg Lakehouse</span>
            </div>
        </div>
        """)

        with gr.Tabs():

            # ══════════════════════════════════════════════
            # TAB 1: TOP PROVINCES (FORECAST RANKING)
            # ══════════════════════════════════════════════
            with gr.Tab("🏆 Forecast Ranking"):
                with gr.Row():
                    with gr.Column(scale=1, min_width=280):
                        gr.HTML('<div class="filter-title">⚙️ Time Range Filters</div>')
                        with gr.Group(elem_classes="filter-panel"):
                            start_m = gr.Dropdown(MONTH_CHOICES, value="10/2025", label="Start Month")
                            end_m   = gr.Dropdown(MONTH_CHOICES, value="09/2026", label="End Month")
                            region_t2 = gr.Dropdown(list(REGIONS_EN.keys()), value="All Regions", label="Geographical Region")
                            top_n_t2  = gr.Dropdown(["5", "10", "15", "20", "30"], value="10", label="Show Top N")
                            sort_t2   = gr.Dropdown(
                                ["Avg Est. Bookings/Month", "Avg Growth %"],
                                value="Avg Est. Bookings/Month",
                                label="Sort by"
                            )
                            btn2 = gr.Button("📊 Run Ranking", variant="primary", size="lg")

                    with gr.Column(scale=3):
                        info2 = gr.HTML()
                        with gr.Row():
                            chart2_vol = gr.Plot(label="Forecasted Est. Bookings Chart")
                            chart2_growth = gr.Plot(label="Growth Rate Chart")
                        table2 = gr.Dataframe(
                            headers=["Rank", "Province", "Region", "Avg Est. Bookings/Month",
                                     "Total Est. Bookings (Period)", "Avg Growth %", "Peak Month"],
                            wrap=True,
                        )

                btn2.click(tab2_top_provinces,
                           inputs=[start_m, end_m, region_t2, top_n_t2, sort_t2],
                           outputs=[table2, chart2_vol, chart2_growth, info2])

            # ══════════════════════════════════════════════
            # TAB 2: SEASONAL & THEMED RECOMMENDATIONS
            # ══════════════════════════════════════════════
            with gr.Tab("🏖️ Seasonal Recommendations"):
                with gr.Row():
                    with gr.Column(scale=1, min_width=300):
                        gr.HTML('<div class="filter-title">⚙️ Recommendation Filters</div>')
                        
                        with gr.Group(elem_classes="filter-panel"):
                            selected_season = gr.State("summer")
                            
                            gr.HTML('<div style="font-weight: 600; font-size: 0.9em; color: var(--block-label-text-color); margin-bottom: 8px;">Quick Season Presets</div>')
                            with gr.Row():
                                summer_btn = gr.Button("☀️ Summer", variant="primary", size="sm")
                                winter_btn = gr.Button("❄️ Winter", variant="secondary", size="sm")
                                spring_btn = gr.Button("🌸 Spring", variant="secondary", size="sm")

                            # ── Hai dropdown Start/End Month ──
                            _SUMMER_MONTHS = ["05/2026", "06/2026", "07/2026", "08/2026"]
                            with gr.Row():
                                t1_start_m = gr.Dropdown(
                                    _SUMMER_MONTHS, value="05/2026",
                                    label="Start Month"
                                )
                                t1_end_m = gr.Dropdown(
                                    _SUMMER_MONTHS, value="08/2026",
                                    label="End Month"
                                )
                            
                            theme_dd = gr.Dropdown(
                                choices=[
                                    "All",
                                    "🏞️ Scenery",
                                    "🍲 Food",
                                    "💰 Price",
                                    "🛎️ Service",
                                    "🏨 Accommodation"
                                ],
                                value="All",
                                label="Aspect Experience (NLP)",
                                info="Incorporate PhoBERT multi-task sentiment weights"
                            )
                            
                            region_dd = gr.Dropdown(
                                choices=list(REGIONS_EN.keys()),
                                value="All Regions",
                                label="Geographical Region"
                            )
                            
                            top_n_dd = gr.Dropdown(
                                choices=["5", "10", "15"],
                                value="10",
                                label="Show Top N"
                            )
                            
                            recommend_btn = gr.Button("🔍 Recommend Best Destinations", variant="primary", size="lg")

                    with gr.Column(scale=3):
                        info_rec = gr.HTML()
                        chart_rec = gr.Plot()
                        
                        with gr.Tabs():
                            with gr.Tab("📍 Visual Destination Cards"):
                                cards_html = gr.HTML()
                            with gr.Tab("📊 Detailed Data Table"):
                                table_rec = gr.Dataframe(
                                    headers=["Rank", "Province", "Region", "Recommendation Score (100)", "Avg Forecasted Reviews/Month"],
                                    wrap=True,
                                    interactive=False
                                )

                # Season preset buttons → update 2 dropdowns
                summer_btn.click(
                    toggle_summer,
                    inputs=[selected_season],
                    outputs=[selected_season, t1_start_m, t1_end_m, summer_btn, winter_btn, spring_btn]
                )
                winter_btn.click(
                    toggle_winter,
                    inputs=[selected_season],
                    outputs=[selected_season, t1_start_m, t1_end_m, summer_btn, winter_btn, spring_btn]
                )
                spring_btn.click(
                    toggle_spring,
                    inputs=[selected_season],
                    outputs=[selected_season, t1_start_m, t1_end_m, summer_btn, winter_btn, spring_btn]
                )

                # Nút Lọc chính
                recommend_btn.click(
                    tab1_seasonal_recommend,
                    inputs=[t1_start_m, t1_end_m, theme_dd, region_dd, top_n_dd],
                    outputs=[table_rec, chart_rec, cards_html, info_rec]
                )

            # ══════════════════════════════════════════════
            # TAB 3: PROVINCE COMPARISON
            # ══════════════════════════════════════════════
            with gr.Tab("📈 Compare Provinces"):
                with gr.Row():
                    with gr.Column(scale=1, min_width=280):
                        gr.HTML('<div class="filter-title">⚙️ Select Provinces to Compare</div>')
                        with gr.Group(elem_classes="filter-panel"):
                            prov_check = gr.Dropdown(
                                provinces,
                                value=["Đà Nẵng", "Lâm Đồng", "Khánh Hòa"] if provinces else [],
                                multiselect=True,
                                label="Select provinces to compare (Max 8)",
                                max_choices=8,
                            )
                            metric_dd = gr.Dropdown(
                                ["Forecasted Reviews (Actual)", "Growth % (vs Previous Month)"],
                                value="Forecasted Reviews (Actual)",
                                label="Display Metric"
                            )
                            btn3 = gr.Button("📊 Compare Trends", variant="primary", size="lg")

                    with gr.Column(scale=3):
                        info3  = gr.HTML()
                        chart3 = gr.Plot()

                btn3.click(tab2_compare,
                           inputs=[prov_check, metric_dd],
                           outputs=[chart3, info3])

            # ══════════════════════════════════════════════
            # TAB 4: TRAVELER TYPE ANALYSIS
            # ══════════════════════════════════════════════
            with gr.Tab("👥 Traveler Demographics"):
                with gr.Row():
                    with gr.Column(scale=1, min_width=280):
                        gr.HTML('<div class="filter-title">⚙️ Select Province</div>')
                        with gr.Group(elem_classes="filter-panel"):
                            feat_prov_dd = gr.Dropdown(
                                feat_provinces,
                                value=feat_provinces[0] if feat_provinces else None,
                                label="Province/City"
                            )
                            feat_year_dd = gr.Dropdown(feat_years, value="All", label="Year")
                            btn4 = gr.Button("🔍 Analyze Demographics", variant="primary", size="lg")
                        gr.Markdown("""
**Significance of Traveler Type Segmentation:**
* **Couple**: Moderate seasonal sensitivity, heavily concentrated in romantic destinations (Da Lat, Sa Pa).
* **Family**: Surges dramatically during summer (May - August) at coastal/beach destinations.
* **Group**: Important customer segment for tourist destinations, highly active during weekends and holiday seasons.
* **Solo**: Emerging trend of independent exploration among youths, focusing on remote mountains or islands.
""")

                    with gr.Column(scale=3):
                        info4   = gr.HTML()
                        chart4a = gr.Plot(label="Traveler Demographics Distribution by Month")
                        chart4b = gr.Plot(label="Est. Bookings by Month")

                btn4.click(tab3_traveler,
                           inputs=[feat_prov_dd, feat_year_dd],
                           outputs=[chart4a, chart4b, info4])

             # ══════════════════════════════════════════════
             # TAB 5: MODEL INFO
             # ══════════════════════════════════════════════
            with gr.Tab("🧠 Model Specs"):
                with gr.Row():
                    refresh_btn = gr.Button("🔄 Sync Latest Metrics from MLflow", variant="secondary")
                model_html = gr.HTML(value=tab4_model_info())
                refresh_btn.click(tab4_model_info, inputs=[], outputs=[model_html])

        # Footer
        gr.HTML("""
        <div style="text-align:center; padding:20px; color:#64748b; font-size:0.88em; margin-top:20px; border-top: 1px solid #e2e8f0;">
            Tourism Analytics Engine · LSTM Hotel Volume Forecaster v5 · Powered by PySpark + Iceberg + MLflow + Gradio
        </div>
        """)
        # Load default ranking on startup (Tab 1 = Forecast Ranking)
        app.load(
            tab2_top_provinces,
            inputs=[start_m, end_m, region_t2, top_n_t2, sort_t2],
            outputs=[table2, chart2_vol, chart2_growth, info2]
        )


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
