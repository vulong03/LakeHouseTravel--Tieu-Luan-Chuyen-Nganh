import os
import sys
import pandas as pd
import numpy as np

# Simulate the Gradio data loading locally
FEATURES_FILE = "d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/minio/data/gold/dl_training/dl_features.parquet"
FORECAST_DIR = "d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/minio/data/gold/dl_forecast/province_hotel_volume_forecast_lstm_v5"

def load_local_forecast():
    # Load all parquet files in forecast dir
    files = [os.path.join(FORECAST_DIR, f) for f in os.listdir(FORECAST_DIR) if f.endswith(".parquet")]
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df["region_vi"] = df["region"]
    return df

def load_local_features():
    df = pd.read_parquet(FEATURES_FILE)
    return df

def load_average_aspects():
    df_feat = load_local_features()
    aspect_cols = [
        "avg_aspect_scenery", "avg_aspect_food", "avg_aspect_price",
        "avg_aspect_service", "avg_aspect_accommodation"
    ]
    df_avg = df_feat.groupby("province_sk")[aspect_cols].mean().reset_index()
    return df_avg

def simulate_recommendation(selected_theme):
    df_forecast = load_local_forecast()
    
    # Filter for typical forecast month, e.g. 202512
    df_filtered = df_forecast[df_forecast["year_month"] == 202512].copy()
    
    df_aspects = load_average_aspects()
    df_filtered = df_filtered.merge(df_aspects, on="province_sk", how="left")
    
    aspect_cols = [
        "avg_aspect_scenery", "avg_aspect_food", "avg_aspect_price",
        "avg_aspect_service", "avg_aspect_accommodation"
    ]
    for c in aspect_cols:
        df_filtered[c] = df_filtered[c].fillna(0.0) # Fill with 0 (neutral) instead of 0.5
        
    v_min, v_max = df_filtered["predicted_hotel_volume_actual"].min(), df_filtered["predicted_hotel_volume_actual"].max()
    df_filtered["volume_norm"] = (df_filtered["predicted_hotel_volume_actual"] - v_min) / (v_max - v_min) if v_max > v_min else 1.0
    
    theme_col_map = {
        "Scenery": "avg_aspect_scenery",
        "Food": "avg_aspect_food",
        "Price": "avg_aspect_price",
        "Service": "avg_aspect_service",
        "Accommodation": "avg_aspect_accommodation"
    }
    
    aspect_col = theme_col_map[selected_theme]
    a_min, a_max = df_filtered[aspect_col].min(), df_filtered[aspect_col].max()
    df_filtered["aspect_norm"] = (df_filtered[aspect_col] - a_min) / (a_max - a_min) if a_max > a_min else 1.0
    
    df_filtered["recommendation_score"] = 0.4 * df_filtered["volume_norm"] + 0.6 * df_filtered["aspect_norm"]
    
    top_provinces = df_filtered.sort_values("recommendation_score", ascending=False).head(5)
    print(f"\n--- Top 5 for {selected_theme} ---")
    for idx, row in top_provinces.iterrows():
        print(f"Rank {row.get('province_name')}: Score={row['recommendation_score']:.4f} (VolNorm={row['volume_norm']:.4f}, AspectNorm={row['aspect_norm']:.4f}, RawAspect={row[aspect_col]:.4f})")

def main():
    print("Simulating recommendations using ONLY correct dl_features.parquet...")
    for theme in ["Scenery", "Food", "Price", "Service", "Accommodation"]:
        simulate_recommendation(theme)

if __name__ == "__main__":
    main()
