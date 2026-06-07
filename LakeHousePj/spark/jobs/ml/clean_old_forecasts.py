import os
from minio import Minio
# pyrefly: ignore [missing-import]
from minio.error import S3Error
from datetime import datetime

# ============================================================
# Configuration
# ============================================================
MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"
BUCKET_NAME = "gold"
PREFIX = "dl_forecast/"
LSTM_FILE_PATTERN = "province_hotel_volume_forecast_lstm_"
DRY_RUN = True  # Set to False to perform actual deletion

def clean_old_forecasts():
    print("=" * 80)
    print("MINIO STORAGE CLEAN-UP: OBSOLETE FORECAST FILES")
    print("=" * 80)
    
    # Initialize MinIO client
    client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )
    
    try:
        # Check if bucket exists
        if not client.bucket_exists(BUCKET_NAME):
            print(f"Error: Bucket '{BUCKET_NAME}' does not exist.")
            return
            
        print(f"Scanning bucket '{BUCKET_NAME}' under prefix '{PREFIX}'...")
        objects = list(client.list_objects(BUCKET_NAME, prefix=PREFIX, recursive=True))
        
        folder_files = {}
        folder_timestamps = {}
        
        # Group files by parent directory and track the latest modified date per folder
        for obj in objects:
            if obj.object_name.endswith(".parquet") and LSTM_FILE_PATTERN in obj.object_name:
                parts = obj.object_name.split("/")
                if len(parts) >= 3:
                    folder = f"{parts[0]}/{parts[1]}/"
                    folder_files.setdefault(folder, []).append(obj.object_name)
                    if folder not in folder_timestamps or obj.last_modified > folder_timestamps[folder]:
                        folder_timestamps[folder] = obj.last_modified

        if not folder_timestamps:
            print("No LSTM forecast directories found. Nothing to clean.")
            return
            
        # Identify the latest folder
        latest_folder = max(folder_timestamps, key=folder_timestamps.get)
        latest_time = folder_timestamps[latest_folder]
        
        print(f"\n[KEEPING LATEST RUN]")
        print(f"  Folder: {latest_folder}")
        print(f"  Last Modified: {latest_time}")
        
        # Identify obsolete folders to delete
        obsolete_folders = [f for f in folder_timestamps.keys() if f != latest_folder]
        
        if not obsolete_folders:
            print("\nNo obsolete runs found. Only the latest run exists.")
            return
            
        if DRY_RUN:
            print(f"\n[DRY RUN: WOULD DELETE {len(obsolete_folders)} OBSOLETE RUNS]")
        else:
            print(f"\n[DELETING {len(obsolete_folders)} OBSOLETE RUNS]")
            
        for folder in obsolete_folders:
            files_to_delete = folder_files[folder]
            if DRY_RUN:
                print(f"  Folder (Would delete): {folder} ({len(files_to_delete)} files, last active: {folder_timestamps[folder]})")
                for file_path in files_to_delete:
                    print(f"    Would delete: {file_path}")
            else:
                print(f"  Folder: {folder} ({len(files_to_delete)} files, last active: {folder_timestamps[folder]})")
                for file_path in files_to_delete:
                    try:
                        client.remove_object(BUCKET_NAME, file_path)
                        print(f"    Deleted: {file_path}")
                    except Exception as e:
                        print(f"    Failed to delete {file_path}: {e}")
                    
        if DRY_RUN:
            print("\nNote: Running in DRY RUN mode. Set DRY_RUN = False in clean_old_forecasts.py to perform actual deletion.")
        else:
            print("\nClean-up completed successfully!")
        print("=" * 80)
        
    except S3Error as e:
        print(f"MinIO S3 Error: {e}")
    except Exception as e:
        print(f"Error during clean-up: {e}")

if __name__ == "__main__":
    clean_old_forecasts()
