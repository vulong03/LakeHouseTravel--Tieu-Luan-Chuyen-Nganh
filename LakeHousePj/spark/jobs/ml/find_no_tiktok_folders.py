import sys
import subprocess

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    print("Installing 'minio' package via pip...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "minio"])
    from minio import Minio
    from minio.error import S3Error

MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"
BUCKET_NAME = "gold"

def find_no_tiktok_paths():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )
    
    try:
        if not client.bucket_exists(BUCKET_NAME):
            print(f"Bucket '{BUCKET_NAME}' does not exist.")
            return
            
        print(f"Scanning bucket '{BUCKET_NAME}' for 'no_tiktok' paths...")
        objects = list(client.list_objects(BUCKET_NAME, recursive=True))
        
        no_tiktok_files = []
        all_prefixes = set()
        
        for obj in objects:
            path = obj.object_name
            # Track all folder prefixes (up to 3 levels)
            parts = path.split('/')
            if len(parts) > 1:
                all_prefixes.add('/'.join(parts[:-1]) + '/')
                
            if "no_tiktok" in path or "no-tiktok" in path:
                no_tiktok_files.append(path)
                
        print("\n--- ALL DETECTED PREFIXES/DIRECTORIES ---")
        for pref in sorted(list(all_prefixes)):
            print(f"  {pref}")
            
        print("\n--- DETECTED 'no_tiktok' RELATED FILES/PATHS ---")
        if not no_tiktok_files:
            print("  None found.")
        else:
            print(f"  Found {len(no_tiktok_files)} files:")
            for path in sorted(no_tiktok_files):
                print(f"    {path}")
                
    except Exception as e:
        print(f"Error scanning MinIO: {e}")

if __name__ == "__main__":
    find_no_tiktok_paths()
