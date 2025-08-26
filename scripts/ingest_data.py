import os
import shutil

RAW_DIR = "../data/raw"
LANDING_DIR = "../data/landing"  # simulate Azure Data Lake landing zone

def ingest_data():
    os.makedirs(LANDING_DIR, exist_ok=True)
    for file in os.listdir(RAW_DIR):
        src = os.path.join(RAW_DIR, file)
        dest = os.path.join(LANDING_DIR, file)
        shutil.copy(src, dest)
        print(f"Uploaded {file} to Azure Landing Zone ✅")

if __name__ == "__main__":
    ingest_data()
