import gspread
import pandas as pd
from pymongo import MongoClient
from google.oauth2.service_account import Credentials

# --- KONFIGURASI ---
CREDENTIALS_FILE = 'credentials.json'
SHEET_NAME = "Data History Film"  # Pastikan nama ini SAMA PERSIS dengan di Drive
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "uas_bi_db"
COLLECTION_NAME = "watch_history"

def seed_data_from_cloud():
    print("🚀 [SEEDING] Memulai proses pemindahan Data History (Cloud -> MongoDB)...")
    
    # --- PERBAIKAN DI SINI (MENAMBAHKAN SCOPE DRIVE) ---
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'  # <--- INI TAMBAHAN PENTINGNYA
    ]
    
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        
        # Buka Sheet
        print("   ...Menghubungi Google Drive...")
        sheet = client.open(SHEET_NAME).sheet1
        data = sheet.get_all_records() 
        print(f"   ✅ Berhasil menarik {len(data)} data dari Google Sheets.")
        
    except Exception as e:
        print(f"   ❌ Gagal koneksi ke Google Sheets: {e}")
        return

    # 2. Masukkan ke MongoDB (Lokal)
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        collection.delete_many({}) 
        
        if data:
            collection.insert_many(data)
            print(f"   ✅ SUKSES! {len(data)} data tersimpan di MongoDB Local.")
            print("   Sekarang MongoDB berisi data history tontonan Anda.")
        else:
            print("   ⚠️ Data di Sheet kosong.")
            
    except Exception as e:
        print(f"   ❌ Gagal koneksi ke MongoDB: {e}")

if __name__ == "__main__":
    seed_data_from_cloud()