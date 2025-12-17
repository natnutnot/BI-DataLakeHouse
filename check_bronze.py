import pandas as pd
import json
import os

BRONZE_PATH = 'bronze_layer'

def check_csv(filename, source_name):
    path = os.path.join(BRONZE_PATH, filename)
    print(f"\n🔎 MEMERIKSA {source_name} ({filename})...")
    
    if not os.path.exists(path):
        print("   ❌ FILE TIDAK DITEMUKAN!")
        return

    try:
        df = pd.read_csv(path)
        count = len(df)
        print(f"   ✅ Status: FILE VALID")
        print(f"   📊 Jumlah Baris: {count}")
        print(f"   👀 Contoh Kolom: {list(df.columns)}")
        print(f"   📝 Sampel Data (Baris 1):")
        print(df.iloc[0].to_dict() if count > 0 else "   ⚠️ DATA KOSONG")
    except Exception as e:
        print(f"   ❌ FILE RUSAK/ERROR: {e}")

def check_json(filename, source_name):
    path = os.path.join(BRONZE_PATH, filename)
    print(f"\n🔎 MEMERIKSA {source_name} ({filename})...")
    
    if not os.path.exists(path):
        print("   ❌ FILE TIDAK DITEMUKAN!")
        return

    try:
        with open(path, 'r') as f:
            data = json.load(f)
        
        count = len(data)
        print(f"   ✅ Status: FILE VALID")
        print(f"   📊 Jumlah Item: {count}")
        
        if count > 0:
            # Ambil sampel item pertama
            sample = data[0]
            # Tampilkan 3 key pertama saja biar rapi
            keys = list(sample.keys())[:5] 
            print(f"   👀 Kunci (Keys): {keys}...")
            print(f"   📝 Sampel Item 1: {sample.get('title') or sample.get('summary') or 'Nama tidak ditemukan'}")
        else:
            print("   ⚠️ DATA KOSONG (List Kosong [])")
            
    except json.JSONDecodeError:
        print(f"   ❌ FORMAT JSON RUSAK (Bukan JSON valid)")
    except Exception as e:
        print(f"   ❌ ERROR LAIN: {e}")

if __name__ == "__main__":
    print("--- 🕵️ MULAI AUDIT DATA BRONZE LAYER ---")
    
    # 1. Cek History (CSV)
    check_csv("raw_history_film.csv", "Data History (MongoDB)")
    
    # 2. Cek Tugas (CSV)
    check_csv("raw_tugas_kesibukan.csv", "Data Tugas (Sheets)")
    
    # 3. Cek Calendar (JSON)
    check_json("raw_calendar_events.json", "Data Rutinitas (Calendar)")
    
    # 4. Cek TMDB (JSON)
    check_json("raw_tmdb_movies.json", "Data Film (TMDB)")
    
    print("\n--- 🏁 AUDIT SELESAI ---")