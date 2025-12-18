import os
import json
import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

CREDENTIALS_FILE = 'credentials.json'
TMDB_API_KEY = os.getenv('TMDB_API_KEY')
GOOGLE_CALENDAR_ID = os.getenv('GOOGLE_CALENDAR_ID')
BRONZE_PATH = 'bronze_layer'
SHEET_TUGAS_NAME = "Data Kesibukan"

# Pastikan folder ada
os.makedirs(BRONZE_PATH, exist_ok=True)

# 1. Ingest MongoDB (History)
def ingest_mongodb():
    print("\n[1/4] Ingest: MongoDB (History) -> CSV Bronze...")
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["uas_bi_db"]
        collection = db["watch_history"]
        
        # Ambil semua data
        data = list(collection.find({}, {'_id': 0}))
        
        if len(data) > 0:
            df = pd.DataFrame(data)
            output = f"{BRONZE_PATH}/raw_history_film.csv"
            df.to_csv(output, index=False)
            print(f"   ✅ Tersimpan: {output} ({len(df)} baris)")
        else:
            print("   ⚠️ Data MongoDB Kosong. Cek seed_nosql.py!")
            
    except Exception as e:
        print(f"   ❌ Error MongoDB: {e}")

# 2. Ingest Google Sheets (Tugas)
def ingest_sheets_tugas():
    print("\n[2/4] Ingest: Google Sheets (Tugas) -> CSV Bronze...")
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        
        sheet = client.open(SHEET_TUGAS_NAME).sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        output = f"{BRONZE_PATH}/raw_tugas_kesibukan.csv"
        df.to_csv(output, index=False)
        print(f"   ✅ Tersimpan: {output} ({len(df)} tugas)")
    except Exception as e:
        print(f"   ❌ Error Sheets: {e}")

# 3. Ingest Google Calendar (FIXED: Timezone & Range)
def ingest_calendar():
    print("\n[3/4] Ingest: Google Calendar API -> JSON Bronze...")
    SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        service = build('calendar', 'v3', credentials=creds)
        
        # 1. Waktu Mulai = SEKARANG (UTC)
        now = datetime.now(timezone.utc)
        start_time = now.isoformat()
        
        # 2. Waktu Akhir = 14 Hari Kedepan (Opsional, biar tidak ambil jadwal tahun depan)
        end_time = (now + timedelta(days=14)).isoformat()
        
        print(f"   ...Mengambil jadwal dari {start_time} s/d {end_time}")
        
        # Request ke API (Ambil 50 event kedepan)
        events_result = service.events().list(
            calendarId=GOOGLE_CALENDAR_ID,
            timeMin=start_time,
            timeMax=end_time,
            maxResults=250,
            singleEvents=True,
            orderBy='startTime'
        ).execute()
        
        events = events_result.get('items', [])
        
        if not events:
            print("   ⚠️ Masih 0 Events. Pastikan akun Calendar Anda ada isinya di tanggal ini.")
        
        output = f"{BRONZE_PATH}/raw_calendar_events.json"
        with open(output, 'w') as f:
            json.dump(events, f, indent=4)
        print(f"   ✅ Tersimpan: {output} ({len(events)} events ditemukan)")
        
    except Exception as e:
        print(f"   ❌ Error Calendar: {e}")

# 4. Ingest TMDB (FIXED: Loop 5 Halaman)
def ingest_tmdb():
    print("\n[4/4] Ingest: TMDB API -> JSON Bronze...")
    all_movies = []
    
    try:
        # Loop ambil 5 halaman (20 film x 5 = 100 film)
        for page in range(1, 51):
            url = f"https://api.themoviedb.org/3/movie/popular?api_key={TMDB_API_KEY}&language=en-US&page={page}"
            resp = requests.get(url)
            
            if resp.status_code == 200:
                results = resp.json().get('results', [])
                all_movies.extend(results)
                print(f"   ...Halaman {page} sukses ({len(results)} film)")
            else:
                print(f"   ❌ Gagal Halaman {page}: {resp.status_code}")
        
        # Simpan Total
        output = f"{BRONZE_PATH}/raw_tmdb_movies.json"
        with open(output, 'w') as f:
            json.dump(all_movies, f, indent=4)
        print(f"   ✅ Tersimpan: {output} (Total {len(all_movies)} film)")
        
    except Exception as e:
        print(f"   ❌ Error TMDB: {e}")

if __name__ == "__main__":
    print("--- START DATA LAKEHOUSE INGESTION V2 ---")
    ingest_mongodb()
    ingest_sheets_tugas()
    ingest_calendar()
    ingest_tmdb()
    print("--- FINISHED ---")