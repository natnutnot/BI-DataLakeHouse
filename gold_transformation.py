import pandas as pd
import os

# --- KONFIGURASI PATH ---
SILVER_PATH = 'silver_layer'
GOLD_PATH = 'gold_layer'

# Buat folder gold jika belum ada
os.makedirs(GOLD_PATH, exist_ok=True)

# --- 1. MEMBUAT FACT PRODUCTIVITY (Gabungan Calendar & Tugas) ---
def create_fact_productivity():
    print("\n[1/2] Gold: Creating Fact Productivity...")
    try:
        # Load Data Silver
        df_cal = pd.read_parquet(f"{SILVER_PATH}/dim_calendar.parquet")
        df_task = pd.read_parquet(f"{SILVER_PATH}/dim_tasks.parquet")

        # --- OLAH DATA CALENDAR ---
        # Hitung durasi event (End - Start) dalam jam
        df_cal['duration_hours'] = (df_cal['end_time'] - df_cal['start_time']).dt.total_seconds() / 3600
        
        # Ambil tanggal saja (tanpa jam)
        df_cal['date'] = df_cal['start_time'].dt.date
        
        # Standarisasi kolom agar bisa digabung
        df_cal_clean = df_cal[['date', 'event_title', 'duration_hours']].copy()
        df_cal_clean['category'] = 'Calendar Activity' # Default category buat calendar
        df_cal_clean['source'] = 'Google Calendar'

        # --- OLAH DATA TUGAS ---
        # Asumsi: Tanggal pengerjaan dianggap sama dengan Deadline (Simplifikasi)
        df_task['date'] = df_task['deadline_clean'].dt.date
        
        # Standarisasi kolom
        df_task_clean = df_task[['date', 'task_name', 'estimation_hours', 'category']].copy()
        df_task_clean = df_task_clean.rename(columns={
            'task_name': 'event_title', 
            'estimation_hours': 'duration_hours'
        })
        df_task_clean['source'] = 'Task List'

        # --- GABUNGKAN (UNION) ---
        df_combined = pd.concat([df_cal_clean, df_task_clean], ignore_index=True)

        # --- AGREGASI (GROUP BY) ---
        # Kita ingin tahu: Per Tanggal & Per Kategori, berapa total jamnya?
        fact_daily = df_combined.groupby(['date', 'category']).agg(
            total_hours=('duration_hours', 'sum'),
            total_activities=('event_title', 'count')
        ).reset_index()

        # Urutkan berdasarkan tanggal
        fact_daily = fact_daily.sort_values('date')

        # Simpan
        output = f"{GOLD_PATH}/fact_daily_productivity.parquet"
        fact_daily.to_parquet(output, index=False)
        print(f"   ✅ Sukses: Data produktivitas harian disimpan ke {output}")
        print(f"   👀 Preview:\n{fact_daily.head(3)}")

    except Exception as e:
        print(f"   ❌ Gagal Productivity: {e}")

# --- 2. MEMBUAT FACT GENRE (Analisa Tontonan) ---
def create_fact_genre():
    print("\n[2/2] Gold: Creating Fact Genre Analytics...")
    try:
        df_film = pd.read_parquet(f"{SILVER_PATH}/dim_history_film.parquet")

        # Masalah: Satu film punya banyak genre (misal: "Action, Comedy")
        # Kita harus memecahnya agar "Action" terhitung 1, "Comedy" terhitung 1.
        
        # 1. Split string menjadi List
        df_film['genre_list'] = df_film['genres'].str.split(', ')
        
        # 2. Explode (Meledakkan list menjadi baris baru)
        # Baris: "Deadpool", ["Action", "Comedy"] 
        # Menjadi:
        # "Deadpool", "Action"
        # "Deadpool", "Comedy"
        df_exploded = df_film.explode('genre_list')
        
        # 3. Hitung jumlah per genre
        fact_genre = df_exploded.groupby('genre_list').agg(
            total_watched=('title', 'count')
        ).reset_index()
        
        # Rename kolom
        fact_genre = fact_genre.rename(columns={'genre_list': 'genre_name'})
        
        # Urutkan dari yang paling sering ditonton
        fact_genre = fact_genre.sort_values('total_watched', ascending=False)

        # Simpan
        output = f"{GOLD_PATH}/fact_genre_stats.parquet"
        fact_genre.to_parquet(output, index=False)
        print(f"   ✅ Sukses: Statistik Genre disimpan ke {output}")
        print(f"   👀 Top 3 Genre:\n{fact_genre.head(3)}")

    except Exception as e:
        print(f"   ❌ Gagal Genre: {e}")

if __name__ == "__main__":
    print("--- 🥇 START GOLD LAYER TRANSFORMATION 🥇 ---")
    create_fact_productivity()
    create_fact_genre()
    print("--- FINISHED ---")