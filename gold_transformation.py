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

        # --- A. OLAH DATA CALENDAR (TETAP: Jangan Dibagi) ---
        df_cal['duration_hours'] = (df_cal['end_time'] - df_cal['start_time']).dt.total_seconds() / 3600
        df_cal['date'] = df_cal['start_time'].dt.date
        df_cal_clean = df_cal[['date', 'event_title', 'duration_hours']].copy()
        df_cal_clean['category'] = 'Calendar Activity'
        df_cal_clean['source'] = 'Google Calendar'

        # --- B. OLAH DATA TUGAS (LOGIKA BARU: Filter Akademik) ---
        tasks_list = []
        
        for _, row in df_task.iterrows():
            deadline = row['deadline_clean'].date()
            hours = row['estimation_hours']
            load_type = row.get('load_type', 'Sesi')
            category = row.get('category', 'General') # Ambil kategori
            
            # CEK KATEGORI: Hanya 'Akademik' yang boleh dicicil
            # Pastikan tulisan di Excel persis 'Akademik' (Huruf besar/kecil berpengaruh)
            is_academic = (category == 'Akademik') 
            
            # LOGIKA UTAMA: 
            # 1. Harus tipe 'Dicicil'
            # 2. Jam > 0
            # 3. Kategori HARUS 'Akademik'
            if load_type == 'Dicicil' and hours > 0 and is_academic:
                
                if hours > 100:
                    days_spread = 120 # 4 Bulan
                elif hours > 20:
                    days_spread = 14  # 2 Minggu
                else:
                    days_spread = 7   # 1 Minggu
                
                daily_load = hours / days_spread
                
                # Buat rentang tanggal mundur
                date_range = pd.date_range(end=deadline, periods=days_spread, freq='D')
                
                for d in date_range:
                    tasks_list.append({
                        'date': d.date(),
                        'event_title': f"{row['task_name']} (Cicil)",
                        'duration_hours': daily_load,
                        'category': row['category'],
                        'source': 'Task List'
                    })
            else:
                # BAGIAN INI UNTUK:
                # 1. Tugas 'Sesi'
                # 2. Tugas 'Non-Akademik' (Biarpun dicicil, masuk sini agar tanggalnya tidak pecah)
                tasks_list.append({
                    'date': deadline,
                    'event_title': row['task_name'],
                    'duration_hours': hours,
                    'category': row['category'],
                    'source': 'Task List'
                })
        
        # Buat DataFrame baru dari list
        df_task_clean = pd.DataFrame(tasks_list)

        # --- GABUNGKAN (UNION) ---
        df_combined = pd.concat([df_cal_clean, df_task_clean], ignore_index=True)

        # --- AGREGASI (GROUP BY) ---
        fact_daily = df_combined.groupby(['date', 'category']).agg(
            total_hours=('duration_hours', 'sum'),
            total_activities=('event_title', 'count')
        ).reset_index()

        # Urutkan berdasarkan tanggal
        fact_daily = fact_daily.sort_values('date')

        # Simpan
        output = f"{GOLD_PATH}/fact_daily_productivity.parquet"
        fact_daily.to_parquet(output, index=False)
        print(f"   ✅ Sukses: Data produktivitas disimpan.")
        print(f"      Hanya 'Akademik' > 20 jam yang disebar. Non-Akademik tetap utuh.")
        print(f"   👀 Preview:\n{fact_daily.head(3)}")

    except Exception as e:
        print(f"   ❌ Gagal Productivity: {e}")

# --- 2. MEMBUAT FACT GENRE (Analisa Tontonan) ---
def create_fact_genre():
    print("\n[2/2] Gold: Creating Fact Genre Analytics...")
    try:
        df_film = pd.read_parquet(f"{SILVER_PATH}/dim_history_film.parquet")

        # 1. Split string menjadi List
        df_film['genre_list'] = df_film['genres'].str.split(', ')
        
        # 2. Explode (Meledakkan list menjadi baris baru)
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