import pandas as pd
import json
import os
import ast

# --- KONFIGURASI PATH ---
BRONZE_PATH = 'bronze_layer'
SILVER_PATH = 'silver_layer'

# Buat folder silver jika belum ada
os.makedirs(SILVER_PATH, exist_ok=True)

# --- KAMUS PERBAIKAN GENRE (SOLUSI MASALAH ANDA) ---
# Kiri: Tulisan Salah/Indo (Lower case) -> Kanan: Tulisan Standar (English)
GENRE_MAP = {
    'komedi': 'Comedy',
    'comedy': 'Comedy',
    'horor': 'Horror',
    'horror': 'Horror',
    'aksi': 'Action',
    'action': 'Action',
    'romantis': 'Romance',
    'romance': 'Romance',
    'fiksi ilmiah': 'Sci-Fi',
    'sci-fi': 'Sci-Fi',
    'science fiction': 'Sci-Fi',
    'animasi': 'Animation',
    'animation': 'Animation',
    'drama': 'Drama',
    'petualangan': 'Adventure',
    'adventure': 'Adventure',
    'thriller': 'Thriller',
    'misteri': 'Mystery',
    'mystery': 'Mystery',
    'fantasi': 'Fantasy',
    'fantasy': 'Fantasy',
    'dokumenter': 'Documentary',
    'documentary': 'Documentary',
    'keluarga': 'Family',
    'family': 'Family',
    'musik': 'Music',
    'music': 'Music',
    'perang': 'War',
    'war': 'War',
    'sejarah': 'History',
    'history': 'History',
    'western': 'Western',
    'crime': 'Crime',
    'kejahatan': 'Crime',
    'film noir': 'Film Noir',
    'film noir': 'Film Noir',
    'sport': 'Sport',
    'olahraga': 'Sport',
    'biografi': 'Biography',
    'biography': 'Biography',
    'musikal': 'Musical',
    'musical': 'Musical',
    'thriller psikologis': 'Psychological Thriller',
    'psychological thriller': 'Psychological Thriller',
    'superhero': 'Superhero'
}

def clean_genre_text(genre_string):
    """
    Fungsi canggih untuk membersihkan genre yang berantakan.
    Contoh input: "Horor, komedi, Action"
    Output: "Horror, Comedy, Action"
    """
    if pd.isna(genre_string) or genre_string == '':
        return 'Unknown'
    
    # 1. Pisahkan berdasarkan koma (jika ada banyak genre)
    parts = [g.strip().lower() for g in str(genre_string).split(',')]
    
    cleaned_parts = []
    for p in parts:
        # 2. Cek di kamus mapping, kalau tidak ada pakai aslinya tapi dikapitalisasi
        clean_word = GENRE_MAP.get(p, p.title())
        cleaned_parts.append(clean_word)
        
    # 3. Gabungkan kembali dan Hapus duplikat (misal: Comedy, Comedy -> Comedy)
    return ', '.join(sorted(list(set(cleaned_parts))))

# --- 1. TRANSFORMASI HISTORY (Perbaikan Genre) ---
def transform_history():
    print("\n[1/4] Transform: Cleaning History Film...")
    try:
        df = pd.read_csv(f"{BRONZE_PATH}/raw_history_film.csv")
        df = df.drop_duplicates(subset=['Nama Film'])
        # Terapkan pembersihan genre
        df['Genre_Clean'] = df['Genre'].apply(clean_genre_text)
        
        # Pilih kolom yang bersih saja
        df_clean = df[['Nama Film', 'Genre_Clean']].rename(columns={
            'Nama Film': 'title',
            'Genre_Clean': 'genres'
        })
        
        output = f"{SILVER_PATH}/dim_history_film.parquet"
        df_clean.to_parquet(output, index=False)
        print(f"   ✅ Sukses: Genre dinormalisasi (Komedi -> Comedy). Simpan ke {output}")
        print(f"   👀 Contoh: {df_clean['genres'].iloc[0]}")
        
    except Exception as e:
        print(f"   ❌ Gagal History: {e}")

# --- 2. TRANSFORMASI TUGAS (Perbaikan Progress & Tanggal) ---
def transform_tugas():
    print("\n[2/4] Transform: Cleaning Data Tugas...")
    try:
        df = pd.read_csv(f"{BRONZE_PATH}/raw_tugas_kesibukan.csv")
        
        # 1. HAPUS DUPLIKAT BARIS (Kalau ada baris kembar persis, sisakan 1)
        initial_count = len(df)

        # 2. Bersihkan Angka (String -> Float)
        df['estimation_hours'] = pd.to_numeric(df['Estimasi (jam)'], errors='coerce').fillna(0)
        
        # 3. STANDARISASI KATEGORI (PENTING untuk logika Gold)
        # " akademik " -> "Akademik" (Hapus spasi, Kapital awal)
        df['Kategori'] = df['Kategori'].astype(str).str.strip().str.title()

        # 4. Bersihkan Progress
        def clean_progress(val):
            val = str(val).replace('%', '').strip()
            try:
                num = float(val)
                return num / 100.0 if num > 1.0 else num
            except:
                return 0.0
        df['progress_clean'] = df['Progress '].apply(clean_progress)
        
        # 5. Tanggal (Day First)
        df['deadline_clean'] = pd.to_datetime(df['Deadline'], dayfirst=True, errors='coerce')
        
        # 6. Filter Sampah (Jam negatif atau Tanggal Error)
        df = df[df['deadline_clean'].notna()]  # Buang yang tanggalnya ngaco
        df = df[df['estimation_hours'] > 0]    # Buang jam 0 atau minus

        # Rename
        df_clean = df.rename(columns={
            'Nama Tugas': 'task_name',
            'Kategori': 'category',
            'Tipe Beban': 'load_type'
        })
        
        # Pilih kolom final
        final_cols = ['task_name', 'estimation_hours', 'progress_clean', 'deadline_clean', 'category', 'load_type']
        df_final = df_clean[final_cols]
        
        output = f"{SILVER_PATH}/dim_tasks.parquet"
        df_final.to_parquet(output, index=False)
        print(f"   ✅ Sukses: Data Tugas Bersih (No Duplicate, Standard Category).")
        
    except Exception as e:
        print(f"   ❌ Gagal Tugas: {e}")

# --- 3. TRANSFORMASI CALENDAR (Flatten JSON) ---
def transform_calendar():
    print("\n[3/4] Transform: Cleaning Calendar...")
    try:
        with open(f"{BRONZE_PATH}/raw_calendar_events.json", 'r') as f:
            data = json.load(f)
            
        # Normalisasi JSON (Meratakan struktur nested)
        if not data:
            print("   ⚠️ Data Calendar Kosong.")
            return

        cleaned_events = []
        for event in data:
            # Ambil hanya yang penting
            summary = event.get('summary', 'No Title')
            
            # Start/End bisa berupa 'dateTime' (rapat) atau 'date' (seharian)
            start = event.get('start', {}).get('dateTime') or event.get('start', {}).get('date')
            end = event.get('end', {}).get('dateTime') or event.get('end', {}).get('date')
            
            cleaned_events.append({
                'event_title': summary,
                'start_time': start,
                'end_time': end
            })
            
        df = pd.DataFrame(cleaned_events)
        
        # Pastikan format tanggal dikenali komputer
        df['start_time'] = pd.to_datetime(df['start_time'], utc=True)
        df['end_time'] = pd.to_datetime(df['end_time'], utc=True)
        
        output = f"{SILVER_PATH}/dim_calendar.parquet"
        df.to_parquet(output, index=False)
        print(f"   ✅ Sukses: JSON diratakan. Simpan ke {output}")
        
    except Exception as e:
        print(f"   ❌ Gagal Calendar: {e}")

# --- 4. TRANSFORMASI TMDB (Select Columns) ---
def transform_tmdb():
    print("\n[4/4] Transform: Cleaning TMDB Movies...")
    try:
        with open(f"{BRONZE_PATH}/raw_tmdb_movies.json", 'r') as f:
            data = json.load(f)
            
        df = pd.DataFrame(data)
        
        # Pilih kolom penting saja (Buang yang tidak perlu)
        wanted_cols = ['id', 'title', 'genre_ids', 'vote_average', 'popularity', 'release_date','overview']
        # Pastikan kolom ada (kalau tidak ada, isi NaN)
        for col in wanted_cols:
            if col not in df.columns:
                df[col] = None
                
        df_clean = df[wanted_cols].copy()
        
        # Konversi genre_ids (List angka) menjadi string (biar bisa disimpan di Parquet)
        df_clean['genre_ids'] = df_clean['genre_ids'].astype(str)
        
        output = f"{SILVER_PATH}/dim_tmdb_movies.parquet"
        df_clean.to_parquet(output, index=False)
        print(f"   ✅ Sukses: {len(df_clean)} film dibersihkan. Simpan ke {output}")
        
    except Exception as e:
        print(f"   ❌ Gagal TMDB: {e}")

if __name__ == "__main__":
    print("--- 🥈 START SILVER LAYER TRANSFORMATION 🥈 ---")
    transform_history()
    transform_tugas()
    transform_calendar()
    transform_tmdb()
    print("--- FINISHED ---")