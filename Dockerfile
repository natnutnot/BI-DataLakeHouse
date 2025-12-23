# Gunakan Python versi ringan (Slim)
FROM python:3.12-slim

# Set folder kerja di dalam container
WORKDIR /app

# Install dependencies sistem (jika butuh curl/git, dsb)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy file requirements dulu (agar caching docker efisien)
COPY requirements.txt .

# Install library Python
RUN pip install --no-cache-dir -r requirements.txt

# Copy seluruh kode proyek ke dalam container
COPY . .

# Buka port 8501 (Port Streamlit)
EXPOSE 8501

# Perintah default saat container nyala (Jalankan Dashboard)
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]