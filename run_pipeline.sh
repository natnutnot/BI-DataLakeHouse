#!/bin/bash

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# --- KONFIGURASI ---
PROJECT_DIR="/home/heri/projects/UAS-DataLakehouse"
LOG_FILE="$PROJECT_DIR/pipeline.log"
CONTAINER_NAME="uas_app"  # Pastikan ini sama dengan nama container di docker-compose.yml

# --- FUNGSI LOGGING ---
log() {
    echo "[$1] $(date '+%Y-%m-%d %H:%M:%S') - $2" >> "$LOG_FILE"
    echo "[$1] $2" # Tampilkan juga di layar terminal
}

# --- MULAI ---
log "INFO" "🚀 MEMULAI PIPELINE DATA LAKEHOUSE (MODE DOCKER)..."

# 1. Masuk ke Folder Proyek (Penting agar command docker terbaca)
cd "$PROJECT_DIR" || { log "ERROR" "Gagal masuk folder $PROJECT_DIR"; exit 1; }

# 2. Cek & Nyalakan Container jika mati
# Logika: Cek apakah container 'uas_app' sedang jalan?
if [ ! "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    log "WARN" "Container $CONTAINER_NAME mati. Sedang menyalakan..."
    docker compose up -d
    log "INFO" "Menunggu 10 detik agar Database siap..."
    sleep 10
else
    log "INFO" "Container $CONTAINER_NAME sudah menyala. Lanjut..."
fi

# --- EKSEKUSI PIPELINE (Host menyuruh Docker) ---

# STEP 1: Seed NoSQL
log "INFO" "▶️ [Docker] Menjalankan Step 1: Seeding NoSQL..."
docker exec $CONTAINER_NAME python seed_nosql.py >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    log "ERROR" "❌ Gagal di Step 1 (Seed NoSQL). Pipeline berhenti."
    exit 1
fi

# STEP 2: Ingestion
log "INFO" "▶️ [Docker] Menjalankan Step 2: Ingestion..."
docker exec $CONTAINER_NAME python ingestion.py >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    log "ERROR" "❌ Gagal di Step 2 (Ingestion). Pipeline berhenti."
    exit 1
fi

# STEP 3: Silver Transform
log "INFO" "▶️ [Docker] Menjalankan Step 3: Silver Transformation..."
docker exec $CONTAINER_NAME python transformation.py >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    log "ERROR" "❌ Gagal di Step 3 (Silver). Pipeline berhenti."
    exit 1
fi

# STEP 4: Gold Transform
log "INFO" "▶️ [Docker] Menjalankan Step 4: Gold Transformation..."
docker exec $CONTAINER_NAME python gold_transformation.py >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    log "ERROR" "❌ Gagal di Step 4 (Gold). Pipeline berhenti."
    exit 1
fi

log "SUCCESS" "✅ SELURUH PIPELINE DOCKER SELESAI DENGAN SUKSES."
log "INFO" "---------------------------------------------------"