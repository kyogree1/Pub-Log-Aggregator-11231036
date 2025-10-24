# Pub-Sub Log Aggregator (FastAPI + SQLite + Docker)

Layanan aggregator sederhana yang menerima event dari publisher (POST /publish), memproses dengan consumer idempotent (dedup berdasarkan `(topic, event_id)`), dan menyediakan observabilitas via `/events` & `/stats`. Dedup store persisten menggunakan SQLite (local-only) sehingga tahan restart container.

## Fitur
- **POST /publish**: single atau batch event JSON
- **Idempotent consumer**: dedup `(topic, event_id)` di sisi consumer
- **Persisten**: SQLite mencegah reprocessing setelah restart
- **GET /events?topic=...**: daftar event unik yang telah diproses
- **GET /stats**: `received`, `unique_processed`, `duplicate_dropped`, `topics`, `uptime`
- **At-least-once delivery**: contoh publisher mengirim duplikat

## Skema Event
```json
{
  "topic": "string",
  "event_id": "string-unik",
  "timestamp": "ISO8601",
  "source": "string",
  "payload": { "any": "json" }
}


python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m src.main

docker build -t uts-aggregator-11231036 .
docker run --rm -p 8080:8080 `
  -e AGG_DB_PATH=/data/agg.db `
  -v ${PWD}/.data:/data `
  uts-aggregator-11231036

  docker compose up --build

`DEMONSTRASI`
https://youtu.be/QbtCG-hsy_4

`LINK LAPORAN(PDF)`
https://drive.google.com/file/d/1J6CJQHY2OKaLHCJtY0hiDZeEcKws9FSJ/view?usp=sharing
