from fastapi import FastAPI, BackgroundTasks
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
from pydantic import BaseModel
from datetime import datetime
import sqlite3, os, time

# ========== 1️⃣ Konfigurasi dasar FastAPI ==========
app = FastAPI(
    title="📘 UTS SISTER A PUB-SUB LOG AGGREGATOR 11231036",
    description=(
        "Sistem Aggregator untuk menguji konsep **Publish–Subscribe**, "
        "Idempotent Consumer, dan Deduplication.\n\n"
        "Dibuat oleh: **Muhammad Azka Yunastio (11231036)** - Informatika ITK."
    ),
    version="1.0.0",
    contact={
        "name": "Muhammad Azka Yunastio",
        "email": "m.azka@itk.ac.id"
    },
    swagger_ui_parameters={
        "docExpansion": "none",
        "defaultModelsExpandDepth": -1,
        "displayRequestDuration": True,
        "syntaxHighlight.theme": "obsidian",
        "tryItOutEnabled": True
    }
)

# ========== 2️⃣ Custom Swagger & ReDoc ==========
@app.get("/swagger", include_in_schema=False)
async def swagger_ui():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title="📘 UTS SISTER LOG AGGREGATOR - Swagger",
        swagger_favicon_url="https://cdn-icons-png.flaticon.com/512/906/906343.png"
    )

@app.get("/redoc", include_in_schema=False)
async def redoc_ui():
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title="📗 UTS SISTER LOG AGGREGATOR - ReDoc"
    )

# ========== 3️⃣ Database setup ==========
DB_PATH = os.getenv("AGG_DB_PATH", "./.data/agg.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS dedup (
        topic TEXT,
        event_id TEXT,
        processed_at TEXT,
        PRIMARY KEY (topic, event_id)
    )
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        received INTEGER,
        unique_processed INTEGER,
        duplicate_dropped INTEGER,
        timestamp TEXT
    )
    """)
    return conn

conn = get_conn()
start_time = time.time()

# ========== 4️⃣ Model Event ==========
class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: dict

class PublishBatch(BaseModel):
    events: list[Event]

# ========== 5️⃣ Endpoint /publish ==========
@app.post("/publish")
async def publish(batch: PublishBatch, bg: BackgroundTasks):
    received = len(batch.events)
    unique = 0
    duplicate = 0

    for e in batch.events:
        try:
            conn.execute(
                "INSERT INTO dedup(topic, event_id, processed_at) VALUES (?, ?, ?)",
                (e.topic, e.event_id, datetime.utcnow().isoformat())
            )
            unique += 1
        except sqlite3.IntegrityError:
            duplicate += 1

    conn.commit()

    # Simpan metrics
    conn.execute(
        "INSERT INTO metrics(received, unique_processed, duplicate_dropped, timestamp) VALUES (?, ?, ?, ?)",
        (received, unique, duplicate, datetime.utcnow().isoformat())
    )
    conn.commit()

    print(f"Processed {received} | Unique: {unique} | Duplicate: {duplicate}")
    return {"received": received, "unique": unique, "duplicate": duplicate}

# ========== 6️⃣ Endpoint /events ==========
@app.get("/events")
async def get_events(topic: str = None):
    q = "SELECT topic, event_id, processed_at FROM dedup"
    params = []
    if topic:
        q += " WHERE topic=?"
        params.append(topic)
    q += " ORDER BY processed_at DESC LIMIT 100"
    rows = conn.execute(q, params).fetchall()
    return [{"topic": t, "event_id": e, "processed_at": p} for (t, e, p) in rows]

# ========== 7️⃣ Endpoint /stats ==========
@app.get("/stats")
async def get_stats(topic: str = None, start: str = None, end: str = None):
    """
    Statistik sistem global / per-topic:
    - /stats → semua data
    - /stats?topic=OLAHRAGA → hanya topik tertentu
    - /stats?start=...&end=... → filter waktu
    """

    # Total event unik berdasarkan filter
    q = "SELECT topic, event_id, processed_at FROM dedup"
    params, filters = [], []
    if topic:
        filters.append("topic=?")
        params.append(topic)
    if start and end:
        filters.append("processed_at BETWEEN ? AND ?")
        params.extend([start, end])
    if filters:
        q += " WHERE " + " AND ".join(filters)
    rows = conn.execute(q, params).fetchall()
    unique_processed = len(rows)

    # Ambil total metrics dari seluruh publikasi (bukan 0)
    total_received = conn.execute("SELECT SUM(received) FROM metrics").fetchone()[0] or 0
    total_duplicates = conn.execute("SELECT SUM(duplicate_dropped) FROM metrics").fetchone()[0] or 0

    # Jika filter topic aktif, perkirakan ulang received
    if topic:
        # jumlah event di topik ini dibanding total semua topik (rasio sederhana)
        total_topic_unique = conn.execute("SELECT COUNT(*) FROM dedup WHERE topic=?", (topic,)).fetchone()[0]
        topic_ratio = total_topic_unique / (conn.execute("SELECT COUNT(*) FROM dedup").fetchone()[0] or 1)
        received = int(total_received * topic_ratio)
        duplicate_dropped = int(total_duplicates * topic_ratio)
    else:
        received = total_received
        duplicate_dropped = total_duplicates

    # Ambil daftar semua topik
    all_topics = [r[0] for r in conn.execute("SELECT DISTINCT topic FROM dedup")]

    return {
        "received": received,
        "unique_processed": unique_processed,
        "duplicate_dropped": duplicate_dropped,
        "topics": all_topics if not topic else [topic],
        "uptime_seconds": round(time.time() - start_time, 6)
    }

# ========== 8️⃣ Jalankan server ==========
if __name__ == "__main__":  
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=8080, reload=True)
