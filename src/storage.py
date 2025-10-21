from __future__ import annotations
import sqlite3, json, os
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

DEFAULT_DB = os.environ.get("AGG_DB_PATH", ".data/agg.db")

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS dedup (
        topic TEXT NOT NULL,
        event_id TEXT NOT NULL,
        processed_at TEXT NOT NULL,
        PRIMARY KEY (topic, event_id)
    ) WITHOUT ROWID;""",
    """CREATE TABLE IF NOT EXISTS events (
        topic TEXT NOT NULL,
        event_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        source TEXT NOT NULL,
        payload TEXT NOT NULL,
        ingest_time TEXT NOT NULL,
        PRIMARY KEY (topic, event_id)
    ) WITHOUT ROWID;""",
    """CREATE TABLE IF NOT EXISTS metrics (
        key TEXT PRIMARY KEY,
        value INTEGER NOT NULL
    );"""
]

class Storage:
    def __init__(self, path: Optional[str] = None):
        self.path = path or DEFAULT_DB
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._init_schema()

    def _init_schema(self):
        c = self.conn.cursor()
        for s in SCHEMA: c.execute(s)
        for k in ["received","unique_processed","duplicate_dropped"]:
            c.execute("INSERT OR IGNORE INTO metrics VALUES(?,0)", (k,))
        self.conn.commit()

    def incr(self, key:str, d:int=1):
        self.conn.execute("UPDATE metrics SET value=value+? WHERE key=?", (d,key))
        self.conn.commit()

    def get_metrics(self)->Dict[str,int]:
        return {k:v for k,v in self.conn.execute("SELECT key,value FROM metrics")}

    def first_seen_and_store(self,*,topic,event_id,timestamp,source,payload)->bool:
        try:
            with self.conn:
                now=datetime.now(timezone.utc).isoformat()
                self.conn.execute(
                    "INSERT INTO dedup VALUES(?,?,?)",(topic,event_id,now))
                self.conn.execute(
                    "INSERT INTO events VALUES(?,?,?,?,?,?)",
                    (topic,event_id,timestamp,source,json.dumps(payload),now))
            return True
        except sqlite3.IntegrityError:
            return False

    def events(self,topic=None)->List[Dict[str,Any]]:
        q=("SELECT topic,event_id,timestamp,source,payload,ingest_time FROM events "
           "WHERE topic=? ORDER BY ingest_time ASC") if topic else \
          "SELECT topic,event_id,timestamp,source,payload,ingest_time FROM events ORDER BY ingest_time ASC"
        rows=self.conn.execute(q,(topic,) if topic else ()).fetchall()
        return [{"topic":t,"event_id":e,"timestamp":ts,"source":s,
                 "payload":json.loads(p),"ingest_time":ing}
                for t,e,ts,s,p,ing in rows]

    def topics(self)->List[str]:
        return [r[0] for r in self.conn.execute("SELECT DISTINCT topic FROM events")]

    def close(self): 
        self.conn.close()
