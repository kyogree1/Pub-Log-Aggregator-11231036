import os,tempfile,asyncio
from fastapi.testclient import TestClient
from src.main import app,queue
from src.storage import Storage

client = TestClient(app)

def test_validation():
    bad={"topic":"t","event_id":"e","timestamp":"bad","source":"s","payload":{}}
    assert client.post("/publish",json=bad).status_code==422

def test_dedup():
    evt={"topic":"t","event_id":"1","timestamp":"2025-10-21T00:00:00Z","source":"s","payload":{}}
    client.post("/publish",json={"events":[evt,evt]})
    asyncio.get_event_loop().run_until_complete(queue.join())
    s=client.get("/stats").json()
    assert s["unique_processed"]>=1 and s["duplicate_dropped"]>=1

def test_persist_tmp():
    with tempfile.TemporaryDirectory() as td:
        db=os.path.join(td,"a.db")
        s1=Storage(db); assert s1.first_seen_and_store(topic="a",event_id="1",timestamp="t",source="s",payload={})
        s1.close()
        s2=Storage(db); assert not s2.first_seen_and_store(topic="a",event_id="1",timestamp="t",source="s",payload={})
        s2.close()
