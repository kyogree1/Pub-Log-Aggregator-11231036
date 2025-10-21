import json
from datetime import datetime, timezone

START_TIME = datetime.now(timezone.utc)

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        try:
            from pydantic import BaseModel
            if isinstance(o, BaseModel):
                return o.model_dump()
        except Exception:
            pass
        if hasattr(o, "isoformat"):
            return o.isoformat()
        return super().default(o)
