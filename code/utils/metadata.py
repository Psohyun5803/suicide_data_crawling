import json
from pathlib import Path
from datetime import datetime

def update_meta(meta_path: str, key: str, record: dict):
    p = Path(meta_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    if p.exists():
        meta = json.loads(p.read_text(encoding="utf-8"))
    else:
        meta = {}

    rec = dict(record)
    rec["collected_at"] = datetime.now().isoformat(timespec="seconds")
    meta[key] = rec

    p.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )