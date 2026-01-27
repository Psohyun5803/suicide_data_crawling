from collectors.common import build_url_with_dynamic_period, fetch_to_df,replace_latest_dated_file
from utils.file_utils import ensure_parent_dir
from utils.metadata import update_meta

import pandas as pd

METADATA_PATH = "../data/metadata/metadata.json"

def run(cfg: dict):
   
    # 1) 수집
    url = build_url_with_dynamic_period(cfg["openapi_url"], cfg.get("start_ym", "196501"))
    raw = fetch_to_df(url)

    # 2) 전처리(collector에 포함)
    df = raw[["PRD_DE", "DT"]].copy()
    df.columns = df.columns.astype(str).str.strip()
    df = df.rename(columns={"PRD_DE": "date", "DT": "cpi"})

    df["cpi"] = pd.to_numeric(df["cpi"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], format="%Y%m").dt.strftime("%Y-%m")
    df = df.sort_values("date").reset_index(drop=True)

    # 3) 저장
    out_csv = cfg["output_csv"]
    ensure_parent_dir(out_csv)
    # 1) 이전 latest_날짜 파일 제거 + 오늘 파일 경로 생성
    out_csv = replace_latest_dated_file(out_csv)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # 4) metadata 기록
    key = cfg.get("metadata_key", "cpi")
    update_meta(METADATA_PATH, key, {
        "saved_file": out_csv,
        "source_url": url,
        "rows": int(df.shape[0]),
        "max_date": df["date"].max(),
    })

    print("✅ CPI 저장:", out_csv, "rows:", len(df), "max_date:", df["date"].max())