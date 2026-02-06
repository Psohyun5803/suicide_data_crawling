#소비자 물가 등락률을 가져오는 consumer_price_change_index collector 입니다 
from collectors.common import build_url_with_dynamic_period, fetch_to_df,replace_latest_dated_file
from utils.file_utils import ensure_parent_dir
from utils.metadata import update_meta
from parser.year_to_month import expand_year_to_months

import pandas as pd

METADATA_PATH = "../data/metadata/metadata.json"

def run(cfg: dict):
   
     # 1) 수집
    url = build_url_with_dynamic_period(cfg["openapi_url"], cfg.get("start_ym",))
    raw = fetch_to_df(url)
    
    # 2) 전처리(collector에 포함)
    raw["C1_NM"] = raw["C1_NM"].astype(str).str.strip()
    raw = raw[raw["C1_NM"].eq("총지수")].copy()

    df = raw[["PRD_DE", "DT"]].copy()
    df.columns = df.columns.astype(str).str.strip()
    df = df.rename(columns={
    "PRD_DE": "date",
    "DT": "소비자물가등락률"
    })

    if df["date"].str.len().iloc[0] == 6:
        df["date"] = pd.to_datetime(df["date"], format="%Y%m").dt.strftime("%Y-%m")
    else:
        df["date"] = pd.to_datetime(df["date"], format="%Y").dt.strftime("%Y")

    df  = expand_year_to_months(df, year_col="date", value_cols = None) # 연간데이터를 복사해서 월별데이터로 변환 
    # 3) 저장
    out_csv = cfg["output_csv"]
    ensure_parent_dir(out_csv)
    # 1) 이전 latest_날짜 파일 제거 + 오늘 파일 경로 생성
    out_csv = replace_latest_dated_file(out_csv)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # 4) metadata 기록
    key = cfg.get("metadata_key", "consumer_price_change_index")
    update_meta(METADATA_PATH, key, {
        "saved_file": out_csv,
        "source_url": url,
        "rows": int(df.shape[0]),
        "max_date": df["date"].max(),
    })

    print("✅ Consumer_Price_Change_Index 저장:", out_csv, "rows:", len(df), "max_date:", df["date"].max())