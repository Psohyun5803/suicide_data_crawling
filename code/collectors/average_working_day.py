from collectors.common import build_url_with_dynamic_period, fetch_to_df,replace_latest_dated_file
from utils.file_utils import ensure_parent_dir
from utils.metadata import update_meta

import pandas as pd

METADATA_PATH = "../data/metadata/metadata.json"

def run(cfg: dict):
   
    url =cfg["openapi_url"] #지표누리 api는 항상 최신 값을 반환 
    df = fetch_to_df(url)
    df.columns = df.columns.astype(str).str.strip()
    df = df[df[date_col].str.len() == 6].copy() # monthly 지표만 받아오게 됩니다
    
    
    date_col =  "시점"
    value_col =  "값"
    item_col = "항목이름"

    df = df[[date_col,item_col,value_col]].copy()
    df = df[df[item_col].isin(["근로일수"])].copy()

    df[value_col] = pd.to_numeric(df[value_col],errors= "coerce")
    df[item_col] = df[item_col].astype(str).str.strip()
    df[date_col] = df[date_col].astype(str).str.strip()


    wide = df.pivot_table(
        index="시점",
        columns=item_col,
        values=value_col,
        aggfunc="first"
    ).sort_index()
    wide = wide.reset_index()
  
    wide = wide.rename(columns = {"시점":"date"})
    if wide["date"].str.len().iloc[0] == 6:
        wide["date"] = pd.to_datetime(wide["date"], format="%Y%m").dt.strftime("%Y-%m").astype(str).str.replace("-", "_", regex=False)
    else:
        wide["date"] = pd.to_datetime(wide["date"], format="%Y").dt.strftime("%Y").astype(str).str.replace("-", "_", regex=False)
        
    out_csv = cfg["output_csv"]
    ensure_parent_dir(out_csv)
    # 1) 이전 latest_날짜 파일 제거 + 오늘 파일 경로 생성
    out_csv = replace_latest_dated_file(out_csv)
    wide.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # 4) metadata 기록
    key = cfg.get("metadata_key", "average_working_day")
    update_meta(METADATA_PATH, key, {
        "saved_file": out_csv,
        "source_url": url,
        "rows": int(wide.shape[0]),
        "max_date": wide["date"].max(),
    })

    print("✅ Average_Working_Day 저장:", out_csv, "rows:", len(wide), "max_date:", wide["date"].max())
   