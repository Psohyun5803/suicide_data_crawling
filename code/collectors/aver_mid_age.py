# 중위연령, 평균연령을 가져오는 aver_mid_age collector입니다 
from collectors.common import build_url_with_dynamic_period, fetch_to_df,replace_latest_dated_file
from utils.file_utils import ensure_parent_dir
from utils.metadata import update_meta

import pandas as pd

METADATA_PATH = "../data/metadata/metadata.json"

def run(cfg: dict):
   
    url = build_url_with_dynamic_period(cfg["openapi_url"], cfg.get("start_ym",))
    raw = fetch_to_df(url)
    

    # 전처리
    raw.columns = raw.columns.astype(str).str.strip()
    raw = raw[raw["C2_NM"] == "전국"].copy()

    date_col = "PRD_DE"
    item_col =  "ITM_NM"
    value_col = "DT"

    raw= raw[[date_col,item_col,value_col]].copy()
    
    raw = raw[raw[item_col].isin(["중위연령", "평균연령"])].copy()
    
    
    raw[value_col] = pd.to_numeric(raw[value_col],errors= "coerce")
    raw[item_col] = raw[item_col].astype(str).str.strip()
    raw[date_col] = raw[date_col].astype(str).str.strip()
    
    wide = raw.pivot_table(
        index=date_col,
        columns=item_col,
        values=value_col,
        aggfunc="first"
    ).sort_index()
    wide = wide.reset_index()
    wide = wide.rename(columns = {date_col : "date"})

    
    if wide["date"].str.len().iloc[0] == 6:
        wide["date"] = pd.to_datetime(wide["date"], format="%Y%m").dt.strftime("%Y-%m")
    else:
        wide["date"] = pd.to_datetime(wide["date"], format="%Y").dt.strftime("%Y")
    
    # 3) 저장
    out_csv = cfg["output_csv"]
    ensure_parent_dir(out_csv)
    # 3-1) 이전 latest_날짜 파일 제거 + 오늘 파일 경로 생성
    out_csv = replace_latest_dated_file(out_csv)
    wide.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # 3-2) metadata 기록
    key = cfg.get("metadata_key", "aver_mid_age")
    update_meta(METADATA_PATH, key, {
        "saved_file": out_csv,
        "source_url": url,
        "rows": int(wide.shape[0]),
        "max_date": wide["date"].max(),
    })

    print("✅ Aver_Mid_Age 저장:", out_csv, "rows:", len(wide), "max_date:", wide["date"].max())