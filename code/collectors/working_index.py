# working_index.py  columns =[전체임금총액, 전체근로일수, 전체근로시간]
from collectors.common import build_url_with_dynamic_period, fetch_to_df,replace_latest_dated_file,fetch_kosis_by_6m
from utils.file_utils import ensure_parent_dir
from utils.metadata import update_meta

import pandas as pd
import datetime as dt

METADATA_PATH = "../data/metadata/metadata.json"


def run(cfg: dict):
    start_ym = cfg.get("start_ym", "202001")
    end_ym = cfg.get("end_ym")  # 없으면 오늘 기준 YYYYMM
    if not end_ym:
        today = dt.date.today()
        end_ym = f"{today.year:04d}{today.month:02d}"

    
    raw = fetch_kosis_by_6m(
        openapi_url=cfg["openapi_url"],
        start_ym=start_ym,
        end_ym=end_ym,
        fetch_to_df=fetch_to_df,
        sleep_s=0.3
    ) # 40000셀 제한으로 인해 6개월 단위로 끊어서 데이터를 가져옵니다
    
    
    
    raw.columns = raw.columns.astype(str).str.strip()
    raw = raw[raw["C2_NM"] == "전규모(1인이상)"].copy() # 계 데이터만 불러옵니다 

    date_col = "PRD_DE"
    item_col= "ITM_NM"
    value_col = "DT"
    
    wanted = ["전체임금총액", "전체근로일수", "전체근로시간"]
    raw = raw[raw[item_col].isin(wanted)].copy()

    raw[value_col] = pd.to_numeric(raw[value_col], errors="coerce")
    raw[date_col] = raw[date_col].astype(str).str.strip()
    

    wide = (
        raw.pivot_table(index=date_col, columns=item_col, values=value_col, aggfunc="first")
          .sort_index()
    )
    wide = wide[[c for c in wanted if c in wide.columns]].reset_index()
    wide = wide.rename(columns={date_col: "date"})
    wide["date"] = pd.to_datetime(wide["date"], format="%Y%m").dt.strftime("%Y-%m")

    
    # 3) 저장
    out_csv = cfg["output_csv"]
    ensure_parent_dir(out_csv)
    # 1) 이전 latest_날짜 파일 제거 +  파일 경로 생성
    out_csv = replace_latest_dated_file(out_csv)
    wide.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # 4) metadata 기록
    key = cfg.get("metadata_key", "working_index")
    update_meta(METADATA_PATH, key, {
        "saved_file": out_csv,
        "source_url": cfg["openapi_url"],
        "rows": int(wide.shape[0]),
        "max_date": wide["date"].max() if not wide.empty else None,
    })

    print("✅ Working_Index 저장:", out_csv, "rows:", len(wide), "max_date:", wide["date"].max() if not wide.empty else None) 
    