import json
import pandas as pd
from collectors.common import replace_latest_dated_file
from utils.file_utils import ensure_parent_dir
from utils.metadata import update_meta

METADATA_PATH = "../data/metadata/metadata.json"

def find_min_data(metadata_path = METADATA_PATH): # 갱신이 가장 오래전에 된 날짜 찾기 
    with open(metadata_path, "r", encoding="utf-8") as f:
        meta = json.load(f)
    max_dates =[]
    for k,v in meta.items():
        if "max_date" in v and v["max_date"]:
            max_dates.append(pd.to_datetime(v["max_date"],format="%Y-%m",errors="raise"))
    common_min_date = min(max_dates)
    #print("기준 월",common_min_date)
    return common_min_date 

def load_and_trim_monthly(csv_path, start_date, end_date, date_col="date"):
    df = pd.read_csv(csv_path)

    # 1) 날짜 → Timestamp (혼합 포맷 안전)
    df[date_col] = pd.to_datetime(df[date_col], format="mixed", errors="raise")

    start_date = pd.to_datetime(start_date, format="mixed")
    end_date = pd.to_datetime(end_date, format="mixed")

    # 2) 기간 필터링 (Timestamp끼리 비교)
    df = df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]

    # 3) 최종 포맷 통일 (문자열로 바꾸는 건 마지막에)
    df[date_col] = df[date_col].dt.strftime("%Y-%m")

    return df.sort_values(date_col).reset_index(drop=True)
    """
    df = pd.read_csv(csv_path)
    # 날짜 파싱 (월 포맷 고정)
    df[date_col] = pd.to_datetime(df[date_col], format="%Y-%m", errors="raise")

    start_date = pd.to_datetime(start_date, format="%Y-%m")
    end_date = pd.to_datetime(end_date, format="%Y-%m")

    df = df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]
    return df.sort_values(date_col).reset_index(drop=True)
    """

def merge_all_monthly_from_metadata(metadata_path = METADATA_PATH, start_date ="2020-01", date_col="date"):
    with open(metadata_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    dfs = []
    common_min_date = find_min_data()
    for key, info in meta.items():
        if key == "suicide_base_data":
            continue
        csv_path = info["saved_file"]

        df = load_and_trim_monthly(
            csv_path=csv_path,
            start_date=start_date,
            end_date=common_min_date,
            date_col=date_col
        ) # start ~ end 기간으로 데이터를 다 자른다 
        dfs.append(df)

    # inner join: 공통 기간만 남김
    df_merged = dfs[0]
    for df in dfs[1:]:
        df_merged = df_merged.merge(df, on=date_col, how="inner")

    return df_merged.sort_values(date_col).reset_index(drop=True)

def run(cfg:dict):
    start_date = cfg.get("start_date", "2020-01")
    output_csv_tpl = cfg["output_csv"]              # "../data/suicide_base_data_2020_{max_year}.csv"
    metadata_key = cfg.get("metadata_key", "suicide_base_data")

    df = merge_all_monthly_from_metadata(start_date=start_date)

    max_year = pd.to_datetime(df["date"]).dt.year.max()

    # 🔥 파일명 동적 치환
    out_csv = output_csv_tpl.format(max_year=max_year)

    # 3) 저장 (기존 collector 스타일 그대로)
    ensure_parent_dir(out_csv)
    out_csv = replace_latest_dated_file(out_csv)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # 4) metadata 기록 (🔥 Timestamp → 문자열 변환)
    max_date_str = (
        pd.to_datetime(df["date"]).max().strftime("%Y-%m")
        if not df.empty else None
    )

    update_meta(METADATA_PATH, metadata_key, {
        "saved_file": out_csv,
        "rows": int(df.shape[0]),
        "max_date": max_date_str,     # ✅ JSON 직렬화 안전
        "start_date": start_date,     # 문자열  
    })

    print(
        "✅ Suicide_Base_Data 저장:",
        out_csv,
        "rows:", len(df),
        "max_date:", max_date_str,
    )
    """
    df = merge_all_monthly_from_metadata()

    max_year = df["date"].max().year
    df.to_csv(f"../data/suicide_base_data_2020_{max_year}.csv", index=False, encoding="utf-8-sig")
    print ( "✅ Suicide_Base_Data 저장" )
    """
    
