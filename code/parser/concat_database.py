import json
import pandas as pd
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
    # 날짜 파싱 (월 포맷 고정)
    df[date_col] = pd.to_datetime(df[date_col], format="%Y-%m", errors="raise")

    start_date = pd.to_datetime(start_date, format="%Y-%m")
    end_date = pd.to_datetime(end_date, format="%Y-%m")

    df = df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]
    return df.sort_values(date_col).reset_index(drop=True)

def merge_all_monthly_from_metadata(metadata_path = METADATA_PATH, start_date ="2020-01", date_col="date"):
    with open(metadata_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    dfs = []
    common_min_date = find_min_data()
    for key, info in meta.items():
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

def run():
    df = merge_all_monthly_from_metadata()
    df.to_csv("../data/concatdatatest.csv", index=False, encoding="utf-8-sig")
    
