# 가계신용, 가계대출, 판매신용을 가져오는 labor_force collector 입니다 
from collectors.common import build_url_with_dynamic_period, fetch_to_df,replace_latest_dated_file 
from utils.file_utils import ensure_parent_dir
from utils.metadata import update_meta
from parser.apply_denton import denton_with_dates
import pandas as pd

METADATA_PATH = "../data/metadata/metadata.json"

def run(cfg: dict):
    start_ym = cfg.get("start_ym", "200204")
    url = build_url_with_dynamic_period(cfg["openapi_url"], start_ym)
    df = fetch_to_df(url)
    df.columns = df.columns.astype(str).str.strip()

    date_col = "PRD_DE"
    value_col = "DT"
    item_col = "C1_NM"

    # 2) 전처리
    df = df[[date_col, item_col, value_col]].copy()

    wanted = ["가계신용", "가계대출", "판매신용"]
    df[item_col] = df[item_col].astype(str).str.strip()
    df = df[df[item_col].isin(wanted)].copy()

    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df[date_col] = df[date_col].astype(str).str.strip()
    

    wide = (
        df.pivot_table(index=date_col, columns=item_col, values=value_col, aggfunc="first")
          .sort_index()
    )
    wide = wide[[c for c in wanted if c in wide.columns]].reset_index()
    wide = wide.rename(columns={date_col: "date"})
    wide["date"] = pd.to_datetime(wide["date"], format="%Y%m").dt.strftime("%Y-%m")
    wide = denton_with_dates(wide ,date_col="date",value_cols=None) ## denton 추가 
    # 3) 저장
    out_csv = cfg["output_csv"]
    ensure_parent_dir(out_csv)
    # 1) 이전 latest_날짜 파일 제거 + 오늘 파일 경로 생성
    out_csv = replace_latest_dated_file(out_csv)
    wide.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # 4) metadata 기록
    key = cfg.get("metadata_key", "loan")
    update_meta(METADATA_PATH, key, {
        "saved_file": out_csv,
        "source_url": url,
        "rows": int(wide.shape[0]),
        "max_date": wide["date"].max() if not wide.empty else None,
    })

    print("✅ Loan 저장:", out_csv, "rows:", len(wide), "max_date:", wide["date"].max() if not wide.empty else None)
    