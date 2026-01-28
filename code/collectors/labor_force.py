# 경제활동인구, 비경제활동인구, 취업자 , 실업자, 경제활동참가율, 실업률, 고용률을 수집하는 labor_force collector 입니다 
import pandas as pd
from pathlib import Path

from collectors.common import build_url_with_dynamic_period, fetch_to_df,replace_latest_dated_file
from utils.file_utils import ensure_parent_dir
from utils.metadata import update_meta

METADATA_PATH = "../data/metadata/metadata.json"


def normalize_item(name: str) -> str:
    s = str(name).strip()
    if "실업률" in s: return "실업률"
    if "고용률" in s: return "고용률"
    if "경제활동참가율" in s: return "경제활동참가율"
    return s


def run(cfg: dict) -> dict:
    # 1) 수집
    start_ym = cfg.get("start_ym",)
    url = build_url_with_dynamic_period(cfg["openapi_url"], start_ym)
    df = fetch_to_df(url)
    df.columns = df.columns.astype(str).str.strip()

    date_col = "PRD_DE"
    value_col = "DT"
    item_col = "ITM_NM"
    group_col = "C1_NM"
    if item_col not in df.columns:
        raise KeyError(f"ITM_NM 컬럼이 없습니다. columns={list(df.columns)}")
    if group_col not in df.columns:
        raise KeyError("‘계’를 담는 분류 컬럼(C1_NM 등)을 찾지 못했습니다.")
 
    # 3) 계만 필터
    df2 = df[df[group_col].astype(str).str.strip().eq("계")].copy()

    # 4) 필요한 항목만 필터 
    df2[item_col] = df2[item_col].astype(str).str.strip()
    pat = "경제활동인구|비경제활동인구|취업자|실업자|경제활동참가율|실업률|고용률"
    df2 = df2[df2[item_col].str.contains(pat, na=False)].copy()

    # 5) 타입 정리
    df2[value_col] = pd.to_numeric(df2[value_col], errors="coerce")
    df2[date_col] = df2[date_col].astype(str).str.strip()

    # 6) 날짜 정리
    if df2[date_col].str.len().iloc[0] == 6:
        df2["date"] = pd.to_datetime(df2[date_col], format="%Y%m", errors="coerce").dt.strftime("%Y-%m")
    else:
        df2["date"] = pd.to_datetime(df2[date_col], format="%Y", errors="coerce").dt.strftime("%Y")

    # 7) 피벗으로 행, 열 원하는 형식으로 정리
    wide = (
        df2.pivot_table(index="date", columns=item_col, values=value_col, aggfunc="first")
           .sort_index()
    )

    # 8) 항목명 정규화
    wide = wide.rename(columns=normalize_item)

    # 9) 컬럼 선택
    wanted_cols = ["경제활동인구", "비경제활동인구", "취업자", "실업자", "실업률", "고용률", "경제활동참가율"]
    final_cols = [c for c in wanted_cols if c in wide.columns]
    wide = wide[final_cols]

    wide = wide.reset_index()  # date를 컬럼으로

    # 10) 저장 (latest_날짜 파일 1개만 유지)
    base_path = cfg["output_csv"] 
    ensure_parent_dir(base_path)
    out_csv = replace_latest_dated_file(base_path) # 기존 파일 없애고 새 파일로 대체한다 

    wide.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # 11) metadata 기록
    key = cfg.get("metadata_key", "labor_force")
    update_meta(METADATA_PATH, key, {
        "saved_file": out_csv,
        "source_url": url,
        "rows": int(wide.shape[0]),
        "max_date": wide["date"].max() if not wide.empty else None,
    })

    print("✅ Labor_Force 저장:", out_csv, "rows:", len(wide), "max_date:", (wide["date"].max() if not wide.empty else None))
    return {
        "saved_to": out_csv,
        "rows": int(wide.shape[0]),
        "max_date": wide["date"].max() if not wide.empty else None,
    } 