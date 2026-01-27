# resident_population.py  columns =[총인구수, 0-14세 구성비, 15-64세 구성비, 고령화 인구 구성비]

from collectors.common import build_url_with_dynamic_period, fetch_to_df,replace_latest_dated_file,fetch_kosis_by_6m
from utils.file_utils import ensure_parent_dir
from utils.metadata import update_meta

import pandas as pd
import datetime as dt
import re

METADATA_PATH = "../data/metadata/metadata.json"


def run(cfg: dict):

    start_ym = str(cfg.get("start_ym", "200801")).strip()
    end_ym = cfg.get("end_ym")  # 없으면 오늘 기준 YYYYMM
    if not end_ym:
        today = dt.date.today()
        end_ym = f"{today.year:04d}{today.month:02d}"
    end_ym = str(end_ym).strip()

    # raw 로딩
    raw = fetch_kosis_by_6m(
        openapi_url=cfg["openapi_url"],
        start_ym=start_ym,
        end_ym=end_ym,
        fetch_to_df=fetch_to_df,
        sleep_s=0.3
    ) # 40000셀 제한으로 인해 6개월 단위로 끊어서 데이터를 가져옵니다
    
    # 전처리
    raw.columns = raw.columns.astype(str).str.strip()

    # 전국 데이터만
    raw["C1_NM"] = raw["C1_NM"].astype(str).str.strip()
    raw = raw[raw["C1_NM"] == "전국"].copy()

    # 필요한 컬럼 체크
    need = ["PRD_DE", "ITM_NM", "C2_NM", "DT"]
    miss = [c for c in need if c not in raw.columns]

    # 문자열 정리 + 값 숫자화
    for c in ["PRD_DE", "ITM_NM", "C2_NM"]:
        raw[c] = raw[c].astype(str).str.strip()
    raw["DT"] = pd.to_numeric(raw["DT"], errors="coerce")

    # 총인구수만 사용
    raw = raw[raw["ITM_NM"] == "총인구수"].copy()

    # 연령 '계' 제거(있으면)
    raw = raw[raw["C2_NM"] != "계"].copy()
    print(raw.head(5))
    
    # 연령 파싱: "0세" -> 0, "100세 이상" -> 100
    def parse_age(s: str):
        s = str(s)
        m = re.search(r"(\d+)", s)
        return int(m.group(1)) if m else None

    raw["age"] = raw["C2_NM"].apply(parse_age)
    raw = raw.dropna(subset=["age"]).copy()
    raw["age"] = raw["age"].astype(int)
    
    # 월별(YYYYMM)로 연령대 합계 + 총인구(분모: 연령합)
    def agg(g: pd.DataFrame) -> pd.Series:
        return pd.Series({
            "총인구수": g["DT"].sum(),
            "pop_0_14": g.loc[g["age"].between(0, 14), "DT"].sum(),
            "pop_15_64": g.loc[g["age"].between(15, 64), "DT"].sum(),
            "pop_65p": g.loc[g["age"] >= 65, "DT"].sum(),
        })

    out = raw.groupby("PRD_DE").apply(agg).reset_index().sort_values("PRD_DE")

    # 구성비(%)
    out["0~14세 구성비"] = out["pop_0_14"] / out["총인구수"] * 100
    out["15~64세 구성비"] = out["pop_15_64"] / out["총인구수"] * 100
    out["고령인구비율"] = out["pop_65p"] / out["총인구수"] * 100
    
    out = out.drop(columns=["pop_0_14", "pop_15_64", "pop_65p"])

    # 날짜 포맷 정리 (YYYYMM -> YYYY-MM)
    out = out.rename(columns={"PRD_DE": "date"})
    out["date"] = pd.to_datetime(out["date"], format="%Y%m").dt.strftime("%Y-%m")

    out.reset_index()
    print(out.head(5))

    # 저장 

    out_csv = cfg.get("output_csv")
    out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    # 메타데이터 기록
    key = cfg.get("metadata_key", "resident_population")
    update_meta(METADATA_PATH, key, {
        "saved_file": out_csv,
        "source_url": cfg.get("openapi_url"),
        "rows": int(out.shape[0]),
        "max_date": out["date"].max() if not out.empty else None,
        "columns": list(out.columns),
    })

    print("✅ Resident_Population 저장:", out_csv, "rows:", len(out), "max_date:", out["date"].max())
    