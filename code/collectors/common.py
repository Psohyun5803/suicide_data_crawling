# collector 들이 공통적으로 사용하는 함수를 담은 common.py 파일입니다 
import datetime as dt
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode

import pandas as pd
import requests
from pathlib import Path
import time 

def _set_query(url: str, **kwargs) -> str: # url의 쿼리 부분을 가져와서 쿼리를 바꾼 url을 return 합니다
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in kwargs.items():
        q[k] = [str(v)]
    new_q = urlencode(q, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

def _add_months(d: dt.date, months: int) -> dt.date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    return dt.date(y, m, 1)

def iter_ym_chunks_6m(start_ym: str, end_ym: str):
    """
    start_ym/end_ym: 'YYYYMM'
    데이터를 6개월 단위로 끊어서 가져옵니다.
    """
    s = dt.date(int(start_ym[:4]), int(start_ym[4:]), 1)
    e = dt.date(int(end_ym[:4]), int(end_ym[4:]), 1)

    cur = s
    while cur <= e:
        nxt = _add_months(cur, 6)         # 다음 청크 시작(6개월 뒤)
        chunk_end = _add_months(nxt, -1)  # 청크 끝 = 다음 시작의 전월
        if chunk_end > e:
            chunk_end = e

        yield f"{cur.year:04d}{cur.month:02d}", f"{chunk_end.year:04d}{chunk_end.month:02d}"
        cur = nxt

def make_latest_dated_path(base_path: str, date: str | None = None) -> str:

    if date is None:
        date = dt.date.today().strftime("%Y%m%d")

    p = Path(base_path)
    return str(p.with_name(f"{p.stem}_{date}{p.suffix}"))

def fetch_kosis_by_6m(openapi_url: str, start_ym: str, end_ym: str, fetch_to_df, sleep_s: float = 0.3) -> pd.DataFrame:
    frames = []
    for s, e in iter_ym_chunks_6m(start_ym, end_ym):
        url = _set_query(openapi_url, startPrdDe=s, endPrdDe=e)
        df = fetch_to_df(url)
        if df is None or df.empty:
            continue
        frames.append(df)
        time.sleep(sleep_s)  # 서버 과부하/타임아웃 완화

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)

    # 중복 제거(기간 겹침/서버 중복 응답 대비)
    out = out.drop_duplicates()
    return out

def replace_latest_dated_file(base_path: str) -> str: # 갱신 당일의 날짜를 기존 파일명 뒤에 붙입니다
    """
    같은 디렉터리 내의 *_latest_*.csv 를 모두 삭제하고
    오늘 날짜의 *_latest_YYYYMMDD.csv 경로를 반환
    """
    p = Path(base_path)
    pattern = f"{p.stem}_*{p.suffix}"   

    # 기존 latest_날짜 파일 전부 삭제
    for f in p.parent.glob(pattern):
        f.unlink(missing_ok=True)

    # 새 파일 경로 생성
    return make_latest_dated_path(base_path)

def build_url_with_dynamic_period(issued_url: str, start_ym: str) -> str:
    """발급 URL을 템플릿으로 사용하되 start/end만 동적으로"""
    u = urlparse(issued_url)
    q = parse_qs(u.query)

    today = dt.date.today()
    end_ym = f"{today.year}{today.month:02d}"

    q["startPrdDe"] = [start_ym]
    q["endPrdDe"] = [end_ym]

    new_query = urlencode(q, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

def fetch_to_df(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    data = r.json()

    # ✅  정상적으로 데이터를 가져왔을 경우 list[dict]
    if isinstance(data, list):
        return pd.DataFrame(data)

    # ✅ 2) dict 케이스: 에러 or 래핑된 결과
    if isinstance(data, dict):
        # (A) KOSIS "데이터 없음"은 err=30 → 예외가 아니라 빈 DF로 처리합니다 -> fetch_6m을 위해서
        if str(data.get("err", "")).strip() == "30":
            return pd.DataFrame()

        # (B) 그 외 KOSIS 에러는 raise
        # - err / error / message 등이 있으면 에러로 간주
        err_keys = ["err", "errMsg", "error", "message"]
        if any(k in data for k in err_keys):
            raise ValueError(f"KOSIS API returned error dict: {data}")

        # (C) dict 안에 list가 들어있는 형태
        for key in ["data", "items", "rows", "list", "result", "RESULT"]:
            if key in data and isinstance(data[key], list):
                return pd.DataFrame(data[key])

        # (D) (디버깅용)
        return pd.DataFrame([data])

    raise ValueError(f"Unexpected JSON response type: {type(data)}")