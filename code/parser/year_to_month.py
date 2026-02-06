import pandas as pd

def expand_year_to_months(df_year, year_col="date", value_cols=None):
    """
    연도 데이터 -> 월별 데이터 (값 단순 복제)
    """
    if value_cols is None:
        value_cols = [c for c in df_year.columns if c != year_col]

    rows = []
    for _, row in df_year.iterrows():
        # 각 행을 순회한다 
        year = int(row[year_col])
        for m in range(1, 13):
            new_row = {"date": f"{year}-{m:02d}"}
            for col in value_cols:
                new_row[col] = row[col]
            rows.append(new_row)

    df_month = pd.DataFrame(rows)
    return df_month