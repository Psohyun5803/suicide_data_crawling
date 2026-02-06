import numpy as np
import pandas as pd
import matplotlib.pyplot as plt 

def build_A(T, m=3):
    """
    제약행렬 A: 분기 -> 월 합계 보존
    """
    A = np.zeros((T, T * m))
    for i in range(T):
        A[i, i*m:(i+1)*m] = 1
    return A


def build_D(n):
    """
    1차 차분 행렬 D
    """
    D = np.zeros((n - 1, n))
    for i in range(n - 1):
        D[i, i] = -1
        D[i, i + 1] = 1
    return D

def quarter_label_to_months(q_label: str):
    # 분기 라벨 -> 월 라벨 
    year, q = q_label.split("-")
    q = int(q)

    if q not in [1, 2, 3, 4]:
        raise ValueError(f"Invalid quarter label: {q_label}")

    start_month = (q - 1) * 3 + 1  # 1,4,7,10

    months = []
    for m in range(start_month, start_month + 3):
        months.append(f"{year}-{m:02d}")

    return months


def apply_denton(y_quarterly, m=3):
    y = np.asarray(y_quarterly).reshape(-1) # 원래 쿼터 데이터
    T = len(y)
    n = T * m # 생성해야할 월 개수 

    A = build_A(T, m)
    D = build_D(n)
    DTD = D.T @ D

    zero = np.zeros((A.shape[0], A.shape[0]))
    # 라그랑주 승수법 적용 후 연립방정식 식 도출 
    KKT = np.block([
        [DTD, A.T],
        [A, zero]
    ]) # 좌변 정의 

    rhs = np.concatenate([np.zeros(n), y]) #우변 정의

    sol = np.linalg.solve(KKT, rhs) # 연립방정식 풀이 
    x_monthly = sol[:n]

    return x_monthly

def denton_with_dates(df_quarter, date_col="date", value_cols="value" ): 
    """
    df_quarter: 분기 데이터 (여러 지표 컬럼)
    date_col: 분기 라벨 컬럼
    value_cols: Denton 적용할 컬럼 리스트 (None이면 date_col 제외 전부)
    """
    df_quarter = df_quarter.sort_values(date_col).reset_index(drop=True)

    if value_cols is None:
        value_cols = [c for c in df_quarter.columns if c != date_col]

    # 분기 -> 월 인덱스 생성
    month_dates = []
    for q_label in df_quarter[date_col]:
        month_dates.extend(quarter_label_to_months(q_label))

    df_month = pd.DataFrame({"date": month_dates})

    # 컬럼별로 Denton 적용
    for col in value_cols:
        y = pd.to_numeric(df_quarter[col], errors="raise").values
        x_month = apply_denton(y)
        df_month[col] = x_month
    #plot_all_columns_one_figure(df_month)
    return df_month

#시각화 테스트 용 함수 
def plot_all_columns_one_figure(df_month, date_col="date", title="All indicators (Monthly, Denton)"):
    cols = [c for c in df_month.columns if c != date_col]
    plt.rcParams["font.family"] = "Malgun Gothic"   # 맑은 고딕
    plt.rcParams["axes.unicode_minus"] = False      # 마이너스 기호 깨짐 방지
    plt.figure(figsize=(12, 5))

    for col in cols:
        plt.plot(df_month[date_col], df_month[col], marker="o", label=col)

    plt.xticks(rotation=45)
    plt.legend()
    plt.title(title)
    plt.tight_layout()
    plt.show()        
            
    

    