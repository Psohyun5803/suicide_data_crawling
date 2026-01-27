# suicide-data-crawling

KOSIS / 지표누리 데이터를 수집하여  
인구, 경제, 노동 관련 지표를 자동으로 크롤링하는 프로젝트입니다.

## 📁 프로젝트 구조

suicide_data_crawling/
├─ code/
│ ├─ main.py # 실행 진입점
│ ├─ config.yaml # 수집 설정 파일
│ ├─ collectors/ # 지표별 수집 로직
│ └─ utils/ # 공통 유틸
├─ data/ # 실행 시 결과 CSV 생성 (git 제외)
└─ .gitignore

## 📦 필수 설치 패키지

이 프로젝트는 아래 패키지를 사용합니다.

- `requests`
- `pandas`
- `PyYAML`

## 🚀 실행 방법 

### 1️⃣ 저장소 클론
```bash
git clone https://github.com/Psohyun5803/suicide-data-crawling.git
cd suicide-data-crawling

###2️⃣ 가상환경 생성 및 활성화
가상환경 생성 및 활성화
Windows (CMD)
python -m venv .venv
.venv\Scripts\activate

###3️⃣ 패키지 설치
pip install --upgrade pip
pip install -r requirements.txt


만약 requirements.txt가 없다면:

pip install requests pandas pyyaml
###4️⃣ 실행
cd code
python main.py

