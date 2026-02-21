# RE_Crawler

KB부동산 데이터를 기반으로 단지 정보를 수집해 엑셀로 저장하는 프로젝트입니다.

## 환경

- Python 3.11+

## 설치

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
```

## 실행

```bash
python -m re_crawler.main --query "백련산SK뷰아이파크"
```

옵션:

- `--query "단지명"`: 검색어 직접 입력
- `--radius-m 1000`: 주변 단지 검색 반경(미터)
- `--min-households 290`: 최소 세대수 필터
- `--log-level DEBUG`: 상세 로그

## 웹 실행

```bash
streamlit run src/re_crawler/web_app.py
```

- 브라우저에서 단지명/옵션 입력 후 즉시 크롤링
- 결과 테이블 확인
- 엑셀 다운로드
- 시드/함께 수집된 단지 위치 지도 마커 표시

## 출력

- 콘솔: pandas DataFrame 출력
- 파일: `./output/{단지명}_{YYYYMMDD}.xlsx`

## 모듈 구조

- `src/re_crawler/auto_excel.py`: KB API 기반 수집/정규화/엑셀 저장
- `src/re_crawler/main.py`: 실행 엔트리포인트
- `src/re_crawler/parser.py`: 가격/평형/전세가율 계산 유틸
- `src/re_crawler/export.py`: 엑셀 저장 유틸
