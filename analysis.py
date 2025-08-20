

import requests
import pandas as pd
import json
import numpy as np

# ===================================================================
# 여기에 다시 한번 DART API 키를 입력해주세요.
api_key = '2725c0dbcde7679f9c840041541f9b6c9adb9d30'
# ===================================================================

# 분석할 회사 및 기간 설정
company_name = '삼성전자'
corp_code = '00126380' # 삼성전자 고유번호
start_year = 2020
end_year = 2024

print(f"{start_year}년부터 {end_year}년까지 {company_name}의 재무 정보 분석을 시작합니다.")

# 모든 재무 정보를 저장할 리스트
all_financial_data = []

# 계정 과목 이름 (DART API가 반환하는 이름)
ACCOUNTS_TO_EXTRACT = [
    '자산총계', '부채총계', '자본총계', 
    '매출액', '영업이익', '당기순이익'
]

try:
    # 지정된 기간 동안 연도별로 반복
    for year in range(start_year, end_year + 1):
        print(f"--- {year}년 데이터 조회 시작 ---")
        # 분기별 보고서 코드: 1분기, 반기, 3분기, 사업보고서(4분기)
        report_codes = {
            '1분기': '11013',
            '2분기': '11012',
            '3분기': '11014',
            '4분기': '11011'
        }

        for quarter, report_code in report_codes.items():
            print(f"  {year}년 {quarter} 보고서 데이터를 조회 중...")
            
            url = (f'https://opendart.fss.or.kr/api/fnlttMultiAcnt.json?'
                   f'crtfc_key={api_key}&corp_code={corp_code}'
                   f'&bsns_year={year}&reprt_code={report_code}')
            
            response = requests.get(url)
            if response.status_code != 200:
                print(f"  -> 서버 오류 발생 (코드: {response.status_code})")
                continue

            data = response.json()
            if data['status'] != '000':
                print(f"  -> API 오류: {data['message']}")
                continue

            # 데이터 추출
            extracted_info = {
                '기업명': company_name,
                '날짜': f'{year}',
                '분기': f'{quarter}'
            }

            # 연결재무제표(CFS)에서 필요한 계정 과목 값 찾기
            for item in data['list']:
                if item['fs_div'] == 'CFS' and item['account_nm'] in ACCOUNTS_TO_EXTRACT:
                    extracted_info[item['account_nm']] = item['thstrm_amount']

            all_financial_data.append(extracted_info)
            print(f"  -> {year}년 {quarter} 데이터 추출 완료.")
        print(f"--- {year}년 데이터 조회 완료 ---\n")

    # 리스트를 Pandas DataFrame으로 변환
    df = pd.DataFrame(all_financial_data)

    # 데이터 타입 변환 및 지표 계산
    if not df.empty:
        # 숫자형으로 변환할 컬럼 리스트
        numeric_cols = ['자산총계', '부채총계', '자본총계', '매출액', '영업이익', '당기순이익']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col].fillna(0).astype(str).str.replace(',', ''), errors='coerce')

        # 수익성 상태 및 이익률 계산
        df['수익성 상태'] = df['당기순이익'].apply(lambda x: '흑자' if x > 0 else '적자')
        df['영업이익률'] = df.apply(lambda row: row['영업이익'] / row['매출액'] if row['매출액'] != 0 else 0, axis=1)
        df['순이익률'] = df.apply(lambda row: row['당기순이익'] / row['매출액'] if row['매출액'] != 0 else 0, axis=1)
        
        # ROA 및 ROE 계산
        df['ROA'] = df.apply(lambda row: row['당기순이익'] / row['자산총계'] if row['자산총계'] != 0 else 0, axis=1)
        df['ROE'] = df.apply(lambda row: row['당기순이익'] / row['자본총계'] if row['자본총계'] != 0 else 0, axis=1)

        # --- 미래 예측 기능 추가 ---
        print("\n--- 미래 재무 데이터 예측 시작 ---")
        
        # 예측할 주요 재무 컬럼
        prediction_cols = ['자산총계', '부채총계', '자본총계', '매출액', '영업이익', '당기순이익']
        
        # 시계열 인덱스 생성
        time_index = np.arange(len(df))
        
        # 예측 결과를 저장할 딕셔너리
        future_predictions = {}

        # 다음 8개 분기 예측
        future_steps = 8
        future_index = np.arange(len(df), len(df) + future_steps)

        for col in prediction_cols:
            # 선형 회귀 모델 학습
            coeffs = np.polyfit(time_index, df[col].astype(float), 1)
            model = np.poly1d(coeffs)
            
            # 미래 값 예측
            future_values = model(future_index)
            future_predictions[col] = future_values

        # 예측된 분기 및 날짜 생성
        last_year = int(df['날짜'].iloc[-1])
        last_quarter_str = df['분기'].iloc[-1]
        last_quarter = int(last_quarter_str.replace('분기', ''))
        
        future_quarters = []
        future_dates = []
        current_year = last_year
        current_quarter = last_quarter
        for i in range(future_steps):
            current_quarter += 1
            if current_quarter > 4:
                current_quarter = 1
                current_year += 1
            future_quarters.append(f"{current_quarter}분기")
            future_dates.append(f"{current_year}")

        # 예측 DataFrame 생성
        forecast_df = pd.DataFrame(future_predictions)
        forecast_df['기업명'] = company_name
        forecast_df['날짜'] = future_dates
        forecast_df['분기'] = future_quarters
        forecast_df['구분'] = '예측'

        # 예측된 값을 기반으로 파생 지표 계산
        forecast_df['수익성 상태'] = forecast_df['당기순이익'].apply(lambda x: '흑자' if x > 0 else '적자')
        forecast_df['영업이익률'] = forecast_df.apply(lambda row: row['영업이익'] / row['매출액'] if row['매출액'] != 0 else 0, axis=1)
        forecast_df['순이익률'] = forecast_df.apply(lambda row: row['당기순이익'] / row['매출액'] if row['매출액'] != 0 else 0, axis=1)
        forecast_df['ROA'] = forecast_df.apply(lambda row: row['당기순이익'] / row['자산총계'] if row['자산총계'] != 0 else 0, axis=1)
        forecast_df['ROE'] = forecast_df.apply(lambda row: row['당기순이익'] / row['자본총계'] if row['자본총계'] != 0 else 0, axis=1)
        
        # 기존 DataFrame에 '구분' 컬럼 추가
        df['구분'] = '실적'

        # 기존 DataFrame과 예측 DataFrame 합치기
        df = pd.concat([df, forecast_df], ignore_index=True)
        
        print("--- 미래 재무 데이터 예측 완료 ---")

        # --- 컬럼 순서 정리 및 파일 저장 ---
        final_columns = ['기업명', '날짜', '분기', '구분', '자산총계', '부채총계', '자본총계', 
                         '매출액', '영업이익', '당기순이익', '수익성 상태', '영업이익률', '순이익률', 'ROA', 'ROE']
        df = df.reindex(columns=final_columns)

        output_filename = f'{company_name}_{start_year}-{end_year}_재무분석_requests.xlsx'
        df.to_excel(output_filename, index=False, float_format='%.4f')

        print(f"\n분석 및 예측 완료! 모든 결과가 '{output_filename}' 파일로 저장되었습니다.")

except Exception as e:
    print(f"스크립트 실행 중 오류가 발생했습니다: {e}")

