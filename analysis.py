import requests
import pandas as pd
import json
import numpy as np

# ===================================================================
# 여기에 다시 한번 DART API 키를 입력해주세요.
api_key = '2725c0dbcde7679f9c840041541f9b6c9adb9d30'
# ===================================================================

# 분석할 기간 설정
start_year = 2020
end_year = 2024

# 분석할 회사 목록: (회사명, DART 고유번호)
companies_to_analyze = [
    ('삼성전자', '00126380'),
    ('LG전자', '00401731')
]

# 계정 과목 이름 (DART API가 반환하는 이름)
ACCOUNTS_TO_EXTRACT = [
    '자산총계', '부채총계', '자본총계', 
    '매출액', '영업이익', '당기순이익'
]

def analyze_company(company_name, corp_code, start_year, end_year):
    """
    지정된 회사의 재무 데이터를 분석하고 예측하여 DataFrame을 반환하는 함수
    """
    print(f"\n{'='*50}")
    print(f"{start_year}년부터 {end_year}년까지 {company_name}의 재무 정보 분석을 시작합니다.")
    print(f"{'='*50}\n")

    all_financial_data = []

    try:
        for year in range(start_year, end_year + 1):
            print(f"--- {company_name}: {year}년 데이터 조회 시작 ---")
            report_codes = {'1분기': '11013', '2분기': '11012', '3분기': '11014', '4분기': '11011'}

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

                extracted_info = {'기업명': company_name, '날짜': f'{year}', '분기': f'{quarter}'}
                for item in data['list']:
                    if item['fs_div'] == 'CFS' and item['account_nm'] in ACCOUNTS_TO_EXTRACT:
                        extracted_info[item['account_nm']] = item['thstrm_amount']
                all_financial_data.append(extracted_info)
                print(f"  -> {year}년 {quarter} 데이터 추출 완료.")
            print(f"--- {company_name}: {year}년 데이터 조회 완료 ---\n")

        df = pd.DataFrame(all_financial_data)

        if df.empty:
            return pd.DataFrame() # 데이터가 없으면 빈 DataFrame 반환

        numeric_cols = ['자산총계', '부채총계', '자본총계', '매출액', '영업이익', '당기순이익']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col].fillna(0).astype(str).str.replace(',', ''), errors='coerce')

        # --- 지표 계산 ---
        df['수익성 상태'] = df['당기순이익'].apply(lambda x: '흑자' if x > 0 else '적자')
        df['영업이익률'] = df.apply(lambda row: (row['영업이익'] / row['매출액']) * 100 if row['매출액'] != 0 else 0, axis=1)
        df['순이익률'] = df.apply(lambda row: (row['당기순이익'] / row['매출액']) * 100 if row['매출액'] != 0 else 0, axis=1)
        df['ROA'] = df.apply(lambda row: (row['당기순이익'] / row['자산총계']) * 100 if row['자산총계'] != 0 else 0, axis=1)
        df['ROE'] = df.apply(lambda row: (row['당기순이익'] / row['자본총계']) * 100 if row['자본총계'] != 0 else 0, axis=1)
        df['영업비용'] = df['매출액'] - df['영업이익']
        df['영업비용률'] = df.apply(lambda row: (row['영업비용'] / row['매출액']) * 100 if row['매출액'] != 0 else 0, axis=1)

        print(f"\n--- {company_name}: 미래 재무 데이터 예측 시작 ---")
        prediction_cols = ['자산총계', '부채총계', '자본총계', '매출액', '영업이익', '당기순이익']
        time_index = np.arange(len(df))
        future_predictions = {}
        future_steps = 8
        future_index = np.arange(len(df), len(df) + future_steps)

        for col in prediction_cols:
            if not df[col].dropna().empty:
                coeffs = np.polyfit(time_index, df[col].astype(float), 1)
                model = np.poly1d(coeffs)
                future_values = model(future_index)
                future_predictions[col] = future_values
            else:
                future_predictions[col] = [np.nan] * future_steps

        last_year = int(df['날짜'].iloc[-1])
        last_quarter = int(df['분기'].iloc[-1].replace('분기', ''))
        
        future_quarters, future_dates = [], []
        current_year, current_quarter = last_year, last_quarter
        for _ in range(future_steps):
            current_quarter += 1
            if current_quarter > 4:
                current_quarter = 1
                current_year += 1
            future_quarters.append(f"{current_quarter}분기")
            future_dates.append(f"{current_year}")

        forecast_df = pd.DataFrame(future_predictions)
        forecast_df['기업명'] = company_name
        forecast_df['날짜'] = future_dates
        forecast_df['분기'] = future_quarters
        forecast_df['구분'] = '예측'

        # --- 예측 데이터에 대한 지표 계산 ---
        forecast_df['수익성 상태'] = forecast_df['당기순이익'].apply(lambda x: '흑자' if x > 0 else '적자')
        forecast_df['영업이익률'] = forecast_df.apply(lambda row: (row['영업이익'] / row['매출액']) * 100 if row['매출액'] != 0 else 0, axis=1)
        forecast_df['순이익률'] = forecast_df.apply(lambda row: (row['당기순이익'] / row['매출액']) * 100 if row['매출액'] != 0 else 0, axis=1)
        forecast_df['ROA'] = forecast_df.apply(lambda row: (row['당기순이익'] / row['자산총계']) * 100 if row['자산총계'] != 0 else 0, axis=1)
        forecast_df['ROE'] = forecast_df.apply(lambda row: (row['당기순이익'] / row['자본총계']) * 100 if row['자본총계'] != 0 else 0, axis=1)
        forecast_df['영업비용'] = forecast_df['매출액'] - forecast_df['영업이익']
        forecast_df['영업비용률'] = forecast_df.apply(lambda row: (row['영업비용'] / row['매출액']) * 100 if row['매출액'] != 0 else 0, axis=1)
        
        df['구분'] = '실적'
        combined_df = pd.concat([df, forecast_df], ignore_index=True)
        print(f"--- {company_name}: 미래 재무 데이터 예측 완료 ---")
        return combined_df

    except Exception as e:
        print(f"\n*** {company_name} 처리 중 오류가 발생했습니다: {e} ***")
        return pd.DataFrame() # 오류 발생 시 빈 DataFrame 반환

# ===================================================================
# 메인 실행 로직
# ===================================================================
if __name__ == "__main__":
    all_results = []
    for name, code in companies_to_analyze:
        result_df = analyze_company(name, code, start_year, end_year)
        if not result_df.empty:
            all_results.append(result_df)

    if all_results:
        # 모든 결과를 하나의 DataFrame으로 합치기
        final_df = pd.concat(all_results, ignore_index=True)

        # --- 회사별 성장률 계산 ---
        final_df['매출액_성장률'] = final_df.groupby('기업명')['매출액'].pct_change(periods=4) * 100
        final_df['영업이익_성장률'] = final_df.groupby('기업명')['영업이익'].pct_change(periods=4) * 100
        final_df['당기순이익_성장률'] = final_df.groupby('기업명')['당기순이익'].pct_change(periods=4) * 100

        # --- 컬럼 순서 정리 및 파일 저장 ---
        final_columns = [
            '기업명', '날짜', '분기', '구분', '자산총계', '부채총계', '자본총계', 
            '매출액', '영업이익', '영업비용', '당기순이익', '수익성 상태', 
            '매출액_성장률', '영업이익_성장률', '당기순이익_성장률', '영업이익률', '순이익률', '영업비용률', 
            'ROA', 'ROE'
        ]
        final_df = final_df.reindex(columns=final_columns)

        # 지정된 파일명으로 저장
        output_filename = f'{companies_to_analyze[0][0]}_{start_year}-{end_year}_재무분석_requests.xlsx'
        final_df.to_excel(output_filename, index=False, float_format='%.4f')

        print(f"\n\n{'='*50}")
        print(f"모든 분석 및 예측 완료! 결과가 '{output_filename}' 파일로 저장되었습니다.")
        print(f"{'='*50}")
    else:
        print("\n\n분석할 데이터가 없거나 오류가 발생하여 파일을 생성하지 않았습니다.")