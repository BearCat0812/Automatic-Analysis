import requests
import pandas as pd
import numpy as np
import zipfile
import io

# ===================================================================
# 여기에 DART API 키를 입력해주세요.
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

def get_corp_codes(key):
    """
    DART에서 제공하는 전체 회사 고유번호를 다운로드하여 corpcode.xml로 저장합니다.
    """
    print("DART 전체 회사 고유번호를 다운로드합니다...")
    url = f'https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={key}'
    response = None
    try:
        response = requests.get(url)
        response.raise_for_status()  # 요청이 성공적이지 않으면 예외 발생

        # ZIP 파일이므로 in-memory에서 처리
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # ZIP 파일 내의 CORPCODE.xml 파일 읽기 (대문자로 명시)
            with z.open('CORPCODE.xml') as corp_code_file:
                # 파일 내용을 읽어서 UTF-8로 디코딩
                xml_content = corp_code_file.read().decode('utf-8')

                # corpcode.xml 파일로 저장
                with open('corpcode.xml', 'w', encoding='utf-8') as f:
                    f.write(xml_content)
                
                print("corpcode.xml 파일이 성공적으로 저장되었습니다.")
                return True

    except requests.exceptions.RequestException as e:
        print(f"HTTP 요청 중 오류가 발생했습니다: {e}")
        return False
    except zipfile.BadZipFile:
        print("다운로드한 파일이 유효한 ZIP 파일이 아닙니다. API 키가 유효한지 확인해보세요.")
        if response:
            print("서버 응답 내용 (앞 500자):")
            print(response.content[:500].decode('utf-8', errors='ignore'))
        return False
    except Exception as e:
        print(f"ZIP 파일 처리 중 오류가 발생했습니다: {e}")
        if response:
            print("서버 응답 내용 (앞 500자):")
            print(response.content[:500].decode('utf-8', errors='ignore'))
        return False

def analyze_company(company_name, corp_code, start_year, end_year):
    """
    지정된 회사의 재무 데이터를 분석하고 예측하여 DataFrame을 반환하는 함수
    """
    print(f"\n{'='*50}")
    print(f"{start_year}년부터 {end_year}년까지 {company_name}의 재무 정보 분석을 시작합니다.")
    print(f"{'{'}={'='*50}\n")

    # --- 1. 회사 개황 정보 조회 (업종 코드 포함) ---
    induty_code = None
    try:
        print(f"--- {company_name}: 회사 개황 정보 조회 시작 ---")
        company_url = f'https://opendart.fss.or.kr/api/company.json?crtfc_key={api_key}&corp_code={corp_code}'
        response = requests.get(company_url)
        response.raise_for_status()
        company_data = response.json()
        if company_data['status'] == '000':
            induty_code = company_data.get('induty_code')
            print(f"  -> 업종 코드: {induty_code} (회사명: {company_data.get('corp_name')})")
        else:
            print(f"  -> 회사 개황 API 오류: {company_data['message']}")
    except Exception as e:
        print(f"  -> 회사 개황 정보 조회 중 오류 발생: {e}")
    print(f"--- {company_name}: 회사 개황 정보 조회 완료 ---\n")

    all_financial_data = []

    try:
        for year in range(start_year, end_year + 1):
            print(f"--- {company_name}: {year}년 데이터 조회 시작 ---")
            report_codes = {'1분기': '11013', '2분기': '11012', '3분기': '11014', '4분기': '11011'}

            for quarter, report_code in report_codes.items():
                print(f"  {year}년 {quarter} 보고서 데이터를 조회 중...")
                url = (
                       f'https://opendart.fss.or.kr/api/fnlttMultiAcnt.json?'
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

                extracted_info = {'기업명': company_name, '날짜': f'{year}', '분기': f'{quarter}', '업종코드': induty_code}
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

        print(f"\n--- {company_name}: 미래 재무 데이터 예측 시작 (성장률 기반) ---")
        prediction_cols = ['자산총계', '부채총계', '자본총계', '매출액', '영업이익', '당기순이익']
        future_predictions = {}
        future_steps = 8

        for col in prediction_cols:
            # 연간 성장률(YoY) 계산 (4분기 기준)
            yoy_growth = df[col].pct_change(periods=4).dropna()
            
            # 성장률의 이상치(inf)를 제외하고 평균 계산
            avg_growth_rate = yoy_growth[np.isfinite(yoy_growth)].mean()

            # 평균 성장률이 유효하지 않으면(데이터 부족 등) 0으로 처리
            if pd.isna(avg_growth_rate) or not np.isfinite(avg_growth_rate):
                avg_growth_rate = 0.0
            
            print(f"  - '{col}'의 평균 연간 성장률: {avg_growth_rate:.2%}")

            # 예측값을 저장할 리스트
            predicted_values = []
            # 예측의 기반이 될 데이터 (원본 데이터)
            temp_data = df[[col]].copy()

            for i in range(future_steps):
                # 1년 전 데이터(실적 또는 바로 이전에 예측된 값)를 가져옴
                base_idx = len(df) + i - 4
                base_value = temp_data.iloc[base_idx][col]
                
                # 성장률을 적용하여 예측
                new_value = base_value * (1 + avg_growth_rate)
                predicted_values.append(new_value)
                
                # 예측된 값을 temp_data에 추가하여 다음 예측에 사용
                new_row = pd.DataFrame({col: [new_value]})
                temp_data = pd.concat([temp_data, new_row], ignore_index=True)

            future_predictions[col] = predicted_values

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
    # DART 고유번호 파일 다운로드
    get_corp_codes(api_key)

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
            '기업명', '날짜', '분기', '구분', '업종코드', '자산총계', '부채총계', '자본총계', 
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
        print(f"{'{'}={'='*50}")
    else:
        print("\n\n분석할 데이터가 없거나 오류가 발생하여 파일을 생성하지 않았습니다.")
