

import requests
import pandas as pd
import json

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
            '2분기(반기)': '11012',
            '3분기': '11014',
            '4분기(사업보고서)': '11011'
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
                '날짜': f"{year}-{quarter.split('(')[0]}",
                '분기': f'{year}년 {quarter}'
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

    # 컬럼 순서 정리
    if not df.empty:
        final_columns = ['기업명', '날짜', '분기', '자산총계', '부채총계', '자본총계', '매출액', '영업이익', '당기순이익']
        df = df.reindex(columns=final_columns)

    # Excel 파일로 저장
    output_filename = f'{company_name}_{start_year}-{end_year}_재무분석_requests.xlsx'
    df.to_excel(output_filename, index=False)

    print(f"\n분석 완료! 모든 결과가 '{output_filename}' 파일로 저장되었습니다.")

except Exception as e:
    print(f"스크립트 실행 중 오류가 발생했습니다: {e}")

