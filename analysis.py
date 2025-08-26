import requests
import pandas as pd
import numpy as np
import zipfile
import io
import xml.etree.ElementTree as ET
import os

# ===================================================================
# 여기에 DART API 키를 입력해주세요.
api_key = '2725c0dbcde7679f9c840041541f9b6c9adb9d30'
# ===================================================================

# 분석할 기간 설정
start_year = 2020
end_year = 2024

# 계정 과목 이름 (DART API가 반환하는 이름)
ACCOUNTS_TO_EXTRACT = [
    '자산총계', '부채총계', '자본총계',
    '매출액', '영업이익', '당기순이익'
]

# 임시: corpcode.xml 파일이 이미 있다고 가정
# def get_corp_codes(key):
#     """
#     DART에서 제공하는 전체 회사 고유번호를 다운로드하여 corpcode.xml로 저장합니다.
#     """
#     print("DART 전체 회사 고유번호를 다운로드합니다...")
#     url = f'https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={key}'
#     response = None
#     try:
#         response = requests.get(url)
#         response.raise_for_status()  # 요청이 성공적이지 않으면 예외 발생

#         # ZIP 파일이므로 in-memory에서 처리
#         with zipfile.ZipFile(io.BytesIO(response.content)) as z:
#             # ZIP 파일 내의 CORPCODE.xml 파일 읽기 (대문자로 명시)
#             with z.open('CORPCODE.xml') as corp_code_file:
#                 # 파일 내용을 읽어서 UTF-8로 디코딩
#                 xml_content = corp_code_file.read().decode('utf-8')

#                 # corpcode.xml 파일로 저장
#                 with open('corpcode.xml', 'w', encoding='utf-8') as f:
#                     f.write(xml_content)
                
#                 print("corpcode.xml 파일이 성공적으로 저장되었습니다.")
#                 return True

#     except requests.exceptions.RequestException as e:
#         print(f"HTTP 요청 중 오류가 발생했습니다: {e}")
#         return False
#     except zipfile.BadZipFile:
#         print("다운로드한 파일이 유효한 ZIP 파일이 아닙니다. API 키가 유효한지 확인해보세요.")
#         if response:
#             print("서버 응답 내용 (앞 500자):")
#             print(response.content[:500].decode('utf-8', errors='ignore'))
#         return False
#     except Exception as e:
#         print(f"ZIP 파일 처리 중 오류가 발생했습니다: {e}")
#         if response:
#             print("서버 응답 내용 (앞 500자):")
#             print(response.content[:500].decode('utf-8', errors='ignore'))
#         return False

def find_companies_by_industry(api_key, industry_code):
    """
    companies_{industry_code}.xml 파일이 있으면 이를 읽어오고,
    없으면 DART API를 호출하여 회사 목록을 찾습니다.
    """
    filename = f'companies_{industry_code}.xml'

    # 1. 파일이 존재하는지 확인
    if os.path.exists(filename):
        print(f"'{filename}' 파일이 이미 존재합니다. 파일을 읽어와서 기업 목록을 불러옵니다.")
        companies = []
        try:
            tree = ET.parse(filename)
            root = tree.getroot()
            for company in root.findall('company'):
                corp_name = company.find('corp_name').text
                corp_code = company.find('corp_code').text
                companies.append((corp_name, corp_code))
            return companies
        except Exception as e:
            print(f"'{filename}' 파일 파싱 중 오류 발생: {e}")
            return []

    # 2. 파일이 없으면 기존 로직 실행 (DART API 호출)
    print(f"'{filename}' 파일이 없어 DART API로 기업을 검색합니다.")
    try:
        tree = ET.parse('corpcode.xml')
        root = tree.getroot()
        companies = []

        for company in root.findall('list'):
            corp_name = company.find('corp_name').text
            corp_code = company.find('corp_code').text

            # 회사 개황 API 호출 (induty_code 확인)
            url = f'https://opendart.fss.or.kr/api/company.json?crtfc_key={api_key}&corp_code={corp_code}'
            try:
                response = requests.get(url)
                if response.status_code != 200:
                    continue
                data = response.json()
                if data.get("status") == "000":
                    induty_code_from_api = data.get("induty_code")
                    if induty_code_from_api == industry_code:
                        companies.append((corp_name, corp_code))
            except Exception as e:
                print(f"회사 개황 조회 오류 ({corp_name}): {e}")
                continue

        return companies
    except FileNotFoundError:
        print("'corpcode.xml' 파일을 찾을 수 없습니다. 먼저 DART 고유번호 파일을 다운로드해야 합니다.")
        return []
    except Exception as e:
        print(f"XML 파싱 중 오류 발생: {e}")
        return []

def save_companies_to_xml(companies, industry_code):
    """
    특정 업종에 속하는 기업 목록을 XML 파일로 저장하는 함수.
    
    Args:
        companies (list): (기업명, 기업코드) 튜플의 리스트.
        industry_code (str): 업종 코드.
    """
    root = ET.Element('companies')
    root.set('industry_code', industry_code)
    
    for name, code in companies:
        company_elem = ET.SubElement(root, 'company')
        corp_name_elem = ET.SubElement(company_elem, 'corp_name')
        corp_name_elem.text = name
        corp_code_elem = ET.SubElement(company_elem, 'corp_code')
        corp_code_elem.text = code

    tree = ET.ElementTree(root)
    filename = f'companies_{industry_code}.xml'
    try:
        tree.write(filename, encoding='utf-8', xml_declaration=True)
        print(f"\n'{industry_code}' 업종의 기업 목록이 '{filename}' 파일로 저장되었습니다.")
    except Exception as e:
        print(f"XML 파일 저장 중 오류 발생: {e}")

def analyze_company(company_name, corp_code, start_year, end_year):
    """
    지정된 회사의 재무 데이터를 분석하고 DataFrame을 반환하는 함수 (예측 기능 제거)
    """
    print(f"\n{'='*50}")
    print(f"{start_year}년부터 {end_year}년까지 {company_name}의 재무 정보 분석을 시작합니다.")
    print(f"{'='*50}\n")

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
            print(f"   -> 업종 코드: {induty_code} (회사명: {company_data.get('corp_name')})")
        else:
            print(f"   -> 회사 개황 API 오류: {company_data['message']}")
    except Exception as e:
        print(f"   -> 회사 개황 정보 조회 중 오류 발생: {e}")
    print(f"--- {company_name}: 회사 개황 정보 조회 완료 ---")

    all_financial_data = []

    try:
        for year in range(start_year, end_year + 1):
            print(f"--- {company_name}: {year}년 데이터 조회 시작 ---")
            report_codes = {'1분기': '11013', '2분기': '11012', '3분기': '11014', '4분기': '11011'}

            for quarter, report_code in report_codes.items():
                print(f"   {year}년 {quarter} 보고서 데이터를 조회 중...")
                url = (
                        f'https://opendart.fss.or.kr/api/fnlttMultiAcnt.json?'
                        f'crtfc_key={api_key}&corp_code={corp_code}'
                        f'&bsns_year={year}&reprt_code={report_code}')
                
                response = requests.get(url)
                if response.status_code != 200:
                    print(f"   -> 서버 오류 발생 (코드: {response.status_code})")
                    continue

                data = response.json()
                if data['status'] != '000':
                    print(f"   -> API 오류: {data['message']}")
                    continue

                extracted_info = {'기업명': company_name, '날짜': f'{year}', '분기': f'{quarter}', '업종코드': induty_code}
                for item in data['list']:
                    if item['fs_div'] == 'CFS' and item['account_nm'] in ACCOUNTS_TO_EXTRACT:
                        extracted_info[item['account_nm']] = item['thstrm_amount']
                all_financial_data.append(extracted_info)
                print(f"   -> {year}년 {quarter} 데이터 추출 완료.")
            print(f"--- {company_name}: {year}년 데이터 조회 완료 ---")

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

        df['구분'] = '실적'
        
        # --- 미래 예측 부분 대신 빈 DataFrame 생성 ---
        last_year = int(df['날짜'].iloc[-1])
        last_quarter = int(df['분기'].iloc[-1].replace('분기', ''))
        
        future_quarters, future_dates = [], []
        current_year, current_quarter = last_year, last_quarter
        for _ in range(4): # 4개 분기
            current_quarter += 1
            if current_quarter > 4:
                current_quarter = 1
                current_year += 1
            future_quarters.append(f"{current_quarter}분기")
            future_dates.append(f"{current_year}")

        # 모든 예측값을 NaN으로 채운 빈 DataFrame 생성
        forecast_df = pd.DataFrame({
            '기업명': [company_name] * 4,
            '날짜': future_dates,
            '분기': future_quarters,
            '구분': ['예측'] * 4,
            '업종코드': [induty_code] * 4
        })
        
        # 예측 관련 모든 숫자 컬럼을 NaN으로 채움
        columns_to_fill = list(set(df.columns) - {'기업명', '날짜', '분기', '구분', '업종코드'})
        for col in columns_to_fill:
             forecast_df[col] = np.nan
        
        combined_df = pd.concat([df, forecast_df], ignore_index=True)
        print(f"--- {company_name}: 미래 예측 데이터 공간 생성 완료 ---")
        return combined_df

    except Exception as e:
        print(f"\n*** {company_name} 처리 중 오류가 발생했습니다: {e} ***")
        return pd.DataFrame()

# ===================================================================
# 메인 실행 로직
# ===================================================================
if __name__ == "__main__":
    # 임시: corpcode.xml 파일이 이미 있다고 가정
    # if not get_corp_codes(api_key):
    #     exit()

    target_induty_code = input("분석할 업종 코드를 입력하세요: ")

    companies_to_analyze = find_companies_by_industry(api_key, target_induty_code)

    if not companies_to_analyze:
        print(f"입력하신 업종 코드 '{target_induty_code}'에 해당하는 회사를 찾을 수 없습니다.")
    else:
        print(f"\n'{target_induty_code}' 업종의 {len(companies_to_analyze)}개 회사를 분석합니다.")
        
        # XML 파일로 저장하는 부분 추가
        save_companies_to_xml(companies_to_analyze, target_induty_code)

        all_results = []
        for name, code in companies_to_analyze:
            result_df = analyze_company(name, code, start_year, end_year)
            if not result_df.empty:
                all_results.append(result_df)

        if all_results:
            final_df = pd.concat(all_results, ignore_index=True)
            final_df.loc[final_df['구분'] == '실적', '매출액_성장률'] = final_df[final_df['구분'] == '실적'].groupby('기업명')['매출액'].pct_change(periods=4) * 100 
            final_df.loc[final_df['구분'] == '실적', '영업이익_성장률'] = final_df[final_df['구분'] == '실적'].groupby('기업명')['영업이익'].pct_change(periods=4) * 100 
            final_df.loc[final_df['구분'] == '실적', '당기순이익_성장률'] = final_df[final_df['구분'] == '실적'].groupby('기업명')['당기순이익'].pct_change(periods=4) * 100 

            final_columns = [
                '기업명', '날짜', '분기', '구분', '업종코드', '자산총계', '부채총계', '자본총계', 
                '매출액', '영업이익', '영업비용', '당기순이익', '수익성 상태', 
                '매출액_성장률', '영업이익_성장률', '당기순이익_성장률', '영업이익률', '순이익률', '영업비용률', 
                'ROA', 'ROE'
            ]
            final_df = final_df.reindex(columns=final_columns)

            output_filename = f'업종분석_{target_induty_code}_{start_year}-{end_year}.xlsx'
            final_df.to_excel(output_filename, index=False, float_format='%.4f')

            print(f"\n\n{'='*50}")
            print(f"모든 분석 및 예측 완료! 결과가 '{output_filename}' 파일로 저장되었습니다.")
            print(f"{'='*50}")
        else:
            print("\n\n분석할 데이터가 없거나 오류가 발생하여 파일을 생성하지 않았습니다.")