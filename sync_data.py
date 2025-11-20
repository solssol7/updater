import os
import csv
import json
import requests
import psycopg2
from datetime import datetime

# --- 1. 외부 DB 연결 설정 (DBeaver 정보 반영) ---
DB_HOST = "pg-3ae9p5.vpc-cdb-kr.ntruss.com"
DB_PORT = "5432"
DB_NAME = "qmarket"
DB_USER = "hansol"
DB_PASSWORD = os.environ.get("DB_PASSWORD")  # GitHub Secrets에서 가져옴

# --- 2. Supabase 설정 ---
# URL은 공개되어도 큰 문제 없으나, Key는 절대 지켜야 합니다.
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")  # Service Role Key 필수
SUPABASE_TABLE = "orders"  # 업로드할 테이블 이름 (필요시 수정)

# --- 3. 추출할 쿼리 (수정 필요) ---
# 예: 어제 하루 동안 생성된 데이터만 가져오기
SQL_QUERY = "SELECT * FROM orders WHERE created_at >= NOW() - INTERVAL '1 day';"
CSV_FILE_PATH = "exported_data.csv"
BATCH_SIZE = 1000

def extract_db_to_csv():
    print("🔄 [1단계] 외부 DB에서 데이터 추출 시작...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, 
            password=DB_PASSWORD, port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY)
        
        # CSV 파일 작성
        with open(CSV_FILE_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # 컬럼명(Header) 작성
            if cursor.description:
                headers = [desc[0] for desc in cursor.description]
                writer.writerow(headers)
            # 데이터 작성
            writer.writerows(cursor)
            
        cursor.close()
        conn.close()
        print(f"✅ 데이터 추출 완료 ({CSV_FILE_PATH})")
        return True
    except Exception as e:
        print(f"❌ DB 추출 실패: {e}")
        return False

def upload_csv_to_supabase():
    print("🔄 [2단계] Supabase로 데이터 업로드 시작...")
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal" # 응답 최소화 (속도 향상)
    }

    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f) # 헤더를 키로 사용하여 딕셔너리 변환
            data_batch = []
            count = 0
            
            for row in reader:
                data_batch.append(row)
                if len(data_batch) >= BATCH_SIZE:
                    _send_batch(data_batch, headers)
                    count += len(data_batch)
                    data_batch = [] # 초기화
            
            # 남은 데이터 처리
            if data_batch:
                _send_batch(data_batch, headers)
                count += len(data_batch)
                
            print(f"✅ 총 {count}개 데이터 업로드 완료.")
            
    except FileNotFoundError:
        print("❌ CSV 파일을 찾을 수 없습니다.")
    except Exception as e:
        print(f"❌ 업로드 중 오류 발생: {e}")

def _send_batch(data, headers):
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code != 201:
        print(f"⚠️ 업로드 경고 (Code {response.status_code}): {response.text}")

if __name__ == "__main__":
    if extract_db_to_csv():
        upload_csv_to_supabase()
