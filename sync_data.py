import os
import csv
import json
import requests
import psycopg2
from datetime import datetime
from io import StringIO

# --- 환경 변수 및 설정 ---
# GitHub Secrets에서 가져옴
DB_PASSWORD = os.environ.get("DB_PASSWORD")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 외부 PostgreSQL DB 접속 정보 (DBeaver 이미지 기반)
DB_HOST = "pg-3ae9p5.vpc-cdb-kr.ntruss.com"
DB_PORT = "5432"
DB_NAME = "qmarket"
DB_USER = "hansol"

# --- 배치 및 임시 파일 설정 ---
BATCH_SIZE = 1000 # 한 번에 보낼 행 수 (1000~2000 추천)
TEMP_DIR = "/tmp" 

# --- SQL 쿼리 정의 및 Supabase 테이블 이름 매핑 (수정 완료) ---
# key: Supabase 테이블명, value: SQL 쿼리 및 Upsert 충돌 방지 컬럼 설정
QUERIES = {
    # 1. 사용자 정보 (user_id를 기준으로 업데이트)
    "users": {
        "sql": """
            WITH latest_user_addresses AS (
                SELECT DISTINCT ON (user_id) user_id, mart_id
                FROM user_addresses
                ORDER BY user_id, user_address_id DESC
            ),
            marketing_pivot AS (
                SELECT
                    user_id,
                    BOOL_OR(CASE WHEN marketing_type = 'APP_PUSH' THEN agreement ELSE NULL END) AS agree_app_push,
                    BOOL_OR(CASE WHEN marketing_type = 'MESSAGE' THEN agreement ELSE NULL END) AS agree_sms,
                    BOOL_OR(CASE WHEN marketing_type = 'ALIM_TALK' THEN agreement ELSE NULL END) AS agree_alim_talk
                FROM marketing_agreements
                GROUP BY user_id
            )
            SELECT
                u.user_id, u.nickname, u.phone, u.created_date AS signup_date, g.name AS grade_name,
                m.mart_id, m.name AS mart_name, m.enabled AS mart_is_enabled,
                mp.agree_app_push, mp.agree_sms, mp.agree_alim_talk
            FROM users u
            LEFT JOIN grades g ON u.grade = g.code
            LEFT JOIN latest_user_addresses lua ON u.user_id = lua.user_id
            LEFT JOIN marts m ON lua.mart_id = m.mart_id
            LEFT JOIN marketing_pivot mp ON u.user_id = mp.user_id
            WHERE u.user_status = 'ACTIVE'
            AND (m.enabled = true AND m.closed != '연중휴무');
        """,
        "on_conflict": "user_id" # Supabase 테이블의 Primary Key와 일치해야 함
    },
    
    # 2. 쿠폰 정보 (user_id를 기준으로 업데이트)
    "coupons": {
        "sql": """
            SELECT
                u.user_id,
                COUNT(uc.user_coupon_id) AS valid_coupon_count,
                ARRAY_AGG(uc.coupon_id) AS valid_coupon_ids
            FROM users u
            JOIN user_coupons uc ON u.user_id = uc.user_id
            WHERE u.user_status = 'ACTIVE'
              AND uc.is_used = false
              AND uc.end_date > CURRENT_DATE
            GROUP BY u.user_id
            ORDER BY valid_coupon_count DESC;
        """,
        "on_conflict": "user_id"
    },
    
    # 3. 주문/결제 정보 (order_id를 기준으로 업데이트)
    "orders": { # 테이블 이름: orders로 확정
        "sql": """
            SELECT
                o.user_id, o.order_id, o.created_date AS order_date,
                SUM(CASE WHEN p.type NOT IN ('QMONEY', 'COUPON', 'DELIVERY_COUPON') THEN p.payment ELSE 0 END) AS real_payment,
                SUM(CASE WHEN p.type = 'QMONEY' THEN p.payment ELSE 0 END) AS qmoney_payment,
                SUM(CASE WHEN p.type IN ('COUPON', 'DELIVERY_COUPON') THEN p.payment ELSE 0 END) AS coupon_payment
            FROM orders o
            JOIN payments p ON o.order_id = p.order_id
            WHERE o.state = 'PURCHASE_COMPLETED'
              AND p.state = 'COMPLETE' 
              AND o.created_date >= (CURRENT_DATE - INTERVAL '1 year')
            GROUP BY o.user_id, o.order_id, o.created_date
            ORDER BY o.user_id, o.created_date;
        """,
        "on_conflict": "order_id" # Supabase 테이블의 Primary Key와 일치해야 함
    }
}

def extract_db_to_csv(query_config, temp_filename):
    """외부 DB에서 쿼리를 실행하고 CSV 파일로 저장"""
    if not DB_PASSWORD:
        raise ValueError("DB_PASSWORD 환경 변수가 설정되지 않았습니다.")

    print(f"\n--- [1단계] {temp_filename} 추출 시작 ---")
    try:
        # DB 연결
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, 
            password=DB_PASSWORD, port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute(query_config['sql'])
        
        filepath = os.path.join(TEMP_DIR, temp_filename)
        
        # CSV 파일 작성
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # 컬럼명(Header) 작성
            if cursor.description:
                headers = [desc[0] for desc in cursor.description]
                writer.writerow(headers)
            # 데이터 작성
            writer.writerows(cursor)
            
        cursor.close()
        conn.close()
        print(f"✅ 데이터 추출 완료: {filepath}")
        return filepath
    except Exception as e:
        print(f"❌ DB 추출 실패: {e}")
        return None

def _send_batch(data, table_name, on_conflict_col):
    """Supabase API로 배치 데이터를 Upsert 전송"""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal" # 응답 최소화
    }
    
    # on_conflict 파라미터를 추가하여 UPSERT 구현
    params = {}
    if on_conflict_col:
        params['on_conflict'] = on_conflict_col
    
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    
    # POST 메소드를 사용하고 on_conflict 파라미터를 사용하면 Upsert가 됩니다.
    response = requests.post(url, headers=headers, params=params, data=json.dumps(data))
    
    if 200 <= response.status_code < 300: # 201 Created, 200 OK 등 성공 코드
        return True
    else:
        print(f"⚠️ {table_name} Upsert 실패 (Code {response.status_code}): {response.text}")
        return False

def upload_csv_to_supabase(table_name, filepath, on_conflict_col):
    """CSV 파일을 읽어 Supabase REST API를 통해 배치 Upsert"""
    print(f"\n--- [2단계] {table_name} 업로드 시작 (배치 크기: {BATCH_SIZE}) ---")
    
    if not os.path.exists(filepath):
        print(f"❌ 파일 경로 오류: {filepath}를 찾을 수 없습니다.")
        return

    try:
        with open(filepath, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data_batch = []
            total_count = 0
            
            for row in reader:
                # PostgreSQL 배열 타입은 Supabase 스키마에 따라 문자열로 전송됩니다.
                data_batch.append(row)
                
                if len(data_batch) >= BATCH_SIZE:
                    if _send_batch(data_batch, table_name, on_conflict_col):
                        total_count += len(data_batch)
                        print(f"✅ {len(data_batch)}개 배치 성공. 누적: {total_count}")
                        data_batch = [] # 초기화
                    else:
                        print(f"❌ {table_name} 업로드 중단됨.")
                        return

            # 남은 데이터 처리
            if data_batch:
                if _send_batch(data_batch, table_name, on_conflict_col):
                    total_count += len(data_batch)
                    print(f"✅ 최종 남은 {len(data_batch)}개 배치 성공. 총 {total_count}개 처리.")
                else:
                    print(f"❌ {table_name} 최종 업로드 중단됨.")
                    return

    except Exception as e:
        print(f"❌ {table_name} 업로드 중 알 수 없는 오류: {e}")
    finally:
        # 임시 파일 삭제
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"✅ 임시 파일 삭제: {filepath}")

def run_all_syncs():
    """정의된 모든 쿼리를 순차적으로 실행"""
    for table_name, config in QUERIES.items():
        print(f"\n==================== {table_name.upper()} 동기화 시작 ====================")
        temp_filename = f"{table_name}_export.csv"
        
        # 1. 추출
        filepath = extract_db_to_csv(config, temp_filename)
        
        # 2. 업로드
        if filepath:
            on_conflict_col = config.get("on_conflict")
            if not on_conflict_col:
                print(f"⚠️ {table_name} 테이블의 'on_conflict' 키가 정의되지 않아 **Insert만** 시도합니다.")
            
            upload_csv_to_supabase(table_name, filepath, on_conflict_col)

if __name__ == "__main__":
    if not (SUPABASE_URL and SUPABASE_KEY and DB_PASSWORD):
        print("❌ 환경 변수 (SUPABASE_URL, SUPABASE_KEY, DB_PASSWORD)를 설정해야 합니다.")
    else:
        run_all_syncs()
