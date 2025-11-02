# test_s3.py (수정본)

import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

print("--- S3 연결 및 권한 테스트 시작 ---")

# ❗️ 테스트하려는 버킷과 파일 이름을 정확하게 입력해주세요.
BUCKET_NAME = "cohobby-crawler"
# ❗️ 버킷 안에 실제로 존재하는 파일 이름 하나를 입력해주세요. (예: "daangn/out.jsonl")
KEY_NAME = "out.jsonl" 

try:
    s3_client = boto3.client("s3")
    
    # 1. 's3:ListBucket' 권한 테스트
    #    cohobby-crawler 버킷 안의 파일 목록을 가져오는지 확인합니다.
    print(f"\n[테스트 1] '{BUCKET_NAME}' 버킷의 파일 목록 조회 (s3:ListBucket 권한)")
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, MaxKeys=5)
    
    print("✅ 테스트 1 성공! 버킷 안의 파일 목록:")
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"  - {obj['Key']}")
    else:
        print("  (버킷이 비어있습니다)")

    # 2. 's3:GetObject' 권한 테스트
    #    실제 파일 하나를 읽어올 수 있는지 확인합니다.
    print(f"\n[테스트 2] '{KEY_NAME}' 파일 정보 조회 (s3:GetObject 권한)")
    s3_client.head_object(Bucket=BUCKET_NAME, Key=KEY_NAME)
    
    print(f"✅ 테스트 2 성공! '{KEY_NAME}' 파일에 접근 가능합니다.")

except ClientError as e:
    error_code = e.response['Error']['Code']
    print(f"\n❌ 에러 발생: {error_code}")
    print(f"   에러 메시지: {e.response['Error']['Message']}")
except Exception as e:
    print(f"\n❌ 예상치 못한 에러 발생: {e}")

print("\n--- 테스트 종료 ---")