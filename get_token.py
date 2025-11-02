# save as: get_token.py
import requests
import webbrowser

# 1. 클라이언트 등록
register_url = "https://server.smithery.ai/@sunggyeong/cohobby-datalookup/auth/register?profile=boiling-lemming-jyM7E8"
register_data = {
    "client_name": "Test Client",
    "redirect_uris": ["http://localhost:8080"]
}

reg = requests.post(register_url, json=register_data)
client_id = reg.json()['client_id']
client_secret = reg.json()['client_secret']

# 2. 인증 URL 열기
auth_url = f"https://server.smithery.ai/@sunggyeong/cohobby-datalookup/auth/authorize?profile=boiling-lemming-jyM7E8&response_type=code&client_id={client_id}&redirect_uri=http://localhost:8080"

print(f"Client ID: {client_id}")
print(f"Client Secret: {client_secret}")
print(f"\n브라우저에서 열기:\n{auth_url}")
webbrowser.open(auth_url)

# 인증 후 URL의 code 부분을 입력받아 토큰 교환...
