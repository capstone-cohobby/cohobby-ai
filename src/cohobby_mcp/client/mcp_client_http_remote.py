"""
MCP HTTP Client (Smithery 배포용 완성 버전)

- JSON-RPC 2.0 기반 MCP 서버 호출용 클라이언트
- Smithery 배포 서버는 Bearer 토큰(OAuth2 Access Token) 인증만 허용함
- tools/list, tools/call 등 MCP 규격 엔드포인트 호출 가능
"""

import os
import json
import requests
from typing import Any, Dict, Optional

PROTOCOL_VERSION = "2025-06-18"


class MCPHttpClient:
    """
    FastMCP 서버(배포 버전)용 HTTP JSON-RPC 클라이언트
    """

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        timeout: int = 60
    ):
        # 예: https://server.smithery.ai/@sunggyeong/cohobby-datalookup/mcp?profile=boiling-lemming-jyM7E8
        self.base_url = base_url.rstrip("/")
        self.api_key = os.getenv("SMITHERY_API_KEY") if api_key is None else api_key
        self.timeout = timeout

    # -------------------------------------------------------------------------
    # 내부 유틸
    # -------------------------------------------------------------------------
    def _headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "MCP-Protocol-Version": PROTOCOL_VERSION,
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        JSON-RPC 2.0 요청 전송
        """
        r = requests.post(
            self.base_url,
            headers=self._headers(),
            data=json.dumps(payload),
            timeout=self.timeout,
        )
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"❌ HTTPError {r.status_code}: {r.text}")
            raise e

        try:
            return r.json()
        except Exception:
            print("⚠️ JSON 파싱 실패:", r.text[:200])
            return {"error": "invalid_json", "raw": r.text}

    # -------------------------------------------------------------------------
    # MCP 표준 메서드
    # -------------------------------------------------------------------------
    def initialize(self) -> Dict[str, Any]:
        payload = {
            "jsonrpc": "2.0",
            "id": "init",
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "python", "version": "1.0"},
            },
        }
        return self._post(payload)

    def tools_list(self) -> Dict[str, Any]:
        payload = {"jsonrpc": "2.0", "id": "list", "method": "tools/list"}
        return self._post(payload)

    def tools_call(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "jsonrpc": "2.0",
            "id": "call",
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
        return self._post(payload)


# -----------------------------------------------------------------------------
# 실행 테스트 (직접 호출 시)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    url = os.getenv(
        "MCP_URL",
        "https://server.smithery.ai/@sunggyeong/cohobby-datalookup/mcp?profile=boiling-lemming-jyM7E8"
    )
    token = os.getenv("SMITHERY_API_KEY")

    client = MCPHttpClient(url, api_key=token)

    print("🔹 1. initialize")
    print(json.dumps(client.initialize(), ensure_ascii=False, indent=2))

    print("\n🔹 2. tools/list")
    print(json.dumps(client.tools_list(), ensure_ascii=False, indent=2))

    print("\n🔹 3. tools/call: summarize_rental_prices")
    dummy_records = {"records": [
        {"rental_price": 10000},
        {"rental_price": 12000},
        {"rental_price": 8000}
    ]}
    print(json.dumps(client.tools_call("summarize_rental_prices", dummy_records),
                     ensure_ascii=False, indent=2))
# 실행 테스트 (직접 호출 시)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. URL 값을 여기에 직접 붙여넣으세요 (profile과 /mcp가 모두 포함된 전체 주소)
    url = "https://server.smithery.ai/@sunggyeong/cohobby-datalookup/mcp?profile=boiling-lemming-jyM7E8"

    # 2. API 키 값을 여기에 직접 붙여넣으세요 ('Personal' 메뉴에서 발급받은 s_... 키)
    token = "1de4fbcc-9405-4c80-a166-4db43c350a61"  # ◀◀◀ 따옴표 안에 실제 키로 교체해주세요.

    # --- 디버깅을 위한 추가 출력 ---
    print("--- [DEBUG INFO] ---")
    print(f"URL Used: {url}")
    if token and len(token) > 10:
        print(f"Token Used: {token[:5]}...{token[-5:]}") # 보안을 위해 일부만 출력
    else:
        print(f"Token Used: {token}")
    print("--------------------")
    
    # 3. 이 값들로 클라이언트를 생성합니다.
    client = MCPHttpClient(url, api_key=token)

    try:
        print("🔹 1. initialize")
        print(json.dumps(client.initialize(), ensure_ascii=False, indent=2))

        print("\n🔹 2. tools/list")
        print(json.dumps(client.tools_list(), ensure_ascii=False, indent=2))

        # (3번 tools/call은 테스트를 위해 잠시 주석 처리)
        # print("\n🔹 3. tools/call: summarize_rental_prices")
        # ...

    except Exception as e:
        print("\n--- ERROR DETECTED ---")
        print(f"An error occurred: {e}")
        print("----------------------")