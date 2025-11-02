# src/cohobby_mcp/client/mcp_client_http.py

import json
import requests
from typing import Any, Dict

PROTOCOL_VERSION = "2025-06-18"

class MCPHttpClient:
    """
    ✅ 로컬/도커 개발용 FastMCP 클라이언트 (인증 없음)
    - OAuth, Bearer, client_credentials 모두 제거
    - 단순 JSON-RPC POST만 수행
    """
    def __init__(self, base_url: str, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "MCP-Protocol-Version": PROTOCOL_VERSION,
        }

    def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            r = requests.post(
                self.base_url,
                headers=self._headers(),
                data=json.dumps(payload),
                timeout=self.timeout,
            )
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            print(f"❌ HTTPError {e.response.status_code}: {e.response.text}")
            raise
        except Exception as e:
            print(f"❌ 예외 발생: {e}")
            raise

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


# -------------------- 실행 예시 --------------------
if __name__ == "__main__":
    print("🚀 MCPHttpClient (로컬용) 테스트 시작")

    # FastMCP 서버가 http://127.0.0.1:8765/mcp 에서 실행 중이라고 가정
    client = MCPHttpClient("http://127.0.0.1:8765/mcp")

    try:
        print("\n🔹 tools/list")
        lst = client.tools_list()
        print(json.dumps(lst, ensure_ascii=False, indent=2))

        print("\n🔹 tools/call: summarize_rental_prices")
        res = client.tools_call("summarize_rental_prices", {
            "records": [{"rental_price": 15000}, {"rental_price": 20000}]
        })
        print(json.dumps(res, ensure_ascii=False, indent=2))

        print("\n🎉 테스트 완료")
    except Exception:
        print("\n--- ❌ 테스트 실패 ---")
