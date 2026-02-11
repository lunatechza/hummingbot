import hashlib
import hmac
import json
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class ChainEXAuth(AuthBase):

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    def generate_signature_from_payload(self, payload: str) -> str:
        secret = self.secret_key.encode("utf-8")
        return hmac.new(secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def _raw_payload(self, url: str, params: Dict[str, Any], timestamp: int) -> str:
        query = urlencode(params) if params else ""
        payload = f"{url}?"
        if query:
            payload = f"{payload}{query}&"
        payload = f"{payload}key={self.api_key}&time={timestamp}"
        return payload

    @staticmethod
    def _to_dict(data: Optional[Any]) -> Dict[str, Any]:
        if data is None:
            return {}
        if isinstance(data, dict):
            return dict(data)
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        if isinstance(data, str):
            return json.loads(data)
        raise TypeError(f"Unsupported data type for ChainEX auth signing: {type(data)}")

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        timestamp = int(self.time_provider.time() * 1e3)

        if request.method == RESTMethod.POST:
            body_dict = self._to_dict(request.data)
            payload = self._raw_payload(request.url, body_dict, timestamp)
            signature = self.generate_signature_from_payload(payload)
            request.data = self.add_auth_to_params(signature, timestamp, body_dict)
            request.params = self.add_auth_to_params(signature, timestamp, request.params)
        else:
            params = request.params or {}
            payload = self._raw_payload(request.url, params, timestamp)
            signature = self.generate_signature_from_payload(payload)
            request.params = self.add_auth_to_params(signature, timestamp, params)

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    def add_auth_to_params(self, hash_value: str, timestamp: int, params: Optional[Dict[str, Any]]):
        request_params = params or {}
        request_params["key"] = self.api_key
        request_params["time"] = timestamp
        request_params["hash"] = hash_value
        return request_params

    def add_auth_to_url(self, url: str, timestamp: int):
        return f"{url}?key={self.api_key}&time={timestamp}"

    def get_auth_headers(self, url: str, timestamp: int):
        payload = f"{url}?key={self.api_key}&time={timestamp}"
        hash_value = self.generate_signature_from_payload(payload)
        return {"Authorization": hash_value}
