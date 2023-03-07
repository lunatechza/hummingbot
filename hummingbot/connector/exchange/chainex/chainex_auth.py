import hashlib
import hmac
import json
import time
from json import dumps
from collections import OrderedDict
from typing import Any, Dict, Optional
from urllib.parse import urlencode, urlparse

import hummingbot.connector.exchange.chainex.chainex_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class ChainEXAuth(AuthBase):

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    @staticmethod
    def keysort(dictionary: Dict[str, str]) -> Dict[str, str]:
        return OrderedDict(sorted(dictionary.items(), key=lambda t: t[0]))

    def generate_signature_from_payload(self, payload: str) -> str:
        secret = bytes(self.secret_key.encode("utf-8"))
        signature = hmac.new(secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()
        return signature

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        path = request.url
        query = urlencode(request.params) if request.params is not None else ''
        if request.method == RESTMethod.POST:
            query = urlencode(json.loads(request.data)) if request.data is not None else ''
        path = path + '?'
        print(query)
        if not (query == ''):
            path = path + query + '&'
        print(path)
        timestamp = int(self.time_provider.time() * 1e3)
        payload = path + 'key=' + self.api_key + '&time=' + str(timestamp)
        print(payload)
        hash = self.generate_signature_from_payload(payload)
        print(hash)
        if request.method == RESTMethod.POST:
            request.data = dumps(self.add_auth_to_params(hash, timestamp, json.loads(request.data)))
        request.params = self.add_auth_to_params(hash, timestamp, request.params)
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request # pass through

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        encoded_params_str = urlencode(params)
        digest = hmac.new(self.secret_key.encode("utf8"), encoded_params_str.encode("utf8"), hashlib.sha256).hexdigest()
        return digest

    def generate_ws_authentication_message(self):
        timestamp = int(self.time_provider.time() * 1e3)
        url =  CONSTANTS.PRIVATE_WSS_URL + '?key={key}&time={time}'.format(key=self.api_key, time=timestamp)
        hash = hmac.new(bytes(self.secret_key.encode("utf-8")), url.encode("utf-8"), hashlib.sha256).hexdigest()
        auth_message = {
            "timestamp": timestamp,
            "hash": hash
        }
        return auth_message

    def add_auth_to_params(self,
                           hash: str,
                           timestamp: int,
                           params: Dict[str, Any]):

        request_params = params or {}
        request_params["key"] = self.api_key
        request_params["time"] = timestamp
        request_params["hash"] = hash

        return request_params

    def add_auth_to_url(self, url: str, timestamp: int):
        return url + '?key={key}&time={time}'.format(key=self.api_key, time=timestamp)

    def get_auth_headers(self, url: str, timestamp: int):
        payload = url + '?key=' + self.api_key + '&time=' + str(timestamp)
        hash = self.generate_signature_from_payload(payload)
        return {'Authorization': hash}