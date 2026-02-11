import asyncio
from unittest import TestCase
from unittest.mock import MagicMock

from hummingbot.connector.exchange.chainex.chainex_auth import ChainEXAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest


class ChainEXAuthTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.api_key = "testApiKey"
        self.secret_key = "testSecretKey"

        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000

        self.auth = ChainEXAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self.mock_time_provider,
        )

    def async_run_with_timeout(self, coroutine, timeout: int = 1):
        return asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))

    def test_add_auth_params_to_get_request_without_params(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://test.url/api/endpoint",
            is_auth_required=True,
            throttler_limit_id="/api/endpoint",
        )

        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        self.assertEqual(self.api_key, request.params["key"])
        self.assertEqual(1000000, request.params["time"])
        self.assertIsNotNone(request.params["hash"])

    def test_add_auth_params_to_post_request(self):
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://test.url/api/endpoint",
            data={"param_a": "value_param_a"},
            is_auth_required=True,
            throttler_limit_id="/api/endpoint",
        )

        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        self.assertEqual(self.api_key, request.params["key"])
        self.assertEqual(self.api_key, request.data["key"])
        self.assertEqual(1000000, request.params["time"])
        self.assertEqual("value_param_a", request.data["param_a"])

    def test_no_auth_added_to_wsrequest(self):
        payload = {"param1": "value_param_1"}
        request = WSJSONRequest(payload=payload, is_auth_required=True)
        self.async_run_with_timeout(self.auth.ws_authenticate(request))
        self.assertEqual(payload, request.payload)
