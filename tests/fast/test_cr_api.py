import unittest
from unittest.mock import AsyncMock, patch

try:
    import httpx
except Exception:
    raise unittest.SkipTest("httpx not available")

import cr_api
from cr_api import ClashRoyaleAPI, ClashRoyaleAPIError


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text="", headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text
        self.headers = headers or {}

    def json(self):
        return self._payload


class _FakeClient:
    def __init__(self, side_effect):
        self.get = AsyncMock(side_effect=side_effect)


class ClashRoyaleAPITests(unittest.IsolatedAsyncioTestCase):
    def test_encode_tag(self) -> None:
        api = ClashRoyaleAPI()
        self.assertEqual(api._encode_tag("#ABC123"), "%23ABC123")
        self.assertEqual(api._encode_tag("ABC123"), "%23ABC123")

    async def test_request_returns_json_on_200(self) -> None:
        api = ClashRoyaleAPI()
        fake = _FakeClient([_FakeResponse(200, {"ok": True})])
        with patch.object(api, "_get_client", new=AsyncMock(return_value=fake)):
            data = await api._request("/x")
        self.assertEqual({"ok": True}, data)

    async def test_request_404_raises(self) -> None:
        api = ClashRoyaleAPI()
        fake = _FakeClient([_FakeResponse(404)])
        with patch.object(api, "_get_client", new=AsyncMock(return_value=fake)):
            with self.assertRaises(ClashRoyaleAPIError) as ctx:
                await api._request("/x")
        self.assertEqual(404, ctx.exception.status_code)

    async def test_request_403_raises(self) -> None:
        api = ClashRoyaleAPI()
        fake = _FakeClient([_FakeResponse(403)])
        with patch.object(api, "_get_client", new=AsyncMock(return_value=fake)):
            with self.assertRaises(ClashRoyaleAPIError) as ctx:
                await api._request("/x")
        self.assertEqual(403, ctx.exception.status_code)

    async def test_request_503_retries_then_success(self) -> None:
        api = ClashRoyaleAPI()
        fake = _FakeClient(
            [
                _FakeResponse(503, text="busy"),
                _FakeResponse(200, {"ok": True}),
            ]
        )
        with patch.object(api, "_get_client", new=AsyncMock(return_value=fake)), patch(
            "cr_api.asyncio.sleep", new=AsyncMock()
        ) as sleep_mock:
            data = await api._request("/x")
        self.assertEqual({"ok": True}, data)
        self.assertEqual(1, sleep_mock.await_count)

    async def test_request_503_exhausted_raises(self) -> None:
        api = ClashRoyaleAPI()
        fake = _FakeClient(
            [
                _FakeResponse(503, text="busy"),
                _FakeResponse(503, text="busy"),
                _FakeResponse(503, text="busy"),
            ]
        )
        with patch.object(api, "_get_client", new=AsyncMock(return_value=fake)), patch(
            "cr_api.asyncio.sleep", new=AsyncMock()
        ) as sleep_mock:
            with self.assertRaises(ClashRoyaleAPIError) as ctx:
                await api._request("/x")
        self.assertEqual(503, ctx.exception.status_code)
        self.assertEqual(2, sleep_mock.await_count)

    async def test_request_429_uses_retry_after(self) -> None:
        api = ClashRoyaleAPI()
        fake = _FakeClient(
            [
                _FakeResponse(429, headers={"Retry-After": "2.5"}),
                _FakeResponse(200, {"ok": True}),
            ]
        )
        with patch.object(api, "_get_client", new=AsyncMock(return_value=fake)), patch(
            "cr_api.asyncio.sleep", new=AsyncMock()
        ) as sleep_mock:
            await api._request("/x")
        sleep_mock.assert_awaited_once_with(2.5)

    async def test_request_429_invalid_retry_after_falls_back(self) -> None:
        api = ClashRoyaleAPI()
        fake = _FakeClient(
            [
                _FakeResponse(429, headers={"Retry-After": "invalid"}),
                _FakeResponse(200, {"ok": True}),
            ]
        )
        with patch.object(api, "_get_client", new=AsyncMock(return_value=fake)), patch(
            "cr_api.asyncio.sleep", new=AsyncMock()
        ) as sleep_mock:
            await api._request("/x")
        sleep_mock.assert_awaited_once_with(0.5)

    async def test_request_429_exhausted_raises_rate_limit(self) -> None:
        api = ClashRoyaleAPI()
        fake = _FakeClient(
            [
                _FakeResponse(429),
                _FakeResponse(429),
                _FakeResponse(429),
            ]
        )
        with patch.object(api, "_get_client", new=AsyncMock(return_value=fake)), patch(
            "cr_api.asyncio.sleep", new=AsyncMock()
        ):
            with self.assertRaises(ClashRoyaleAPIError) as ctx:
                await api._request("/x")
        self.assertEqual(429, ctx.exception.status_code)
        self.assertIn("Rate limit", ctx.exception.message)

    async def test_request_network_error_retries_then_success(self) -> None:
        api = ClashRoyaleAPI()
        request = httpx.Request("GET", "https://example.com/x")
        fake = _FakeClient(
            [
                httpx.RequestError("net", request=request),
                _FakeResponse(200, {"ok": True}),
            ]
        )
        with patch.object(api, "_get_client", new=AsyncMock(return_value=fake)), patch(
            "cr_api.asyncio.sleep", new=AsyncMock()
        ) as sleep_mock:
            data = await api._request("/x")
        self.assertEqual({"ok": True}, data)
        self.assertEqual(1, sleep_mock.await_count)

    async def test_request_network_error_exhausted(self) -> None:
        api = ClashRoyaleAPI()
        request = httpx.Request("GET", "https://example.com/x")
        fake = _FakeClient(
            [
                httpx.RequestError("net", request=request),
                httpx.RequestError("net", request=request),
                httpx.RequestError("net", request=request),
            ]
        )
        with patch.object(api, "_get_client", new=AsyncMock(return_value=fake)), patch(
            "cr_api.asyncio.sleep", new=AsyncMock()
        ):
            with self.assertRaises(ClashRoyaleAPIError) as ctx:
                await api._request("/x")
        self.assertEqual(0, ctx.exception.status_code)

    async def test_request_with_404_retry_eventual_success(self) -> None:
        api = ClashRoyaleAPI()
        with patch.object(
            api,
            "_request",
            new=AsyncMock(
                side_effect=[
                    ClashRoyaleAPIError(404, "missing"),
                    {"ok": True},
                ]
            ),
        ), patch("cr_api.asyncio.sleep", new=AsyncMock()) as sleep_mock:
            data = await api._request_with_404_retry("/x")
        self.assertEqual({"ok": True}, data)
        self.assertEqual(1, sleep_mock.await_count)

    async def test_request_with_404_retry_exhausted(self) -> None:
        api = ClashRoyaleAPI()
        with patch.object(
            api,
            "_request",
            new=AsyncMock(side_effect=ClashRoyaleAPIError(404, "missing")),
        ), patch("cr_api.asyncio.sleep", new=AsyncMock()):
            with self.assertRaises(ClashRoyaleAPIError) as ctx:
                await api._request_with_404_retry("/x")
        self.assertEqual(404, ctx.exception.status_code)

    async def test_get_clan_members_extracts_items(self) -> None:
        api = ClashRoyaleAPI()
        with patch.object(
            api,
            "_request_with_404_retry",
            new=AsyncMock(return_value={"items": [{"tag": "#A"}]}),
        ) as req_mock:
            items = await api.get_clan_members("#CLAN")
        self.assertEqual([{"tag": "#A"}], items)
        req_mock.assert_awaited_once()

    async def test_get_river_race_log_extracts_items(self) -> None:
        api = ClashRoyaleAPI()
        with patch.object(
            api,
            "_request_with_404_retry",
            new=AsyncMock(return_value={"items": [{"seasonId": 1}]}),
        ):
            items = await api.get_river_race_log("#CLAN")
        self.assertEqual([{"seasonId": 1}], items)

    async def test_get_location_clan_rankings_builds_query(self) -> None:
        api = ClashRoyaleAPI()
        with patch.object(
            api,
            "_request_with_404_retry",
            new=AsyncMock(return_value={"items": []}),
        ) as req_mock:
            await api.get_location_clan_rankings(57000000, limit=50, after="abc")
        req_mock.assert_awaited_once()
        endpoint = req_mock.await_args.args[0]
        self.assertIn("/locations/57000000/rankings/clans?", endpoint)
        self.assertIn("limit=50", endpoint)
        self.assertIn("after=abc", endpoint)

    async def test_get_location_clanwar_rankings_builds_query(self) -> None:
        api = ClashRoyaleAPI()
        with patch.object(
            api,
            "_request_with_404_retry",
            new=AsyncMock(return_value={"items": []}),
        ) as req_mock:
            await api.get_location_clanwar_rankings(
                57000000,
                limit=50,
                before="zzz",
            )
        endpoint = req_mock.await_args.args[0]
        self.assertIn("/locations/57000000/rankings/clanwars?", endpoint)
        self.assertIn("limit=50", endpoint)
        self.assertIn("before=zzz", endpoint)

    async def test_global_client_singleton_and_close(self) -> None:
        cr_api._api_client = None
        first = await cr_api.get_api_client()
        second = await cr_api.get_api_client()
        self.assertIs(first, second)
        with patch.object(first, "close", new=AsyncMock()) as close_mock:
            await cr_api.close_api_client()
        close_mock.assert_awaited_once()
        self.assertIsNone(cr_api._api_client)
