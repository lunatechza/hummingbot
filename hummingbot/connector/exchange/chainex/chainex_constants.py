from hummingbot.core.api_throttler.data_types import RateLimit

DEFAULT_DOMAIN = "io"
TESTNET_DOMAIN = "testnet"
HBOT_ORDER_ID_PREFIX = "48"
MAX_ORDER_ID_LEN = 32
HEARTBEAT_TIME_INTERVAL = 30.0

DOMAIN_REST_URLS = {
    DEFAULT_DOMAIN: "https://api.chainex.io/",
    TESTNET_DOMAIN: "https://api.chainex.test/",
}
DOMAIN_PUBLIC_WSS_URLS = {
    DEFAULT_DOMAIN: "wss://push.chainex.io/ws/market",
    TESTNET_DOMAIN: "wss://push.chainex.test/ws/market",
}
DOMAIN_PRIVATE_WSS_URLS = {
    DEFAULT_DOMAIN: "wss://push.chainex.io/ws/account",
    TESTNET_DOMAIN: "wss://push.chainex.test/ws/account",
}

# Backwards compatible aliases
REST_URL = DOMAIN_REST_URLS[DEFAULT_DOMAIN]
PUBLIC_WSS_URL = DOMAIN_PUBLIC_WSS_URLS[DEFAULT_DOMAIN]
PRIVATE_WSS_URL = DOMAIN_PRIVATE_WSS_URLS[DEFAULT_DOMAIN]
PRIVATE_WSS_ENDPOINT = "/ws/account"

# Public API endpoints
TICKER_PRICE_CHANGE_PATH_URL = "stats/"  # e.g. stats/{COIN}/{EXCHANGE}
TICKER_BOOK_PATH_URL = "stats/"
EXCHANGE_INFO_PATH_URL = "market/summary/"  # e.g. market/summary/{EXCHANGE}
MARKET_SUMMARY_PATH_URL = "market/summary/"
SNAPSHOT_PATH_URL = "market/orders/"  # e.g. market/orders/{COIN}/{EXCHANGE}/ALL
SERVER_TIME_PATH_URL = "timestamp"
PING_PATH_URL = "timestamp"
TRADING_PATH_URL = "trading/order"

# Private API endpoints
BALANCE_PATH_URL = "wallet/balances"
DEPOSITS_PATH_URL = "wallet/deposits/"  # e.g. wallet/deposits/{COIN}
ORDER_PATH_URL = "trading/order/"  # e.g. trading/order/{ID}
MY_TRADES_PATH_URL = "trading/trades/"  # e.g. trading/trades/{COIN}

# WS Topics
ORDERBOOK_UPDATE = "ORDERBOOK_UPDATE"
ORDERBOOK_TRADE = "NEW_TRADE"
USER_TRADE = "NEW_USER_TRADE"
USER_ORDER_ADDED = "ORDER_ADDED"
USER_ORDER_CANCELLED = "ORDER_CANCELLED"

ORDER_STATE = {
    "open": "OPEN",
    "pending": "PENDING_CREATE",
    "filled": "FILLED",
    "cancelled": "CANCELED",
    "canceled": "CANCELED",
    "partially_filled": "PARTIALLY_FILLED",
    "partially filled": "PARTIALLY_FILLED",
    "rejected": "FAILED",
    "failed": "FAILED",
}

RATE_LIMITS = [
    RateLimit(limit_id=TRADING_PATH_URL, limit=120, time_interval=30),
    RateLimit(limit_id=BALANCE_PATH_URL, limit=120, time_interval=30),
    RateLimit(limit_id=DEPOSITS_PATH_URL, limit=120, time_interval=30),
    RateLimit(limit_id=ORDER_PATH_URL, limit=120, time_interval=30),
    RateLimit(limit_id=MY_TRADES_PATH_URL, limit=120, time_interval=30),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=600, time_interval=60),
    RateLimit(limit_id=MARKET_SUMMARY_PATH_URL, limit=600, time_interval=60),
    RateLimit(limit_id=TICKER_PRICE_CHANGE_PATH_URL, limit=600, time_interval=60),
    RateLimit(limit_id=SNAPSHOT_PATH_URL, limit=600, time_interval=60),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=600, time_interval=60),
    RateLimit(limit_id=PING_PATH_URL, limit=600, time_interval=60),
    RateLimit(limit_id=PUBLIC_WSS_URL, limit=120, time_interval=30),
    RateLimit(limit_id=PRIVATE_WSS_URL, limit=120, time_interval=30),
]
