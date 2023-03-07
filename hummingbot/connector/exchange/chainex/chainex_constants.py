from hummingbot.core.api_throttler.data_types import RateLimit

DEFAULT_DOMAIN = "io"
HBOT_ORDER_ID_PREFIX = "48"
MAX_ORDER_ID_LEN = 32
HEARTBEAT_TIME_INTERVAL = 30.0

# Base URL
REST_URL = "https://api.chainex.io/"
# REST_URL = "https://api.chainex.test/"
PUBLIC_WSS_URL = "wss://push.chainex.io/ws/market"
# PUBLIC_WSS_URL = "wss://push.chainex.test/ws/market"
PRIVATE_WSS_URL = "wss://push.chainex.io/ws/account"
# PRIVATE_WSS_URL = "wss://push.chainex.test/ws/account"
PRIVATE_WSS_ENDPOINT = "/ws/account"

# Public API endpoints
TICKER_PRICE_CHANGE_PATH_URL = "/stats/"
# TICKER_BOOK_PATH_URL = "/ticker/bookTicker"
EXCHANGE_INFO_PATH_URL = "market/summary/" # e.g. market/summary/{EXCHANGE}
MARKET_SUMMARY_PATH_URL = "market/summary/" # e.g. market/summary/{EXCHANGE}
SNAPSHOT_PATH_URL = "market/orders/" # e.g. market/orders/{COIN}/{EXCHANGE}/ALL
SERVER_TIME_PATH_URL = "timestamp"
PING_PATH_URL = "timestamp"
TRADING_PATH_URL = "trading/order"

# Private API endpoints
BALANCE_PATH_URL = "wallet/balances"
DEPOSITS_PATH_URL = "wallet/deposits/" # e.g. wallet/deposits/{COIN}
ORDER_PATH_URL = "trading/order/" # e.g. trading/order/{ID}
MY_TRADES_PATH_URL = "trading/trades/" # e.g. trading/trades/{COIN}

# WS Topics
ORDERBOOK_UPDATE = "ORDERBOOK_UPDATE"
ORDERBOOK_TRADE = "NEW_TRADE"

RATE_LIMITS = [
    # Place Orders
    RateLimit(limit_id=TRADING_PATH_URL, limit=120, time_interval=30),

    # Account queries
    RateLimit(limit_id=BALANCE_PATH_URL, limit=120, time_interval=30),
    RateLimit(limit_id=DEPOSITS_PATH_URL, limit=120, time_interval=30),
    RateLimit(limit_id=ORDER_PATH_URL, limit=120, time_interval=30),
    RateLimit(limit_id=MY_TRADES_PATH_URL, limit=120, time_interval=30),

    # Exchange Information
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=600, time_interval=60),
    RateLimit(limit_id=MARKET_SUMMARY_PATH_URL, limit=600, time_interval=60),
    RateLimit(limit_id=TICKER_PRICE_CHANGE_PATH_URL, limit=600, time_interval=60),
    RateLimit(limit_id=SNAPSHOT_PATH_URL, limit=600, time_interval=60),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=600, time_interval=60),
    RateLimit(limit_id=PING_PATH_URL, limit=600, time_interval=60),

    # WS Connections
    RateLimit(limit_id=PUBLIC_WSS_URL, limit=120, time_interval=30),
    RateLimit(limit_id=PRIVATE_WSS_URL, limit=120, time_interval=30),
]