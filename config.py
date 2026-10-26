import os
BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
KRAKEN_BASE_URL = os.getenv("KRAKEN_BASE_URL", "https://api.kraken.com")
WATCHLIST = [s.strip().upper() for s in os.getenv("WATCHLIST", "SOL,BTC").split(",") if s.strip()]
ANALYSIS_INTERVAL = int(os.getenv("ANALYSIS_INTERVAL", "60"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "15"))
DB_PATH = os.getenv("DB_PATH", "./data/signals.db")
APP_PASSWORD = os.getenv("APP_PASSWORD", "")
REQUIRE_PASSWORD = os.getenv("REQUIRE_PASSWORD", "") == "1"
EPHEMERAL_STORAGE = os.getenv("EPHEMERAL_STORAGE", "") == "1"
