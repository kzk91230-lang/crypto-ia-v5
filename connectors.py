import asyncio, time
import pandas as pd
from . import config

class ProviderError(Exception):
    def __init__(self, provider, status, message):
        super().__init__(f"[{provider}] {message}")
        self.provider, self.status, self.message = provider, status, message

HINTS = {400: "requête invalide (symbole inconnu ?)", 403: "accès refusé (WAF, IP bloquée ou clé requise)",
         418: "IP bannie temporairement (limite dépassée)", 429: "limite de requêtes atteinte",
         451: "indisponible dans cette région (restriction légale)"}
BINANCE_TF = {t: t for t in ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w", "1M"]}
KRAKEN_TF = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440, "1w": 10080}  # pas de 3m/2h/1M
STATUS = {
    "binance": {"name": "Binance Spot (API publique, sans clé)", "status": "INCONNU", "last_update": None, "last_error": None,
                "limits": "Limites de poids par IP ; HTTP 451 possible selon le pays ; 11 timeframes natifs."},
    "kraken": {"name": "Kraken (API publique, secours)", "status": "INCONNU", "last_update": None, "last_error": None,
               "limits": "720 bougies max ; pas de 3m, 2h ni 1M ; la dernière bougie est en cours."},
    "tradingview": {"name": "TradingView", "status": "NON CONNECTÉ", "last_update": None, "last_error": None,
                    "limits": "Pas d'API publique de données : utilisé uniquement pour l'affichage (phase 2)."},
    "activtrades": {"name": "ActivTrades", "status": "NON CONNECTÉ", "last_update": None, "last_error": None,
                    "limits": "Aucune API de données publique vérifiée : aucune connexion simulée."},
}
_now = lambda: pd.Timestamp.now(tz="UTC").isoformat()

def _frame(rows):
    df = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close", "volume", "closed"])
    df.index = pd.to_datetime(df.pop("t"), unit="ms", utc=True)
    cols = ["open", "high", "low", "close", "volume"]
    df[cols] = df[cols].astype(float)
    return df[~df.index.duplicated()].sort_index()

def parse_binance(rows, now_ms):  # [openT,o,h,l,c,v,closeT,...] ; clôturée si closeT < maintenant
    return _frame([(r[0], r[1], r[2], r[3], r[4], r[5], int(r[6]) < now_ms) for r in rows])

def parse_kraken(data, tf_min, now_ms):  # [time,o,h,l,c,vwap,volume,count]
    if data.get("error"): raise ProviderError("kraken", 200, "; ".join(data["error"]))
    rows = next(v for k, v in data["result"].items() if k != "last")
    return _frame([(int(r[0]) * 1000, r[1], r[2], r[3], r[4], r[6], int(r[0]) * 1000 + tf_min * 60000 <= now_ms) for r in rows])

class MarketData:
    def __init__(self, transport=None, ttl=None):
        import httpx
        self.client = httpx.AsyncClient(transport=transport, timeout=10)
        self.ttl, self.cache, self.sem = ttl or config.CACHE_TTL, {}, asyncio.Semaphore(4)

    async def _get(self, prov, url, params):
        import httpx
        last = None
        for attempt in range(3):
            try:
                async with self.sem: r = await self.client.get(url, params=params)
            except httpx.HTTPError as e:
                last = ProviderError(prov, None, f"erreur réseau : {e}")
            else:
                if r.status_code == 200: return r.json()
                last = ProviderError(prov, r.status_code, f"HTTP {r.status_code} : " + HINTS.get(r.status_code, "erreur serveur" if r.status_code >= 500 else "requête refusée"))
                if r.status_code in (400, 403, 404, 451): break          # inutile de réessayer
                if r.status_code in (418, 429):
                    try: await asyncio.sleep(min(float(r.headers.get("Retry-After", 2)), 10))
                    except ValueError: await asyncio.sleep(2)
                    continue
            await asyncio.sleep(0.5 * 2 ** attempt)
        raise last

    async def _binance(self, sym, tf, limit):
        raw = await self._get("binance", f"{config.BINANCE_BASE_URL}/api/v3/klines", {"symbol": f"{sym}USDT", "interval": BINANCE_TF[tf], "limit": limit})
        return parse_binance(raw, int(time.time() * 1000))

    async def _kraken(self, sym, tf, limit):
        if tf not in KRAKEN_TF: raise ProviderError("kraken", "unsupported", f"timeframe {tf} non supporté")
        pair = {"BTC": "XBT", "DOGE": "XDG"}.get(sym, sym) + "USD"
        raw = await self._get("kraken", f"{config.KRAKEN_BASE_URL}/0/public/OHLC", {"pair": pair, "interval": KRAKEN_TF[tf]})
        return parse_kraken(raw, KRAKEN_TF[tf], int(time.time() * 1000))

    async def fetch(self, symbol, tf, limit=500, use_cache=True):
        """Retourne (DataFrame OHLCV, meta source). Jamais de donnée fictive : erreur explicite sinon."""
        hit = self.cache.get((symbol, tf))
        if use_cache and hit and time.time() - hit[0] < self.ttl: return hit[1], hit[2]
        errors = []
        for prov in ("binance", "kraken"):
            try: df = await getattr(self, "_" + prov)(symbol, tf, limit)
            except ProviderError as e:
                if e.status != "unsupported": STATUS[prov].update(status="ERREUR", last_error=f"{_now()} {e.message}")
                errors.append(str(e)); continue
            STATUS[prov].update(status="OK", last_update=_now(), last_error=None)
            meta = {"provider": prov, "name": STATUS[prov]["name"], "fetched_at": _now(), "fallback": prov != "binance"}
            self.cache[(symbol, tf)] = (time.time(), df, meta)
            return df, meta
        raise ProviderError("all", None, " | ".join(errors))
