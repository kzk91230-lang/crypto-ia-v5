"""Calendrier économique et sentiment de marché.
Données réelles uniquement : si une source échoue, l'erreur est affichée (jamais de valeur inventée)."""
import asyncio, json, logging, math, os, time
from datetime import datetime, timedelta, timezone
from .config import DB_PATH
from .connectors import STATUS, HINTS, _now

FMP_KEY = os.getenv("FMP_API_KEY", "").strip()          # clé gratuite Financial Modeling Prep (250 requêtes/jour) : source fiable du calendrier
FMP_URL = "https://financialmodelingprep.com/stable/economic-calendar"
logging.getLogger("httpx").setLevel(logging.WARNING)    # httpx écrit les URL en INFO : on évite que la clé API apparaisse dans les journaux
FF_HOSTS = ("https://nfs.faireconomy.media", "https://cdn-nfs.faireconomy.media")     # 2e hôte = secours (même fournisseur)
FF_FILES = ("ff_calendar_thisweek.json", "ff_calendar_nextweek.json")
CAL_CACHE = os.getenv("CALENDAR_CACHE", os.path.join(os.path.dirname(DB_PATH) or ".", "calendar_cache.json"))
PERIODS = ("5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d")          # périodes acceptées par Binance Futures
FNG_URL = "https://api.alternative.me/fng/"
FAPI = os.getenv("BINANCE_FUTURES_URL", "https://fapi.binance.com")
UA = {"User-Agent": "Mozilla/5.0 (compatible; crypto-trading-ai)"}

STATUS.update({
    "calendar": {"name": "Calendrier économique (flux Faireconomy / ForexFactory, non officiel)", "status": "INCONNU", "last_update": None, "last_error": None,
                 "limits": "Flux public hebdomadaire, cache 1 h ; la semaine suivante est publiée en cours de semaine. Dates Fed/BLS ajoutées à la main."},
    "fng": {"name": "Alternative.me — Fear & Greed Index crypto", "status": "INCONNU", "last_update": None, "last_error": None,
            "limits": "Mis à jour une fois par jour ; indice du marché crypto global (pas propre à une crypto)."},
    "bfutures": {"name": "Binance Futures (funding, long/short)", "status": "INCONNU", "last_update": None, "last_error": None,
                 "limits": "Contrats perpétuels USDT uniquement ; HTTP 451 possible selon le pays."},
})

if FMP_KEY: STATUS["calendar"].update(name="Calendrier économique (Financial Modeling Prep, clé gratuite ; secours : flux ForexFactory)",
                                      limits="250 requêtes/jour sur le plan gratuit, cache 1 h. Dates Fed/BLS ajoutées à la main.")

def _utc(y, m, d, h, mi=0): return datetime(y, m, d, h, mi, tzinfo=timezone.utc).isoformat()

NFP_KW = ["NON-FARM", "NON FARM", "NONFARM", "EMPLOYMENT CHANGE", "PAYROLL"]
CPI_KW = ["CPI", "INFLATION RATE"]
# Dates officielles vérifiées le 25/10/2026 (Réserve fédérale, BLS). Heures en UTC.
# (date, titre, mots-clés servant à retirer le doublon quand le flux en direct contient déjà l'événement)
KEY_DATES = [
    (_utc(2026, 10, 28, 18), "FOMC : décision de taux de la Fed (conférence de presse 30 min après)", ["FOMC", "FEDERAL FUNDS", "FED INTEREST RATE"]),
    (_utc(2026, 11, 6, 13, 30), "Emploi US (NFP, rapport d'octobre)", NFP_KW),
    (_utc(2026, 11, 10, 13, 30), "Inflation US (CPI d'octobre)", CPI_KW),
    (_utc(2026, 11, 18, 19), "FOMC : compte rendu de la réunion d'octobre", ["MINUTES"]),
    (_utc(2026, 12, 4, 13, 30), "Emploi US (NFP, rapport de novembre)", NFP_KW),
    (_utc(2026, 12, 9, 19), "FOMC : décision de taux + projections économiques", ["FOMC", "FEDERAL FUNDS", "FED INTEREST RATE"]),
    (_utc(2026, 12, 10, 13, 30), "Inflation US (CPI de novembre)", CPI_KW),
]

def _dt(s): return datetime.fromisoformat(s)

def parse_ff(rows):
    """Garde : impact FORT (tous pays) + impact MOYEN pour l'USD. Ignore jours fériés et impact faible."""
    out = []
    for r in rows if isinstance(rows, list) else []:
        try:
            impact, country = r.get("impact"), r.get("country")
            if not (impact == "High" or (country == "USD" and impact == "Medium")): continue
            dt = datetime.fromisoformat(r["date"])
            dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
        except (KeyError, ValueError, TypeError, AttributeError): continue
        out.append({"date": dt.isoformat(), "title": r.get("title") or "—", "country": country, "impact": impact,
                    "forecast": r.get("forecast") or None, "previous": r.get("previous") or None, "source": "direct"})
    return out

def parse_fmp(rows):
    """Format Financial Modeling Prep : impact FORT (toutes devises) + MOYEN pour l'USD. Dates supposées en UTC (heure du serveur FMP)."""
    out = []
    for r in rows if isinstance(rows, list) else []:
        try:
            impact, cur = r.get("impact"), r.get("currency") or ""
            if not (impact == "High" or (cur == "USD" and impact == "Medium")): continue
            dt = datetime.fromisoformat(str(r["date"]).replace(" ", "T").replace("Z", "+00:00"))
            dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
        except (KeyError, ValueError, TypeError, AttributeError): continue
        num = lambda x: None if x in (None, "") else f"{x:g}" if isinstance(x, (int, float)) and not isinstance(x, bool) else str(x)
        out.append({"date": dt.isoformat(), "title": r.get("event") or "—", "country": cur or r.get("country") or "—", "impact": impact,
                    "forecast": num(r.get("estimate")), "previous": num(r.get("previous")), "source": "direct"})
    return out

def merge_events(live, now):
    """Ajoute les dates officielles, sauf si le flux en direct contient déjà l'événement (il a prévision/précédent)."""
    out = list(live)
    for date, title, kws in KEY_DATES:
        d = _dt(date)
        if any(e["country"] == "USD" and abs(_dt(e["date"]) - d) <= timedelta(hours=36) and any(k in e["title"].upper() for k in kws) for e in live): continue
        out.append({"date": date, "title": title, "country": "USD", "impact": "High", "forecast": None, "previous": None, "source": "officiel"})
    out = [e for e in out if _dt(e["date"]) >= now - timedelta(hours=1)]
    return sorted(out, key=lambda e: e["date"])

FNG_CLASSES = {"Extreme Fear": ("Peur extrême", "dn"), "Fear": ("Peur", "dn"), "Neutral": ("Neutre", "nu"),
               "Greed": ("Avidité", "up"), "Extreme Greed": ("Avidité extrême (euphorie)", "up")}

def read_fng(v, cls=None):
    """Classe donnée par la source si elle existe, sinon les seuils d'Alternative.me (24 / 46 / 54 / 75)."""
    if cls in FNG_CLASSES: return FNG_CLASSES[cls]
    if v <= 24: return FNG_CLASSES["Extreme Fear"]
    if v <= 46: return FNG_CLASSES["Fear"]
    if v <= 54: return FNG_CLASSES["Neutral"]
    if v <= 75: return FNG_CLASSES["Greed"]
    return FNG_CLASSES["Extreme Greed"]

def read_funding(pct):  # pct = taux de funding en % par période de 8 h ; ≈ +0,01 % = niveau de base chez Binance
    if pct >= 0.05: return "Très élevé : beaucoup de longs, risque de liquidations", "up"
    if pct >= 0.02: return "Élevé : biais acheteur", "up"
    if pct > -0.01: return "Neutre (niveau de base)", "nu"
    return "Négatif : biais vendeur (les shorts paient les longs)", "dn"

COINS = ("BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "AVAX", "LINK", "DOT", "LTC", "TRX")

def _err(e): return str(e) if isinstance(e, RuntimeError) else f"réponse inattendue ({type(e).__name__})"

def funding_item(sym, pct):
    reading, tone = read_funding(pct)
    return {"label": f"Funding {sym} (perpétuel)", "display": f"{pct:+.4f} %", "reading": reading, "tone": tone,
            "detail": "taux payé toutes les 8 h entre acheteurs et vendeurs ; seuils indicatifs"}

def read_oi(chg):  # variation de l'open interest (en %) : ne dit pas si ce sont des acheteurs ou des vendeurs
    if chg >= 3: return "Forte hausse : beaucoup de nouvelles positions ouvertes (plus de levier)", "nu"
    if chg <= -3: return "Forte baisse : positions fermées (désendettement)", "nu"
    return "Stable", "nu"

def read_ls(ratio):
    if ratio >= 1.5: return "Très majoritairement acheteurs", "up"
    if ratio >= 1.15: return "Majorité acheteuse", "up"
    if ratio <= 0.67: return "Très majoritairement vendeurs", "dn"
    if ratio <= 0.87: return "Majorité vendeuse", "dn"
    return "Équilibré", "nu"

class _SrcErr(RuntimeError):
    """Source indisponible : garde le code HTTP et le délai avant le prochain essai pour pouvoir écrire un message clair."""
    def __init__(self, base, code, wait):
        super().__init__(f"{base} · réessai automatique dans {_retry_txt(wait)}"); self.base, self.code, self.wait = base, code, wait

def _why(code):
    return ("le fournisseur gratuit limite le nombre de requêtes" if code == 429 else
           "le fournisseur refuse l'accès depuis ce serveur" if code in (403, 451) else
           "le fournisseur a un problème temporaire" if code and code >= 500 else
           "accès refusé : clé API invalide ou expirée ?" if code == 401 else
           "ce service n'est pas inclus dans le plan gratuit" if code == 402 else
           "connexion impossible avec le fournisseur" if code is None else "le fournisseur ne répond pas comme prévu")

def _live_msg(e):
    code, why = getattr(e, "code", None), _why(getattr(e, "code", None))
    wait = getattr(e, "wait", None)
    retry = f" Nouvel essai automatique dans {_retry_txt(wait)}." if wait else ""
    tip = " Pour des chiffres fiables : clé gratuite Financial Modeling Prep (variable FMP_API_KEY sur Render)." if code == 429 and not FMP_KEY else ""
    return f"Prévisions et chiffres en direct indisponibles pour le moment : {why}. Les dates officielles Fed/BLS restent affichées.{retry}{tip} (détail : {getattr(e, 'base', str(e))})"

class _Http(RuntimeError):
    def __init__(self, msg, code, retry_after=None): super().__init__(msg); self.code, self.retry_after = code, retry_after

def _backoff(code, retry_after):
    """Durée (s) pendant laquelle on ne redemande PAS une source qui vient d'échouer."""
    if code == 404: return 6 * 3600                                           # page pas encore publiée : inutile de réessayer souvent
    if code == 429:                                                           # limite de requêtes : on respecte Retry-After, sinon 30 min
        return min(max(retry_after, 300), 3600) if retry_after else 1800
    if code in (403, 418, 451): return 1800
    return 300

def _retry_txt(sec):
    m = max(1, math.ceil(sec / 60))
    return f"{m} min" if m < 120 else f"{round(m / 60)} h"

class Macro:
    def __init__(self, client=None, cache_file=CAL_CACHE):
        if client is None:
            import httpx
            client = httpx.AsyncClient(timeout=10, headers=UA)
        self.client, self.cache, self.neg, self.file = client, {}, {}, cache_file   # neg : échecs récents, pour ne pas marteler une source

    def _load(self, name):
        """Dernière copie sur disque (utile après un redémarrage ou quand la source limite les requêtes). Ignorée si > 8 jours."""
        try:
            e = json.load(open(self.file, encoding="utf-8")).get(name)
            if e and isinstance(e.get("rows"), list) and time.time() - e["saved_at"] < 8 * 86400: return (e["saved_at"], e["rows"])
        except Exception: pass
        return None

    def _save(self, name, rows):
        try:
            try: allc = json.load(open(self.file, encoding="utf-8"))
            except Exception: allc = {}
            allc[name] = {"saved_at": time.time(), "rows": rows}
            os.makedirs(os.path.dirname(self.file) or ".", exist_ok=True)
            tmp = self.file + ".tmp"
            json.dump(allc, open(tmp, "w", encoding="utf-8")); os.replace(tmp, self.file)
        except Exception: pass                                                # le cache disque est un bonus : jamais bloquant

    async def _fetch(self, url, params):
        r = await self.client.get(url, params=params)
        if r.status_code != 200:
            ra = None
            try: ra = int(getattr(r, "headers", {}).get("retry-after", ""))
            except (ValueError, TypeError, AttributeError): pass
            hint = HINTS.get(r.status_code, "introuvable (pas encore publié ?)" if r.status_code == 404 else "erreur serveur" if r.status_code >= 500 else "requête refusée")
            raise _Http(f"HTTP {r.status_code} : {hint}", r.status_code, ra)
        return r.json()

    async def _json(self, prov, key, url, ttl, params=None, persist=None):
        """Retourne (données, périmé). `url` peut être une liste (hôtes de secours). Sert le cache si la source échoue (et le signale)."""
        hit = self.cache.get(key)
        if hit is None and persist:
            hit = self._load(persist)
            if hit: self.cache[key] = hit
        if hit and time.time() - hit[0] < ttl: return hit[1], False
        bad = self.neg.get(key)
        if bad and time.time() - bad[0] < bad[2]:                              # échec récent : pas de nouvelle requête avant la fin du délai
            if hit: return hit[1], True
            raise _SrcErr(bad[1], bad[3], bad[0] + bad[2] - time.time())
        msg = wait = code = None
        for u in ([url] if isinstance(url, str) else url):
            try: data = await self._fetch(u, params); break
            except Exception as e:
                if msg is None:
                    msg = str(e) if isinstance(e, RuntimeError) else f"erreur réseau : {type(e).__name__} {e}".strip()
                    code = getattr(e, "code", None); wait = _backoff(code, getattr(e, "retry_after", None))
        else:
            self.neg[key] = (time.time(), msg, wait, code)
            if prov: STATUS[prov].update(status="ERREUR", last_error=f"{_now()} {msg}")
            if hit: return hit[1], True
            raise _SrcErr(msg, code, wait)
        self.neg.pop(key, None)
        if prov: STATUS[prov].update(status="OK", last_update=_now(), last_error=None)
        self.cache[key] = (time.time(), data)
        if persist: self._save(persist, data)
        return data, False

    async def calendar(self):
        now, live, errors, notes, stale, got, fmp_ok = datetime.now(timezone.utc), [], [], [], False, False, False
        if FMP_KEY:
            try:
                d0 = now.date()
                rows, st = await self._json("calendar", ("fmp",), FMP_URL, 3600, {"from": d0.isoformat(), "to": (d0 + timedelta(days=14)).isoformat(), "apikey": FMP_KEY}, persist="fmp")
                if not isinstance(rows, list): raise RuntimeError(str(rows.get("Error Message", rows) if isinstance(rows, dict) else rows)[:120])
                live, stale, fmp_ok = parse_fmp(rows), st, True
            except RuntimeError as e: notes.append(f"Source Financial Modeling Prep indisponible : {_why(getattr(e, 'code', None))} (détail : {getattr(e, 'base', e)}). Essai du flux ForexFactory.")
        for i, name in enumerate(() if fmp_ok else ("thisweek", "nextweek")):
            if i == 1 and not got: continue                                    # semaine suivante : seulement si la semaine en cours a répondu (moins de requêtes)
            urls = tuple(f"{h}/{FF_FILES[i]}" for h in FF_HOSTS)
            try: rows, st = await self._json("calendar" if i == 0 else None, ("ff", i), urls, 3600 if i == 0 else 6 * 3600, persist=name)
            except RuntimeError as e:
                if i == 0: errors.append(_live_msg(e))
                elif getattr(e, "code", None) == 404: notes.append(f"Semaine suivante : pas encore publiée par le fournisseur (nouvel essai automatique dans {_retry_txt(getattr(e, 'wait', 0) or 21600)}).")
                else: notes.append("Semaine suivante : indisponible pour le moment.")
                continue
            got |= i == 0; stale |= st; live += parse_ff(rows)
        return {"generated_at": now.isoformat(), "events": merge_events(live, now), "errors": errors, "notes": notes, "stale": stale}

    async def _fng(self):
        data, _ = await self._json("fng", "fng", FNG_URL, 600, {"limit": 31, "format": "json"})
        d = data["data"]; v = int(d[0]["value"])
        def pt(i):                                                   # i = nombre de jours en arrière (0 = aujourd'hui)
            if i >= len(d): return None
            x = int(d[i]["value"]); return {"value": x, "label": read_fng(x, d[i].get("value_classification"))[0]}
        reading, tone = read_fng(v, d[0].get("value_classification"))
        try: updated = int(d[0]["timestamp"])
        except (KeyError, ValueError, TypeError): updated = None
        return {"key": "fng", "label": "Fear & Greed (marché crypto)", "display": f"{v} / 100", "reading": reading, "tone": tone,
                "detail": f"hier : {d[1]['value'] if len(d) > 1 else '—'} · 0 = peur, 100 = euphorie",
                "value": v, "updated": updated, "history": {"now": pt(0), "yesterday": pt(1), "week": pt(7), "month": pt(30)}}

    async def _funding(self, sym):
        data, _ = await self._json("bfutures", ("fund", sym), f"{FAPI}/fapi/v1/premiumIndex", 60, {"symbol": f"{sym}USDT"})
        return funding_item(sym, float(data["lastFundingRate"]) * 100)

    async def _funding_all(self):
        """Une seule requête pour tous les contrats perpétuels : {SYMBOLUSDT: funding en %}."""
        data, _ = await self._json("bfutures", "fund_all", f"{FAPI}/fapi/v1/premiumIndex", 60)
        return {r["symbol"]: float(r["lastFundingRate"]) * 100 for r in data if "lastFundingRate" in r}

    async def _ls(self, sym, period="1h"):
        data, _ = await self._json("bfutures", ("ls", sym, period), f"{FAPI}/futures/data/globalLongShortAccountRatio", 120,
                                   {"symbol": f"{sym}USDT", "period": period, "limit": 2})
        row = data[-1]; ratio, longs = float(row["longShortRatio"]), float(row["longAccount"]) * 100
        reading, tone = read_ls(ratio)
        prev = f" · période précédente : {float(data[-2]['longShortRatio']):.2f}" if len(data) > 1 else ""
        return {"label": f"Long / Short {sym}", "display": f"{longs:.0f} % longs", "reading": reading, "tone": tone,
                "detail": f"ratio {ratio:.2f}{prev} · comptes Binance Futures, période {period}"}

    async def _oi(self, sym, period="1h"):
        data, _ = await self._json("bfutures", ("oi", sym, period), f"{FAPI}/futures/data/openInterestHist", 120,
                                   {"symbol": f"{sym}USDT", "period": period, "limit": 24})
        first, last = float(data[0]["sumOpenInterestValue"]), float(data[-1]["sumOpenInterestValue"])
        chg = (last / first - 1) * 100
        reading, tone = read_oi(chg)
        return {"label": f"Open interest {sym}", "display": f"{chg:+.1f} %", "reading": reading, "tone": tone,
                "detail": f"variation sur les {len(data)} dernières périodes de {period} ; ne dit pas si ce sont des achats ou des ventes"}

    async def sentiment(self, sym, period="1h"):
        names = ("Fear & Greed", f"Funding {sym}", f"Long / Short {sym}", f"Open interest {sym}")
        res = await asyncio.gather(self._fng(), self._funding(sym), self._ls(sym, period), self._oi(sym, period), return_exceptions=True)
        items, errors = [], {}
        for name, r in zip(names, res):
            if isinstance(r, Exception): errors[name] = _err(r)
            else: items.append(r)
        return {"symbol": sym, "period": period, "generated_at": _now(), "items": items, "errors": errors}

    async def sentiment_all(self, coins=COINS, period="1h"):
        """Liste de toutes les cryptos : funding (1 requête pour toutes) + long/short (1 requête par crypto, 6 en parallèle)."""
        sem = asyncio.Semaphore(6)
        async def ls(c):
            async with sem: return await self._ls(c, period)
        fng, funds, *lss = await asyncio.gather(self._fng(), self._funding_all(), *[ls(c) for c in coins], return_exceptions=True)
        errors = {}
        if isinstance(fng, Exception): errors["Fear & Greed"], fng = _err(fng), None
        if isinstance(funds, Exception): errors["Funding"], funds = _err(funds), {}
        rows = []
        for c, l in zip(coins, lss):
            e, pct = {}, funds.get(f"{c}USDT")
            if pct is None: e["funding"] = errors.get("Funding") or "pas de contrat perpétuel USDT pour cette crypto"
            if isinstance(l, Exception): e["ls"], l = _err(l), None
            rows.append({"symbol": c, "funding": funding_item(c, pct) if pct is not None else None, "ls": l, "errors": e})
        return {"period": period, "generated_at": _now(), "fng": fng, "rows": rows, "errors": errors}
