"""Sessions de marché (Sydney, Tokyo, Londres, New York) : horaires, état ouvert/fermé, variation du prix depuis l'ouverture.
Horaires conventionnels des sessions « forex / crypto », en heure LOCALE de chaque place : l'heure d'été est gérée automatiquement.
Session fermée (nuit, week-end) = aucun pourcentage. Pour changer un horaire ou compter le week-end, modifier SESSIONS / OPEN_DAYS."""
from datetime import datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo
from .connectors import ProviderError

OPEN_DAYS = (0, 1, 2, 3, 4)       # lundi → vendredi (heure locale de la place). Ajouter 5 et 6 pour compter aussi samedi et dimanche.
SESSIONS = [                      # (clé, nom, région, fuseau, ouverture locale, fermeture locale)
    ("sydney", "Sydney", "Océanie", "Australia/Sydney", "07:00", "16:00"),
    ("tokyo", "Tokyo", "Asie", "Asia/Tokyo", "09:00", "18:00"),
    ("london", "Londres", "Europe", "Europe/London", "08:00", "17:00"),
    ("newyork", "New York", "États-Unis", "America/New_York", "08:00", "17:00"),
]
MAX_GAP = timedelta(minutes=10)   # écart toléré entre l'ouverture et la première bougie disponible

def _hm(s): h, m = s.split(":"); return dtime(int(h), int(m))

def windows(now=None):
    """Pour chaque session : ouverte (fenêtre en cours) ou fermée (prochaine fenêtre). Heures en UTC, ISO 8601."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    out = []
    for key, name, region, tz, o, c in SESSIONS:
        z = ZoneInfo(tz)
        today = now.astimezone(z).date()
        for d in range(9):                                            # 9 jours couvrent n'importe quel week-end ou jour férié de la config
            day = today + timedelta(days=d)
            if day.weekday() not in OPEN_DAYS: continue
            op, cl = (datetime.combine(day, _hm(x), tzinfo=z).astimezone(timezone.utc) for x in (o, c))
            if now < cl:
                out.append({"key": key, "name": name, "region": region, "tz": tz, "open_local": o, "close_local": c,
                            "status": "open" if op <= now else "closed", "window_open": op.isoformat(), "window_close": cl.isoformat(), "pct": None})
                break
    return out

async def snapshot(fetch, symbol, now=None):
    """Variation du prix de `symbol` depuis l'ouverture de chaque session OUVERTE (bougies 5 min). Fermée : pct reste None."""
    ws = windows(now)
    res = {"symbol": symbol, "sessions": ws, "last": None, "source": None, "error": None}
    if not any(w["status"] == "open" for w in ws): return res
    try: df, meta = await fetch(symbol, "5m", 1000)
    except ProviderError as e:
        res["error"] = str(e); return res
    res["source"], res["last"] = meta, float(df["close"].iloc[-1])
    for w in ws:
        if w["status"] != "open": continue
        op = datetime.fromisoformat(w["window_open"])
        part = df[df.index >= op]
        if part.empty or part.index[0] - op > MAX_GAP:
            w["note"] = "prix à l'ouverture indisponible (historique trop court)"; continue
        w["open_price"] = float(part["open"].iloc[0])
        w["pct"] = round((res["last"] / w["open_price"] - 1) * 100, 2)
    return res
