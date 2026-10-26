import numpy as np

def swings(df, k=3):
    """Pivots fractals. Un pivot en i n'est connu qu'à la bougie i+k (pas de look-ahead)."""
    h, l, out = df["high"].values, df["low"].values, []
    for i in range(k, len(df) - k):
        if h[i] > np.delete(h[i - k:i + k + 1], k).max():
            out.append({"i": i, "kind": "high", "price": float(h[i]), "confirmed": i + k})
        if l[i] < np.delete(l[i - k:i + k + 1], k).min():
            out.append({"i": i, "kind": "low", "price": float(l[i]), "confirmed": i + k})
    return out

def analyze_structure(df, k=3):
    """BOS / CHoCH sur clôtures. BOS = cassure dans le sens de la tendance, CHoCH = cassure contre elle."""
    by = {}
    for s in swings(df, k):
        by.setdefault(s["confirmed"], []).append(s)
    c, hi, lo, trend, ev = df["close"].values, None, None, 0, []
    for t in range(len(c)):
        for s in by.get(t, []):
            if s["kind"] == "high": hi = {**s, "broken": False}
            else: lo = {**s, "broken": False}
        if hi and not hi["broken"] and c[t] > hi["price"]:
            ev.append({"i": t, "type": "CHoCH" if trend == -1 else "BOS", "dir": "bull", "level": hi["price"]})
            hi["broken"], trend = True, 1
        elif lo and not lo["broken"] and c[t] < lo["price"]:
            ev.append({"i": t, "type": "CHoCH" if trend == 1 else "BOS", "dir": "bear", "level": lo["price"]})
            lo["broken"], trend = True, -1
    return {"trend": trend, "events": ev, "last_high": hi, "last_low": lo}
