import asyncio
import pandas as pd
from .indicators import ema, rsi, macd, atr, adx, vwap
from .structure import analyze_structure

TFS = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w", "1M"]
TF_SECONDS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200,
              "4h": 14400, "1d": 86400, "1w": 604800, "1M": 2592000}
WEIGHTS = {"1m": .5, "3m": .5, "5m": 1, "15m": 1, "30m": 1.5, "1h": 2, "2h": 2, "4h": 3, "1d": 3, "1w": 2, "1M": 1}
MIN_BARS, TP_R = 200, (1.5, 2.5, 4.0)
NO_DATA = "DONNÉES INSUFFISANTES"
DEFAULT_STRATEGY = "v2"     # "v1" = ancienne stratégie (score élevé), conservée pour comparer dans le backtest
PB_LOOK, PB_ADX, PB_EXT, PB_MIN_SL, PB_MAX_SL, HTF_MULT = 8, 20, 2.0, 1.0, 3.0, 4
BASIS_V1 = "Entrée = dernière clôture (indicative). SL = dernier pivot ∓0,25 ATR (sinon 1,5 ATR). TP = multiples de R."
BASIS_V2 = ("Entrée = dernière clôture (indicative). Stratégie v2 : reprise de tendance après repli sur l'EMA20, tendance du timeframe supérieur dans le même sens. "
            "SL = extrême du repli ∓0,25 ATR (entre 1 et 3 ATR). TP = multiples de R.")

def _n(x): return None if pd.isna(x) else round(float(x), 6)

# Stratégie v3 : Turtle Traders (Richard Dennis & William Eckhardt, années 80). Règle publique, simplifiée.
DON_N, DON_SL_ATR = 20, 2.0
BASIS_V3 = ("Entrée = dernière clôture (indicative). Stratégie v3 (Turtle) : cassure du plus haut (LONG) ou du plus bas (SHORT) des 20 dernières bougies, "
            "au moment du franchissement. SL = 2 ATR. TP = multiples de R.")

def donchian_setup(df, av):
    """Stratégie v3 : déclencheur uniquement au franchissement du canal de 20 bougies (pas de re-entrée à chaque bougie)."""
    n = DON_N
    if len(df) < n + 3 or not av > 0: return None
    c, h, l = df["close"], df["high"], df["low"]
    hh_now, ll_now = float(h.iloc[-1 - n:-1].max()), float(l.iloc[-1 - n:-1].min())     # canal de la bougie précédant la dernière
    hh_prev, ll_prev = float(h.iloc[-2 - n:-2].max()), float(l.iloc[-2 - n:-2].min())    # canal de l'avant-dernière bougie
    px, px_prev = float(c.iloc[-1]), float(c.iloc[-2])
    if px > hh_now and px_prev <= hh_prev: s, lvl = 1, hh_now
    elif px < ll_now and px_prev >= ll_prev: s, lvl = -1, ll_now
    else: return None
    return {"s": s, "sl": px - s * DON_SL_ATR * av, "extreme": lvl, "basis": BASIS_V3}

# Stratégie v4 : retour à la moyenne (RSI(2) extrême dans le sens de la tendance de fond, inspiré de Larry Connors).
MR_N, MR_LO, MR_HI, MR_SL_ATR, MR_TP_R, MR_HOLD = 2, 10, 90, 2.5, (0.2, 0.4, 0.6), 20
BASIS_V4 = ("Entrée = dernière clôture (indicative). Stratégie v4 (retour à la moyenne) : RSI(2) passe sous 10 au-dessus de l'EMA200 (LONG) ou au-dessus de 90 "
            "sous l'EMA200 (SHORT). SL = 2,5 ATR ; objectifs proches : 0,5 / 1 / 1,5 ATR (= 0,2 / 0,4 / 0,6 R). Sortie forcée après 20 bougies.")

def meanrev_setup(df, emas, av):
    """Stratégie v4 : déclencheur au franchissement du seuil RSI(2) (une seule fois par excès, pas à chaque bougie)."""
    if len(df) < 220 or not av > 0: return None
    c = df["close"]; r = rsi(c, MR_N)
    now, prev, px, e200 = float(r.iloc[-1]), float(r.iloc[-2]), float(c.iloc[-1]), float(emas[200].iloc[-1])
    if px > e200 and now < MR_LO <= prev: s = 1
    elif px < e200 and now > MR_HI >= prev: s = -1
    else: return None
    ext = float(df["low"].iloc[-1]) if s == 1 else float(df["high"].iloc[-1])
    return {"s": s, "sl": px - s * MR_SL_ATR * av, "extreme": ext, "basis": BASIS_V4, "tp_r": MR_TP_R, "max_hold": MR_HOLD}

def _htf_closes(df, tf):
    """Clôtures du timeframe 4x plus long, bougies COMPLÈTES seulement (pas de repainting)."""
    sec = TF_SECONDS[tf]
    hc = df.resample(f"{sec * HTF_MULT}s", origin="epoch", label="left", closed="left")["close"].last()
    done = pd.Series(hc.index + pd.Timedelta(seconds=sec * HTF_MULT) <= df.index[-1] + pd.Timedelta(seconds=sec), index=hc.index)
    return hc[done & hc.notna()]

def htf_trend(df, tf):
    """+1 / -1 / 0 : tendance du timeframe supérieur (prix et EMA20 du bon côté de l'EMA50). None pour 1w/1M (non applicable)."""
    if tf in ("1w", "1M"): return None
    hc = _htf_closes(df, tf)
    if len(hc) < 40: return 0
    e20, e50, px = ema(hc, 20).iloc[-1], ema(hc, 50).iloc[-1], hc.iloc[-1]
    return 1 if px > e50 and e20 > e50 else -1 if px < e50 and e20 < e50 else 0

def pullback_setup(df, tf, emas, av, adx_v, pdi_v, mdi_v):
    """Stratégie v2 : reprise de tendance après un repli sur l'EMA20. Retourne None ou {s, sl, extreme}."""
    n = PB_LOOK
    if len(df) < 220 or not av > 0: return None
    c, o, h, l = (df[k] for k in ("close", "open", "high", "low"))
    e20s, e50s, e200s = emas[20], emas[50], emas[200]
    px, e20, e50, e200 = float(c.iloc[-1]), float(e20s.iloc[-1]), float(e50s.iloc[-1]), float(e200s.iloc[-1])
    for s in (1, -1):
        # 1) tendance de fond : EMA50 / prix du bon côté de l'EMA200, et EMA200 orientée dans le même sens
        if not (s * (e50 - e200) > 0 and s * (px - e200) > 0 and s * (e200 - float(e200s.iloc[-21])) > 0): continue
        # 2) force : ADX >= 20 et DI dans le sens
        if not (adx_v >= PB_ADX and s * (pdi_v - mdi_v) > 0): continue
        # 3) déclencheur : la bougie repart (clôture du bon côté de l'EMA20, bougie dans le sens, casse l'extrême précédent)
        ref = float(h.iloc[-2]) if s == 1 else float(l.iloc[-2])
        if not (s * (px - e20) > 0 and s * (px - float(o.iloc[-1])) > 0 and s * (px - ref) > 0): continue
        # 4) pas d'entrée tardive : prix pas étiré de plus de 2 ATR au-dessus/sous l'EMA20
        if s * (px - e20) > PB_EXT * av: continue
        # 5) repli : EMA20 touchée dans les 8 bougies précédentes, sans clôture au-delà de l'EMA50
        w = slice(-n - 1, -1)
        touched = bool((l.iloc[w] <= e20s.iloc[w]).any()) if s == 1 else bool((h.iloc[w] >= e20s.iloc[w]).any())
        if not (touched and bool((s * (c.iloc[w] - e50s.iloc[w]) > 0).all())): continue
        # 6) stop sous/sur l'extrême du repli, borné entre 1 et 3 ATR (trop serré = bruit + coûts ; trop large = risque mal payé)
        ext = float(l.iloc[-n - 1:].min()) if s == 1 else float(h.iloc[-n - 1:].max())
        sl = ext - s * 0.25 * av
        dist = s * (px - sl)
        if dist > PB_MAX_SL * av: continue
        if dist < PB_MIN_SL * av: sl = px - s * PB_MIN_SL * av
        # 7) le timeframe supérieur doit aller dans le même sens (test le plus coûteux : en dernier)
        ht = htf_trend(df, tf)
        if ht is not None and ht != s: continue
        return {"s": s, "sl": sl, "extreme": ext}
    return None

def analyze(df, tf, now=None, include_open=False, strategy=DEFAULT_STRATEGY, fast=False):
    """df : index UTC (ouverture de bougie), colonnes open/high/low/close/volume/closed.
    Par défaut seules les bougies clôturées sont utilisées (pas de repainting)."""
    provisional = bool(include_open and not df["closed"].iloc[-1])
    if not include_open: df = df[df["closed"]]
    if len(df) < MIN_BARS:
        return {"tf": tf, "state": "INDISPONIBLE", "signal": NO_DATA, "usable": False,
                "reason": f"{len(df)} bougies clôturées disponibles, {MIN_BARS} requises"}
    c, px, last = df["close"], float(df["close"].iloc[-1]), len(df) - 1
    emas = {n: ema(c, n) for n in (20, 50, 200)}
    e20, e50, e200 = (emas[n].iloc[-1] for n in (20, 50, 200))
    r, (_, _, mh), a = rsi(c), macd(c), atr(df)
    ax, pdi, mdi = adx(df)
    setup = None
    if strategy == "v3": setup = donchian_setup(df, float(a.iloc[-1]))
    elif strategy == "v4": setup = meanrev_setup(df, emas, float(a.iloc[-1]))
    elif strategy != "v1":
        setup = pullback_setup(df, tf, emas, float(a.iloc[-1]), float(ax.iloc[-1]), float(pdi.iloc[-1]), float(mdi.iloc[-1]))
    if strategy != "v1" and fast and not setup: return {"tf": tf, "state": "NEUTRE", "signal": "ATTENTE", "usable": True, "plan": None}   # backtest : seul le plan compte
    st = analyze_structure(df)
    relv = (df["volume"] / df["volume"].rolling(20).mean()).iloc[-1]
    comps = []
    def add(name, pts, why): comps.append({"name": name, "points": pts, "detail": why})

    if px > e20 > e50 > e200: add("EMA", 25, "prix > EMA20 > EMA50 > EMA200")
    elif px < e20 < e50 < e200: add("EMA", -25, "prix < EMA20 < EMA50 < EMA200")
    else: add("EMA", 10 if px > e50 else -10, f"alignement partiel (prix {'au-dessus' if px > e50 else 'sous'} EMA50)")
    d, strong = (1 if pdi.iloc[-1] > mdi.iloc[-1] else -1), ax.iloc[-1] >= 20
    add("ADX/DI", d * (15 if strong else 5), f"ADX {ax.iloc[-1]:.1f}, DI+ {pdi.iloc[-1]:.1f}, DI- {mdi.iloc[-1]:.1f}" + ("" if strong else " (tendance faible)"))
    rv = r.iloc[-1]
    add("RSI", 10 if rv > 55 else -10 if rv < 45 else 0, f"RSI {rv:.1f}")
    h, hp = mh.iloc[-1], mh.iloc[-2]
    add("MACD", (10 if h > hp else 5) if h > 0 else (-10 if h < hp else -5), f"histogramme {h:.4g}, {'en hausse' if h > hp else 'en baisse'}")
    t = st["trend"]
    add("Structure", 20 * t, {1: "dernier break haussier", -1: "dernier break baissier", 0: "aucune structure confirmée"}[t])
    ev = st["events"][-1] if st["events"] else None
    if ev and last - ev["i"] <= 10:
        add("Événement récent", 10 if ev["dir"] == "bull" else -10, f"{ev['type']} {ev['dir']} il y a {last - ev['i']} bougies")
    if relv >= 1.5:
        add("Volume", 5 if px > df["open"].iloc[-1] else -5, f"volume relatif {relv:.2f}")

    score = sum(x["points"] for x in comps)
    state = "HAUSSIER" if score >= 25 else "BAISSIER" if score <= -25 else "NEUTRE"
    signal = "ATTENTE"
    if strategy == "v1":
        if score >= 50 and t == 1: signal = "LONG"        # score élevé ET structure concordante
        elif score <= -50 and t == -1: signal = "SHORT"
    elif setup: signal = "LONG" if setup["s"] == 1 else "SHORT"

    close_ts = df.index[-1] + pd.Timedelta(seconds=TF_SECONDS[tf])
    fresh = now is None or (now - close_ts).total_seconds() <= 2 * TF_SECONDS[tf]
    if not fresh: signal = NO_DATA

    plan = None
    if signal in ("LONG", "SHORT"):
        s, av = (1 if signal == "LONG" else -1), float(a.iloc[-1])
        sl, inval, basis = px - s * 1.5 * av, None, BASIS_V1
        if setup: sl, inval, basis = setup["sl"], setup["extreme"], setup.get("basis", BASIS_V2)
        else:
            lvl = st["last_low"] if s == 1 else st["last_high"]
            if lvl:
                cand = lvl["price"] - s * 0.25 * av
                if 0 < s * (px - cand) <= 3 * av: sl, inval = cand, lvl["price"]
        risk = s * (px - sl)
        trs = setup.get("tp_r", TP_R) if setup else TP_R
        if risk >= 0.2 * av:
            plan = {"direction": signal, "entry": px, "invalidation": inval if inval else sl, "stop_loss": sl,
                    "tp": [px + s * risk * k for k in trs], "rr": list(trs), "score": score, "tf": tf, "max_hold": setup.get("max_hold") if setup else None,
                    "created_at": (now or pd.Timestamp.now(tz="UTC")).isoformat(),
                    "basis": basis, "strategy": strategy}
    return {"tf": tf, "state": state, "signal": signal, "score": score, "strength": abs(score), "usable": fresh,
            "provisional": provisional, "stale": not fresh, "price": px, "rsi": _n(rv), "adx": _n(ax.iloc[-1]),
            "atr": _n(a.iloc[-1]), "vwap": _n(vwap(df).iloc[-1]), "structure": {1: "haussière", -1: "baissière", 0: "indéfinie"}[t],
            "last_event": ev, "ema_position": {k: ("au-dessus" if px > v else "sous") for k, v in (("ema20", e20), ("ema50", e50), ("ema200", e200))},
            "momentum": "haussier" if h > 0 else "baissier", "last_candle": df.index[-1].isoformat(),
            "components": comps, "plan": plan}

def aggregate(results):
    ok = [r for r in results if r.get("usable")]
    if len(ok) < 4:
        return {"signal": NO_DATA, "score": None, "reason": f"{len(ok)}/{len(results)} timeframes exploitables (minimum 4)"}
    w = sum(WEIGHTS[r["tf"]] for r in ok)
    sc = sum(r["score"] * WEIGHTS[r["tf"]] for r in ok) / w
    bull, bear = (sum(r["state"] == s for r in ok) / len(ok) for s in ("HAUSSIER", "BAISSIER"))
    sig = "LONG" if sc >= 35 and bull >= .6 else "SHORT" if sc <= -35 and bear >= .6 else "ATTENTE"
    return {"signal": sig, "score": round(sc, 1), "bull_share": round(bull, 2), "bear_share": round(bear, 2),
            "tfs_used": [r["tf"] for r in ok], "tfs_unavailable": [r["tf"] for r in results if not r.get("usable")],
            "formula": "moyenne pondérée des scores par timeframe (poids dans WEIGHTS) ; LONG/SHORT si |score| ≥ 35 et ≥ 60 % des TF concordent"}

async def analyze_symbol(symbol, fetch, tfs=TFS, ref_tf="1h", now=None, strategy=DEFAULT_STRATEGY):
    now = now or pd.Timestamp.now(tz="UTC")
    async def one(tf):
        try: df, meta = await fetch(symbol, tf)
        except Exception as e:
            return {"tf": tf, "state": "INDISPONIBLE", "signal": NO_DATA, "usable": False, "reason": str(e)}
        res = analyze(df, tf, now, strategy=strategy); res["source"] = meta
        return res
    results = await asyncio.gather(*[one(t) for t in tfs])
    g = aggregate(results)
    ref = next((r for r in results if r["tf"] == ref_tf), {})
    opp = {"LONG": "SHORT", "SHORT": "LONG"}
    gated = strategy in ("v1", "v2")      # stratégies de tendance : pas de plan à contre-sens du signal global ; v3/v4 suivent leur propre règle, comme dans le backtest
    plan = ref.get("plan") if ref.get("signal") in opp and (not gated or g["signal"] not in (opp[ref["signal"]], NO_DATA)) else None
    return {"symbol": symbol, "strategy": strategy, "generated_at": now.isoformat(), "global": g, "plan": plan, "timeframes": list(results),
            "notice": "Analyse statistique à but éducatif / paper trading. Aucun signal n'est garanti. Aucun ordre réel n'est envoyé."}
