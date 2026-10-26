"""Backtest walk-forward : à chaque clôture de bougie t, le moteur ne voit que les bougies <= t (pas de look-ahead)."""
import pandas as pd
from .engine import analyze, MIN_BARS, DEFAULT_STRATEGY

ASSUMPTIONS = [
    "Signal calculé sur bougies clôturées uniquement ; entrée à l'ouverture de la bougie suivante (pas à la clôture du signal).",
    "SL = niveau du plan ; objectif = TP1/TP2/TP3 du plan selon le paramètre `tp` ; si SL et objectif sont touchés dans la même bougie, le SL est retenu.",
    "Un trade est ignoré si l'ouverture suivante dépasse déjà le SL ou l'objectif (gap). Une seule position à la fois.",
    "Sortie forcée à la clôture après 100 bougies (TIMEOUT). Un trade encore ouvert à la fin des données n'est pas compté.",
    "Frais en % par côté et spread total en % (moitié par côté) : valeurs configurées, jamais devinées. Le slippage est une ESTIMATION (entrée, SL, TIMEOUT ; pas sur les objectifs).",
    "Fenêtre glissante de 500 bougies par évaluation (comme en direct). Les performances passées ne préjugent pas des résultats futurs.",
    "Stratégies v2, v3 et v4 : un signal est ignoré si les coûts aller-retour estimés (frais + spread + slippage) dépassent 25 % du risque du trade (v4 : du plus petit entre le risque et le gain visé), compté dans « signaux ignorés ».",
    "Stratégie v4 : sortie forcée après 20 bougies (TIMEOUT) au lieu de 100 ; objectifs proches du prix, donc le taux de réussite vient surtout de la géométrie stop/objectif.",
]

def summarize(trades, risk_pct, buy_hold_pct=None):
    out = {"trades": len(trades), "buy_hold_pct": buy_hold_pct}
    if not trades: return {**out, "note": "aucun trade sur la période"}
    rs = [t["r"] for t in trades]
    w, l = [x for x in rs if x > 0], [x for x in rs if x < 0]
    eq = peak = 1.0; ddp = cum = peak_r = ddr = 0.0
    for x in rs:
        eq *= 1 + x * risk_pct / 100; peak = max(peak, eq); ddp = max(ddp, (peak - eq) / peak * 100)
        cum += x; peak_r = max(peak_r, cum); ddr = max(ddr, peak_r - cum)
    out.update(win_rate=100 * len(w) / len(rs), profit_factor=sum(w) / -sum(l) if l else None, expectancy_r=sum(rs) / len(rs),
               total_r=sum(rs), max_dd_r=ddr, max_dd_pct=ddp, return_pct=(eq - 1) * 100,
               exits={k: sum(t["reason"] == k for t in trades) for k in ("TP", "SL", "TIMEOUT")})
    if len(trades) < 30: out["warning"] = "moins de 30 trades : résultat statistiquement non significatif"
    return out

def simulate(df, tf, fee_pct=0.0, spread_pct=0.0, slippage_pct=0.0, risk_pct=1.0, tp_index=1, max_hold=100, window=500, strategy=DEFAULT_STRATEGY, max_cost_r=0.25):
    df = df[df["closed"]]
    n = len(df)
    if n < MIN_BARS + 2: return [], df, 0
    o, h, l, c = (df[k].values for k in ("open", "high", "low", "close"))
    adverse = spread_pct / 200 + slippage_pct / 100          # coût d'une exécution au marché, par côté
    trades, t, skipped = [], MIN_BARS - 1, 0
    while t < n - 1:
        plan = analyze(df.iloc[max(0, t + 1 - window):t + 1], tf, strategy=strategy, fast=True).get("plan")
        if not plan: t += 1; continue
        d = 1 if plan["direction"] == "LONG" else -1
        fill, sl, tp = o[t + 1] * (1 + d * adverse), plan["stop_loss"], plan["tp"][tp_index]
        risk = d * (fill - sl)
        if risk <= 0: t += 1; continue
        if d * (tp - fill) <= 0:
            if strategy == "v4": skipped += 1                  # v4 : l'objectif proche est déjà dépassé par les coûts/le gap -> signal compté comme ignoré
            t += 1; continue
        base = min(risk, d * (tp - fill)) if strategy == "v4" else risk       # v4 : objectif proche -> coûts comparés au gain visé
        if strategy != "v1" and (2 * adverse + 2 * fee_pct / 100) * fill / base > max_cost_r: skipped += 1; t += 1; continue   # coûts trop lourds vs risque
        hold = plan.get("max_hold") or max_hold
        px = why = None
        for j in range(t + 1, min(n, t + 1 + hold)):
            if (l[j] <= sl) if d == 1 else (h[j] >= sl): px, why = sl * (1 - d * adverse), "SL"; break
            if (h[j] >= tp) if d == 1 else (l[j] <= tp): px, why = tp * (1 - d * spread_pct / 200), "TP"; break
        else:
            if j - t < hold: break                       # données épuisées : trade non terminé, non compté
            px, why = c[j] * (1 - d * adverse), "TIMEOUT"
        r = (d * (px - fill) - (fill + px) * fee_pct / 100) / risk
        trades.append({"entry_i": t + 1, "exit_i": j, "entry_time": df.index[t + 1].isoformat(), "exit_time": df.index[j].isoformat(),
                       "direction": plan["direction"], "entry": fill, "exit": px, "reason": why, "r": r})
        t = j
    return trades, df, skipped

def backtest(df, tf, fee_pct=0.0, spread_pct=0.0, slippage_pct=0.0, risk_pct=1.0, tp_index=1, max_hold=100, window=500, strategy=DEFAULT_STRATEGY):
    trades, d, skipped = simulate(df, tf, fee_pct, spread_pct, slippage_pct, risk_pct, tp_index, max_hold, window, strategy)
    if len(d) < MIN_BARS + 2: return {"trades": 0, "note": f"données insuffisantes ({len(d)} bougies clôturées)"}
    res = summarize(trades, risk_pct, float(d["close"].iloc[-1] / d["close"].iloc[MIN_BARS - 1] - 1) * 100)
    return {**res, "skipped_cost": skipped, "strategy": strategy, "period": [d.index[MIN_BARS - 1].isoformat(), d.index[-1].isoformat()], "last_trades": trades[-10:]}
