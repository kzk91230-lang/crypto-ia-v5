"""Paper trading : simulation uniquement, aucun ordre réel, indépendant de tout courtier."""
import os, sqlite3
from contextlib import contextmanager
import pandas as pd
from . import config

METHOD = ("Exécution simulée au dernier prix du flux de données (fournisseur indiqué), spread = écart total configuré "
          "(moitié par côté), frais en % par côté. SL/TP réglés sur bougies 1m clôturées, exécutés au niveau exact ; "
          "si SL et TP sont touchés dans la même bougie, le SL est retenu (hypothèse prudente). Taille calculée sur le solde réalisé.")
SCHEMA = """CREATE TABLE IF NOT EXISTS paper_account(id INTEGER PRIMARY KEY CHECK(id=1), capital REAL, risk_pct REAL, fee_pct REAL, spread_pct REAL, created_at TEXT);
CREATE TABLE IF NOT EXISTS paper_positions(id INTEGER PRIMARY KEY, symbol TEXT, direction TEXT, qty REAL, entry REAL, sl REAL, tp REAL,
 opened_at TEXT, status TEXT DEFAULT 'OPEN', exit_price REAL, exit_reason TEXT, closed_at TEXT, fees REAL DEFAULT 0, pnl REAL, risk REAL, source TEXT);"""
_now = lambda: pd.Timestamp.now(tz="UTC").isoformat()
_dir = lambda d: {"LONG": 1, "SHORT": -1}[d]

@contextmanager
def _db():
    os.makedirs(os.path.dirname(config.DB_PATH) or ".", exist_ok=True)
    c = sqlite3.connect(config.DB_PATH); c.row_factory = sqlite3.Row
    try:
        yield c; c.commit()
    finally: c.close()

def init():
    with _db() as c:
        c.executescript(SCHEMA)
        if not c.execute("SELECT 1 FROM paper_account").fetchone():
            c.execute("INSERT INTO paper_account VALUES(1,10000,1,0,0,?)", (_now(),))

def account():
    with _db() as c:
        a = dict(c.execute("SELECT * FROM paper_account").fetchone())
        a["balance"] = a["capital"] + c.execute("SELECT COALESCE(SUM(pnl),0) FROM paper_positions WHERE status='CLOSED'").fetchone()[0]
    return a

def reset(capital, risk_pct=1.0, fee_pct=0.0, spread_pct=0.0):
    if not (capital > 0 and 0 < risk_pct <= 10 and 0 <= fee_pct <= 5 and 0 <= spread_pct <= 5):
        raise ValueError("paramètres invalides (capital > 0, risque 0-10 %, frais et spread 0-5 %)")
    with _db() as c:
        c.execute("DELETE FROM paper_positions")
        c.execute("UPDATE paper_account SET capital=?,risk_pct=?,fee_pct=?,spread_pct=?,created_at=?", (capital, risk_pct, fee_pct, spread_pct, _now()))

def open_position(symbol, direction, price, sl, tp=None, qty=None, risk_pct=None, source=None):
    if direction not in ("LONG", "SHORT"): raise ValueError("direction : LONG ou SHORT")
    a, d = account(), _dir(direction)
    entry = price * (1 + d * a["spread_pct"] / 200)
    if d * (entry - sl) <= 0: raise ValueError(f"stop-loss du mauvais côté de l'entrée ({entry:.6g})")
    if tp is not None and d * (tp - entry) <= 0: raise ValueError(f"take-profit du mauvais côté de l'entrée ({entry:.6g})")
    dist, bal = abs(entry - sl), a["balance"]
    max_risk = a["risk_pct"] / 100 * bal
    if qty is None: qty = min(risk_pct or a["risk_pct"], a["risk_pct"]) / 100 * bal / dist
    if qty <= 0 or bal <= 0: raise ValueError("taille ou solde nul")
    if qty * dist > max_risk * (1 + 1e-9): raise ValueError(f"risque {qty * dist:.2f} > maximum autorisé {max_risk:.2f}")
    with _db() as c:
        return c.execute("INSERT INTO paper_positions(symbol,direction,qty,entry,sl,tp,opened_at,fees,risk,source) VALUES(?,?,?,?,?,?,?,?,?,?)",
                         (symbol, direction, qty, entry, sl, tp, _now(), qty * entry * a["fee_pct"] / 100, qty * dist, source)).lastrowid

def positions(status=None, limit=50):
    q = "SELECT * FROM paper_positions" + (" WHERE status=?" if status else "") + " ORDER BY id DESC" + (" LIMIT %d" % limit if limit else "")
    with _db() as c: return [dict(r) for r in c.execute(q, (status,) if status else ())]

def unrealized(p, price, spread_pct=0.0):
    d = _dir(p["direction"])
    return d * (price * (1 - d * spread_pct / 200) - p["entry"]) * p["qty"] - p["fees"]

def close_position(pid, price, reason="MANUEL", apply_spread=True, ts=None):
    p = next((x for x in positions("OPEN", None) if x["id"] == pid), None)
    if not p: raise ValueError(f"position ouverte #{pid} introuvable")
    a, d = account(), _dir(p["direction"])
    exit_ = price * (1 - d * (a["spread_pct"] / 200 if apply_spread else 0))
    fees = p["fees"] + p["qty"] * exit_ * a["fee_pct"] / 100
    pnl = d * (exit_ - p["entry"]) * p["qty"] - fees
    with _db() as c:
        c.execute("UPDATE paper_positions SET status='CLOSED',exit_price=?,exit_reason=?,closed_at=?,fees=?,pnl=? WHERE id=?", (exit_, reason, ts or _now(), fees, pnl, pid))
    return pnl

def hit(p, df):
    """Premier SL/TP touché sur bougies 1m clôturées postérieures à l'ouverture (SL prioritaire)."""
    t, d = pd.Timestamp(p["opened_at"]), _dir(p["direction"])
    for ts, r in df.iterrows():
        if not r["closed"] or ts <= t: continue
        if (r["low"] <= p["sl"]) if d == 1 else (r["high"] >= p["sl"]): return "SL", p["sl"], ts
        if p["tp"] is not None and ((r["high"] >= p["tp"]) if d == 1 else (r["low"] <= p["tp"])): return "TP", p["tp"], ts
    return None

def settle(frames):
    done = []
    for p in positions("OPEN", None):
        if p["symbol"] in frames and (h := hit(p, frames[p["symbol"]])):
            close_position(p["id"], h[1], h[0], apply_spread=False, ts=h[2].isoformat()); done.append(p["id"])
    return done

def stats():
    with _db() as c: rows = [dict(r) for r in c.execute("SELECT pnl,risk FROM paper_positions WHERE status='CLOSED' ORDER BY id")]
    if not rows: return {"trades": 0}
    cap, pn = account()["capital"], [r["pnl"] for r in rows]
    w, l, eq, peak, dd = [x for x in pn if x > 0], [x for x in pn if x < 0], cap, cap, 0.0
    for x in pn:
        eq += x; peak = max(peak, eq); dd = max(dd, (peak - eq) / peak * 100 if peak > 0 else 0)
    return {"trades": len(pn), "win_rate": 100 * len(w) / len(pn), "profit_factor": sum(w) / -sum(l) if l else None,
            "expectancy": sum(pn) / len(pn), "max_drawdown_pct": dd, "net_pnl": sum(pn),
            "avg_r": sum(r["pnl"] / r["risk"] for r in rows) / len(rows)}
