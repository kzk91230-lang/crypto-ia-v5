import pandas as pd
from .indicators import ema

def candles_records(df, limit):
    """Bougies + EMA 20/50/200 calculées côté serveur (None tant que la période n'est pas atteinte)."""
    full = df.copy()
    for n in (20, 50, 200):
        e = ema(full["close"], n)
        e.iloc[:n - 1] = float("nan")
        full[f"ema{n}"] = e
    out = full.tail(limit).rename_axis("time").reset_index()
    out["time"] = out["time"].map(lambda t: int(t.timestamp()))
    return out.astype(object).where(out.notna(), None).to_dict("records")
