import asyncio, json, logging, os, sqlite3
from contextlib import asynccontextmanager
import pandas as pd
from typing import Literal
from pydantic import BaseModel, Field
from fastapi import FastAPI, Request, Response, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from .serialize import candles_records
from . import config, paper, sessions
from .connectors import MarketData, ProviderError, STATUS
from .auth import check_basic
from .backtest import backtest, ASSUMPTIONS
from .engine import analyze_symbol, TFS
from .macro import Macro
from .news import News

log = logging.getLogger("cta")
logging.basicConfig(level=logging.INFO)
md = MarketData()
mc = Macro()
nw = News()

def init_db():
    os.makedirs(os.path.dirname(config.DB_PATH) or ".", exist_ok=True)
    with sqlite3.connect(config.DB_PATH) as c:
        c.execute("CREATE TABLE IF NOT EXISTS signal_log(id INTEGER PRIMARY KEY, ts TEXT, symbol TEXT, signal TEXT, score REAL, plan TEXT)")

def record(res):  # n'enregistre que les changements de signal
    g, sym = res["global"], res["symbol"]
    with sqlite3.connect(config.DB_PATH) as c:
        row = c.execute("SELECT signal FROM signal_log WHERE symbol=? ORDER BY id DESC LIMIT 1", (sym,)).fetchone()
        if row is None or row[0] != g["signal"]:
            c.execute("INSERT INTO signal_log(ts,symbol,signal,score,plan) VALUES(?,?,?,?,?)", (res["generated_at"], sym, g["signal"], g.get("score"), json.dumps(res["plan"])))
            log.info("Changement de signal %s -> %s", sym, g["signal"])

async def settle_open():
    frames = {}
    for sym in {p["symbol"] for p in paper.positions("OPEN", None)}:
        try: frames[sym] = (await md.fetch(sym, "1m", 1000))[0]
        except ProviderError as e: log.warning("règlement %s impossible : %s", sym, e)
    for pid in paper.settle(frames): log.info("position paper #%s clôturée (SL/TP)", pid)

async def background():
    while True:
        for s in config.WATCHLIST:
            try: record(await analyze_symbol(s, md.fetch))
            except Exception: log.exception("analyse %s", s)
        try: await settle_open()
        except Exception: log.exception("règlement paper")
        await asyncio.sleep(config.ANALYSIS_INTERVAL)

async def news_background():
    while True:
        try: await nw.refresh(); await nw.score_ai()
        except Exception: log.exception("actualités")
        await asyncio.sleep(30)

@asynccontextmanager
async def lifespan(_):
    if config.REQUIRE_PASSWORD and not config.APP_PASSWORD:
        raise RuntimeError("APP_PASSWORD obligatoire (REQUIRE_PASSWORD=1) : refus de démarrer un site public sans mot de passe")
    init_db(); paper.init()
    tasks = [asyncio.create_task(background()), asyncio.create_task(news_background())]
    yield
    for t in tasks: t.cancel()

app = FastAPI(title="Crypto Trading AI — phase 1", lifespan=lifespan)
@app.middleware("http")
async def basic_auth(request: Request, call_next):
    if config.APP_PASSWORD and request.url.path != "/api/health" and not check_basic(request.headers.get("authorization"), config.APP_PASSWORD):
        return Response("Authentification requise", 401, headers={"WWW-Authenticate": 'Basic realm="Crypto Trading AI"'})
    return await call_next(request)

SYM = Query(pattern=r"^[A-Za-z0-9]{2,10}$")

@app.get("/api/health")
def health(): return {"status": "ok", "mode": "analyse + paper trading uniquement (aucun ordre réel)", "ephemeral_storage": config.EPHEMERAL_STORAGE}

@app.get("/api/sources")
def sources(): return STATUS

@app.get("/api/candles")
async def candles(symbol: str = SYM, tf: str = "1h", limit: int = Query(300, le=1000)):
    if tf not in TFS: raise HTTPException(400, f"tf doit être parmi {TFS}")
    try: df, meta = await md.fetch(symbol.upper(), tf, 1000)
    except ProviderError as e: raise HTTPException(502, str(e))
    return {"source": meta, "candles": candles_records(df, limit)}

@app.get("/api/analysis")
async def analysis(symbol: str = SYM, ref_tf: str = "1h", strat: Literal["v1", "v2", "v3", "v4"] = "v2"):
    return await analyze_symbol(symbol.upper(), md.fetch, ref_tf=ref_tf, strategy=strat)

@app.get("/api/signals/history")
def history(symbol: str | None = None, limit: int = 100):
    with sqlite3.connect(config.DB_PATH) as c:
        q, a = "SELECT ts,symbol,signal,score,plan FROM signal_log" + (" WHERE symbol=?" if symbol else "") + " ORDER BY id DESC LIMIT ?", ([symbol.upper()] if symbol else []) + [limit]
        return [dict(zip(("ts", "symbol", "signal", "score", "plan"), r)) for r in c.execute(q, a)]

class ResetReq(BaseModel):
    capital: float = Field(gt=0); risk_pct: float = 1.0; fee_pct: float = 0.0; spread_pct: float = 0.0

class OpenReq(BaseModel):
    symbol: str = Field(pattern=r"^[A-Za-z0-9]{2,10}$"); direction: Literal["LONG", "SHORT"]
    sl: float = Field(gt=0); tp: float | None = Field(None, gt=0); qty: float | None = Field(None, gt=0); risk_pct: float | None = Field(None, gt=0)

async def price_of(symbol):
    try: df, meta = await md.fetch(symbol, "1m", 1000)
    except ProviderError as e: raise HTTPException(502, str(e))
    age = (pd.Timestamp.now(tz="UTC") - df.index[-1]).total_seconds()
    if age > 180: raise HTTPException(503, f"prix périmé ({int(age)} s) : opération simulée refusée")
    return float(df["close"].iloc[-1]), meta

@app.get("/api/paper/account")
async def p_account():
    await settle_open()
    a = paper.account(); eq, note = a["balance"], None
    for p in paper.positions("OPEN", None):
        try: eq += paper.unrealized(p, (await price_of(p["symbol"]))[0], a["spread_pct"])
        except HTTPException as e: eq, note = None, f"equity indisponible : {e.detail}"; break
    return {**a, "equity": eq, "note": note, "method": paper.METHOD}

@app.get("/api/paper/positions")
async def p_positions(status: Literal["all", "open", "closed"] = "all"):
    rows, a = paper.positions(None if status == "all" else status.upper()), paper.account()
    for p in rows:
        if p["status"] == "OPEN":
            try: p["pnl_latent"] = paper.unrealized(p, (await price_of(p["symbol"]))[0], a["spread_pct"])
            except HTTPException: p["pnl_latent"] = None
    return rows

@app.post("/api/paper/open")
async def p_open(o: OpenReq):
    price, meta = await price_of(o.symbol.upper())
    try: pid = paper.open_position(o.symbol.upper(), o.direction, price, o.sl, o.tp, o.qty, o.risk_pct, meta["name"])
    except ValueError as e: raise HTTPException(400, str(e))
    return {"id": pid, "price": price, "source": meta}

@app.post("/api/paper/close/{pid}")
async def p_close(pid: int):
    p = next((x for x in paper.positions("OPEN", None) if x["id"] == pid), None)
    if not p: raise HTTPException(404, "position ouverte introuvable")
    price, _ = await price_of(p["symbol"])
    return {"pnl": paper.close_position(pid, price, "MANUEL")}

@app.post("/api/paper/reset")
def p_reset(r: ResetReq):
    try: paper.reset(r.capital, r.risk_pct, r.fee_pct, r.spread_pct)
    except ValueError as e: raise HTTPException(400, str(e))
    return paper.account()

@app.get("/api/paper/stats")
def p_stats(): return paper.stats()

@app.get("/api/backtest")
async def backtest_ep(symbol: str = SYM, tfs: str = "15m,1h,4h", fee_pct: float = Query(0, ge=0, le=5), spread_pct: float = Query(0, ge=0, le=5),
                      slippage_pct: float = Query(0, ge=0, le=5), risk_pct: float = Query(1, gt=0, le=10), tp: int = Query(2, ge=1, le=3), strat: Literal["v1", "v2", "v3", "v4"] = "v2"):
    lst = [t for t in tfs.split(",") if t]
    if not lst or len(lst) > 4 or any(t not in TFS for t in lst): raise HTTPException(400, f"tfs : 1 à 4 valeurs parmi {TFS}")
    out = {}
    for tf in lst:
        try: df, meta = await md.fetch(symbol.upper(), tf, 1000, use_cache=False)
        except ProviderError as e: out[tf] = {"error": str(e)}; continue
        out[tf] = {**await asyncio.to_thread(backtest, df, tf, fee_pct, spread_pct, slippage_pct, risk_pct, tp - 1, strategy=strat), "source": meta}
    return {"symbol": symbol.upper(), "type": "PERFORMANCES HISTORIQUES (aucune garantie future)", "assumptions": ASSUMPTIONS,
            "costs": {"fee_pct": fee_pct, "spread_pct": spread_pct, "slippage_pct_estime": slippage_pct, "risk_pct": risk_pct, "objectif": f"TP{tp}", "strategie": strat}, "results": out}

@app.get("/api/news")
async def news_ep(min_impact: int = Query(0, ge=0, le=3), bias: Literal["up", "down", "neutral"] | None = None, limit: int = Query(80, ge=1, le=200)):
    await nw.refresh()
    return {**nw.status(), "items": nw.listing(min_impact, bias, limit)}

@app.get("/api/sessions")
async def sessions_ep(symbol: str = SYM):
    return await sessions.snapshot(md.fetch, symbol.upper())

@app.get("/api/calendar")
async def calendar_ep(): return await mc.calendar()

PERIOD = Literal["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"]

@app.get("/api/sentiment/all")
async def sentiment_all_ep(period: PERIOD = "1h"): return await mc.sentiment_all(period=period)

@app.get("/api/sentiment")
async def sentiment_ep(symbol: str = SYM, period: PERIOD = "1h"): return await mc.sentiment(symbol.upper(), period)

app.mount("/", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="ui")
