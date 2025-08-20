# real_time_streaming/app/main.py
from pathlib import Path
from fastapi import FastAPI, Body, Request, WebSocket
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from real_time_streaming.rts_kafka_publisher import publish_click
from real_time_streaming.rts_clicks_stream import aggregator  # ← NEW

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5500", "http://localhost:5500"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")

@app.post("/api/event")
async def event(request: Request, payload: dict = Body(...)):
    btn = (payload or {}).get("button")
    ua = (request.headers.get("user-agent") or "")[:300]
    ok = publish_click(button=btn, session_id=payload.get("session_id"), user_agent=ua, page="/", extra=None)
    print(f"[EVENT] button={btn}")
    return JSONResponse({"ok": bool(ok)})

# ---- Streaming lifecycle (start/stop) ----------------------------------------
@app.on_event("startup")
def _start_stream():
    aggregator.start()

@app.on_event("shutdown")
def _stop_stream():
    aggregator.stop()

# ---- WebSocket: push latest counts every 1s ----------------------------------
@app.websocket("/ws/clicks")
async def ws_clicks(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await asyncio_sleep(1.0)
            await ws.send_json({"counts": aggregator.snapshot()})
    finally:
        await ws.close()

# small awaitable sleep without importing asyncio at top-level
async def asyncio_sleep(seconds: float):
    import asyncio
    await asyncio.sleep(seconds)
