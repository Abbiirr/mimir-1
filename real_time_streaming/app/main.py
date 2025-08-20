# real_time_streaming/app/main.py
from pathlib import Path
from fastapi import FastAPI, Body, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from real_time_streaming.rts_kafka_publisher import publish_click

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5500", "http://localhost:5500"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,  # keep False if you use allow_origins="*"
)

# Serve /static/* from the folder next to this file
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Serve the homepage
@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")

# Button-click endpoint (logs only for now)
@app.post("/api/event")
async def event(request: Request, payload: dict = Body(...)):
    btn = (payload or {}).get("button")
    ua = (request.headers.get("user-agent") or "")[:300]
    ok = publish_click(button=btn, session_id=payload.get("session_id"), user_agent=ua, page="/", extra=None)
    print(f"[EVENT] button={btn}")
    return JSONResponse({"ok": True})
