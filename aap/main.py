# app/main.py
import asyncio
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import time
import traceback

from .config import SECRET, EMAIL, TIMEOUT_SECONDS
from .worker import solve_quiz_chain

app = FastAPI(title="LLM Analysis Quiz Endpoint")

class QuizRequest(BaseModel):
    email: str
    secret: str
    url: str

@app.post("/", status_code=200)
async def quiz_entry(req: Request):
    try:
        j = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Basic schema validation
    if not all(k in j for k in ("email", "secret", "url")):
        raise HTTPException(status_code=400, detail="Missing fields")

    if j["secret"] != SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    # respond 200 quickly (spec requires 200 JSON response if secret matches)
    # but we must also solve the quiz within 3 minutes; we'll start the worker immediately and return success ping
    # We'll run the worker concurrently but still return 200 to the caller that triggered the POST (the grader expects 200)
    # The grader expects our endpoint to then go solve and submit back; that happens in background here (but within process)
    # NOTE: we can't "do work later" beyond the current response — however the grading service will just check that we submitted the answer
    # We'll spawn an asyncio task to perform the chain; ensure we honor TIMEOUT_SECONDS
    try:
        # start worker task
        asyncio.create_task(run_worker(j["url"], j["secret"]))
        return JSONResponse(status_code=200, content={"status": "accepted"})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def run_worker(url: str, secret: str):
    start = time.time()
    try:
        result = await solve_quiz_chain(url, secret)
        elapsed = time.time() - start
        print(f"[worker] finished in {elapsed:.2f}s result={result}")
    except Exception as e:
        print("[worker] error:", str(e))
        traceback.print_exc()
