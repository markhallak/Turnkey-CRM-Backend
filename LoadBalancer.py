# LoadBalancer.py
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import aiofiles
import httpx
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask

BACKENDS_FILE = "backends.json"

class Server(BaseModel):
    url: str

# ——— Logging Configuration ———
formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger("loadbalancer")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ——— In-memory state ———
backends: list[str] = []
queue_counts: dict[str, int] = {}
health_status: dict[str, bool] = {}
lock = asyncio.Lock()
client = httpx.AsyncClient(timeout=None)

# ——— Persistence Helpers ———
async def load_backends() -> list[str]:
    try:
        async with aiofiles.open(BACKENDS_FILE, "r") as f:
            data = await f.read()
            return json.loads(data).get("backends", [])
    except Exception as e:
        logger.warning(f"Could not load backends file: {e}")
        return []

async def save_backends(backends: list[str]) -> None:
    async with aiofiles.open(BACKENDS_FILE, "w") as f:
        await f.write(json.dumps({"backends": backends}, indent=2))
    logger.info(f"Persisted backends: {backends}")

# ——— Periodic Health-Check ———
async def health_check_loop() -> None:
    while True:
        async with lock:
            targets = list(backends)
        for url in targets:
            try:
                resp = await client.get(f"{url}/connection-test", timeout=2.0)
                ok = resp.status_code == 200
            except Exception as e:
                ok = False
                logger.debug(f"Health check failed for {url}: {e}")
            async with lock:
                health_status[url] = ok
        await asyncio.sleep(5)

# ——— Application Lifespan ———
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    loaded = await load_backends()
    backends.extend(loaded)
    for url in backends:
        queue_counts[url] = 0
        health_status[url] = False

    logger.info(f"Starting health check for backends: {backends}")
    health_task = asyncio.create_task(health_check_loop())

    yield  # ← app runs here

    logger.info("Shutting down health check task and HTTP client")
    health_task.cancel()
    try:
        await health_task
    except asyncio.CancelledError:
        pass
    await client.aclose()

app = FastAPI(lifespan=lifespan)

# ——— Management Endpoints ———
@app.post("/servers")
async def add_server(server: Server):
    async with lock:
        if server.url not in queue_counts:
            backends.append(server.url)
            queue_counts[server.url] = 0
            health_status[server.url] = False
            await save_backends(backends)
            logger.info(f"Added backend: {server.url}")
    return {"servers": backends}

@app.delete("/servers")
async def remove_server(server: Server):
    async with lock:
        if server.url in queue_counts:
            backends.remove(server.url)
            queue_counts.pop(server.url, None)
            health_status.pop(server.url, None)
            await save_backends(backends)
            logger.info(f"Removed backend: {server.url}")
    return {"servers": backends}

@app.get("/servers")
async def list_servers():
    async with lock:
        return {"servers": backends}

@app.get("/queue-lengths")
async def list_queue_lengths():
    async with lock:
        return {"queue_lengths": queue_counts, "health": health_status}

# ——— Load-Balancer Middleware ———
@app.middleware("http")
async def load_balancer(request: Request, call_next):
    client_ip = request.client.host if request.client else "-"
    path = request.url.path

    # 1) Bypass management endpoints
    if path.startswith("/servers") or path == "/queue-lengths":
        return await call_next(request)

    # 2) Pick least-loaded healthy backend
    async with lock:
        available = [u for u in backends if health_status.get(u)]
        if not available:
            logger.error(f"{client_ip} -> No available backends for {path}")
            raise HTTPException(status_code=503, detail="No available backends")
        target = min(available, key=lambda u: queue_counts[u])
        queue_counts[target] += 1

    logger.info(f"{client_ip} -> {request.method} {path} ➔ {target}")

    # 3) Construct upstream URL properly
    qs = request.url.query
    upstream_url = target + path + (f"?{qs}" if qs else "")

    # 4) Proxy with retries and manual stream-manager enter/exit
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            # Create, but don’t “await” the async-context manager
            manager = client.stream(
                request.method,
                upstream_url,
                headers=request.headers.raw,
                content=await request.body(),
                timeout=None
            )

            # Manually enter to get the Response
            resp = await manager.__aenter__()
            logger.info(f"Response {resp.status_code} from {target} (attempt {attempt})")

            # Return a StreamingResponse that will close the manager in background
            return StreamingResponse(
                resp.aiter_bytes(),
                status_code=resp.status_code,
                headers=resp.headers,
                background=BackgroundTask(manager.__aexit__, None, None, None)
            )

        except httpx.RequestError as e:
            logger.warning(f"Attempt {attempt} to {upstream_url} failed: {e}")
            if attempt == max_retries:
                logger.error(f"All {max_retries} attempts failed for {client_ip} -> {path}")
                raise HTTPException(status_code=502, detail=str(e))
            await asyncio.sleep(1)

        finally:
            async with lock:
                queue_counts[target] -= 1

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8100, log_level="info")
