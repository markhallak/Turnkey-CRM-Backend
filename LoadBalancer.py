from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
import httpx
import asyncio
import json
import aiofiles
from pydantic import BaseModel

app = FastAPI()
BACKENDS_FILE = "backends.json"

class Server(BaseModel):
    url: str

async def load_backends():
    try:
        async with aiofiles.open(BACKENDS_FILE, "r") as f:
            data = await f.read()
            return json.loads(data).get("backends", [])
    except Exception:
        return []

async def save_backends(backends):
    async with aiofiles.open(BACKENDS_FILE, "w") as f:
        await f.write(json.dumps({"backends": backends}))

backends = []
queue_counts = {}
health_status = {}
lock = asyncio.Lock()
client = httpx.AsyncClient(timeout=None)

async def health_check_loop():
    while True:
        async with lock:
            targets = list(backends)
        for url in targets:
            try:
                await client.head(url, timeout=1.0)
                status = True
            except Exception:
                status = False
            async with lock:
                health_status[url] = status
        await asyncio.sleep(0.1)

@app.on_event("startup")
async def startup():
    loaded = await load_backends()
    backends.extend(loaded)
    for url in backends:
        queue_counts[url] = 0
        health_status[url] = False
    asyncio.create_task(health_check_loop())

@app.post("/servers")
async def add_server(server: Server):
    async with lock:
        if server.url not in queue_counts:
            backends.append(server.url)
            queue_counts[server.url] = 0
            health_status[server.url] = False
            await save_backends(backends)
    return {"servers": backends}

@app.delete("/servers")
async def remove_server(server: Server):
    async with lock:
        if server.url in queue_counts:
            backends.remove(server.url)
            queue_counts.pop(server.url, None)
            health_status.pop(server.url, None)
            await save_backends(backends)
    return {"servers": backends}

@app.get("/servers")
async def list_servers():
    async with lock:
        return {"servers": backends}

@app.get("/queue-lengths")
async def list_queue_lengths():
    async with lock:
        return {"queue_lengths": queue_counts, "health": health_status}

@app.middleware("http")
async def load_balancer(request: Request, call_next):
    path = request.url.path
    if path.startswith("/servers") or path == "/queue-lengths":
        return await call_next(request)
    async with lock:
        available = [url for url in backends if health_status.get(url)]
        if not available:
            raise HTTPException(status_code=503, detail="No available backends")
        target = min(available, key=lambda u: queue_counts.get(u, 0))
        queue_counts[target] += 1
    try:
        url = httpx.URL(path=request.url.path, query=request.url.query.encode("utf-8"))
        proxy_req = client.build_request(
            request.method,
            f"{target}{url}",
            headers=request.headers.raw,
            content=await request.body()
        )
        resp = await client.send(proxy_req, stream=True)
        return StreamingResponse(
            resp.aiter_raw(),
            status_code=resp.status_code,
            headers=resp.headers,
            background=BackgroundTask(resp.aclose)
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        async with lock:
            queue_counts[target] -= 1

