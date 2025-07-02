import asyncio
import base64
import json
import os
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional
from uuid import UUID, uuid4

import httpx
import jwt
import nacl.bindings
import uvicorn
from argon2.low_level import hash_secret_raw, Type
from asyncpg import create_pool, Pool, Connection
from casbin import SyncedEnforcer
from casbin_redis_watcher import new_watcher, WatcherOptions
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from databases import Database
from fastapi import FastAPI, HTTPException, Query, Body, Request, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from sqlalchemy import Table, Column, Integer, String, MetaData

from CasbinAdapter import CasbinAdapter
from constants import ASYNCPG_URL, SECRET_KEY, REDIS_URL, KMS_URL
from util import isUUIDv4, createMagicLink, generateJwtRs256, decodeJwtRs256


class SimpleUser:
    def __init__(self, id: UUID, email: str, setup_done: bool, onboarding_done: bool):
        self.id = id
        self.email = email
        self.setup_done = setup_done
        self.onboarding_done = onboarding_done


async def getCurrentUser(request: Request) -> SimpleUser | None:
    email = request.headers.get("x-user-email", None)

    if email:
        async with request.app.state.db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, has_set_recovery_phrase, onboarding_done FROM \"user\" WHERE email=$1",
                email,
            )
        if row:
            return SimpleUser(row["id"], email, row["has_set_recovery_phrase"], row["onboarding_done"])
    return None


def getEnforcer(request: Request) -> SyncedEnforcer:
    return request.app.state.enforcer


async def authorize(request: Request, user: SimpleUser = Depends(getCurrentUser),
                    enforcer: SyncedEnforcer = Depends(getEnforcer)):
    path = request.url.path
    if not path.startswith("/auth"):
        if not user.setup_done and path != "/set-recovery-phrase":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="set-recovery-phrase")
        if user.setup_done and not user.onboarding_done and path != "/onboarding":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="onboarding")
        sub = str(user.email)
        domain = "*"
        obj = path
        act = request.method.lower()

        if not enforcer.enforce(sub, domain, obj, act):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    return True


async def verifySession(request: Request):
    token = request.cookies.get("session")
    if not token:
        raise HTTPException(status_code=401, detail="unauthenticated")
    try:
        data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except Exception:
        raise HTTPException(status_code=401, detail="unauthenticated")
    redis = request.app.state.redis
    jti = data.get("jti")
    sub = data.get("sub")
    if not jti or not sub:
        raise HTTPException(status_code=401, detail="unauthenticated")
    if not await redis.sismember("active_jtis", jti):
        raise HTTPException(status_code=401, detail="revoked")
    if await redis.sismember("blacklisted_users", sub):
        raise HTTPException(status_code=403, detail="blacklisted")
    request.state.user_email = sub
    return sub


def decryptPayload(includePublic: bool = False):
    async def _dep(payload: dict = Body(), request: Request = None):
        if "clientPubKey" not in payload:
            return payload
        clientPublicKeyB64 = payload.get("clientPubKey")
        ivB64 = payload.get("nonce")
        ciphertextB64 = payload.get("ciphertext")
        if not clientPublicKeyB64 or not ivB64 or not ciphertextB64:
            raise HTTPException(status_code=400, detail="Invalid payload")
        try:
            clientPublicKey = base64.b64decode(clientPublicKeyB64)
            iv = base64.b64decode(ivB64)
            ciphertext = base64.b64decode(ciphertextB64)
        except Exception:
            raise HTTPException(status_code=400, detail="Bad encoding")
        serverEdPriv = base64.b64decode(request.app.state.ed25519PrivateKey)
        serverEdPub = base64.b64decode(request.app.state.ed25519PublicKey)
        serverEdSecret = serverEdPriv + serverEdPub
        serverCurvePriv = nacl.bindings.crypto_sign_ed25519_sk_to_curve25519(serverEdSecret)
        clientCurvePub = nacl.bindings.crypto_sign_ed25519_pk_to_curve25519(clientPublicKey)
        sharedSecret = nacl.bindings.crypto_scalarmult(serverCurvePriv, clientCurvePub)
        aesGcm = AESGCM(sharedSecret)
        try:
            data = aesGcm.decrypt(iv, ciphertext, None)
        except Exception:
            raise HTTPException(status_code=400, detail="Decrypt failed")
        try:
            payloadObj = json.loads(data.decode())
            payloadObj["_client_pub"] = clientPublicKey
            payloadObj["_nonce"] = iv
            return payloadObj
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid data")

    return _dep


def encryptForClient(data: dict, client_pub: bytes, app: FastAPI) -> dict:
    server_priv = base64.b64decode(app.state.ed25519PrivateKey)
    server_pub = base64.b64decode(app.state.ed25519PublicKey)
    secret = server_priv + server_pub
    server_curve_priv = nacl.bindings.crypto_sign_ed25519_sk_to_curve25519(secret)
    client_curve_pub = nacl.bindings.crypto_sign_ed25519_pk_to_curve25519(client_pub)
    shared = nacl.bindings.crypto_scalarmult(server_curve_priv, client_curve_pub)
    aes = AESGCM(shared)
    iv = os.urandom(12)
    cipher = aes.encrypt(iv, json.dumps(data).encode(), None)
    return {
        "nonce": base64.b64encode(iv).decode(),
        "ciphertext": base64.b64encode(cipher).decode(),
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup logic
    metadata = MetaData()
    casbin_rule = Table(
        "casbin_rule", metadata,
        Column("id", Integer, primary_key=True),
        Column("ptype", String(100), nullable=False),
        Column("subject", String(255), nullable=False),
        Column("domain", String(255), nullable=False),
        Column("object", String(255), nullable=False),
        Column("action", String(255), nullable=False),
        Column("extra", String(255)),
    )
    db = Database(ASYNCPG_URL)
    await db.connect()

    adapter = CasbinAdapter(db=db, table=casbin_rule)
    enforcer = SyncedEnforcer("model.conf", adapter)
    enforcer.enable_auto_save(True)
    opts = WatcherOptions()
    opts.host = "localhost"
    opts.port = "6379"
    watcher = new_watcher(opts)
    watcher.set_update_callback(lambda msg: asyncio.create_task(enforcer.load_policy()))
    enforcer.set_watcher(watcher)

    await adapter.load_policy(enforcer.get_model())
    enforcer.build_role_links()

    app.state.enforcer = enforcer

    loop = asyncio.get_running_loop()
    app.state.db_pool: Pool = await create_pool(
        dsn=ASYNCPG_URL, min_size=5, max_size=20
    )
    print("DB pool created")

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    app.state.redis = redis
    async with app.state.db_pool.acquire() as c:
        rows = await c.fetch("SELECT jti FROM jwt_token WHERE revoked=FALSE AND expires_at>NOW()")
        if rows:
            await redis.sadd("active_jtis", *[r["jti"] for r in rows])
        rows = await c.fetch("SELECT email FROM \"user\" WHERE is_blacklisted=TRUE")
        if rows:
            await redis.sadd("blacklisted_users", *[r["email"] for r in rows])

    async def listen_jwt():
        pub = redis.pubsub()
        await pub.subscribe("jwt_updates")
        async for m in pub.listen():
            if m.get("type") != "message":
                continue
            d = m.get("data")
            if isinstance(d, bytes):
                d = d.decode()
            if d.startswith("add:"):
                await redis.sadd("active_jtis", d[4:])
            elif d.startswith("remove:"):
                await redis.srem("active_jtis", d[7:])

    async def listen_blacklist():
        pub = redis.pubsub()
        await pub.subscribe("blacklist_updates")
        async for m in pub.listen():
            if m.get("type") != "message":
                continue
            d = m.get("data")
            if isinstance(d, bytes):
                d = d.decode()
            if d.startswith("add:"):
                await redis.sadd("blacklisted_users", d[4:])
            elif d.startswith("remove:"):
                await redis.srem("blacklisted_users", d[7:])

    asyncio.create_task(listen_jwt())
    asyncio.create_task(listen_blacklist())

    async with httpx.AsyncClient() as client:
        privRes = await client.get(f"{KMS_URL}/private-key")
        pubRes = await client.get(f"{KMS_URL}/public-key")
        edPrivRes = await client.get(f"{KMS_URL}/ed25519-private-key")
        edPubRes = await client.get(f"{KMS_URL}/ed25519-public-key")
    app.state.privateKey = privRes.json()["privateKey"]
    app.state.publicKey = pubRes.json()["publicKey"]
    app.state.ed25519PrivateKey = edPrivRes.json()["privateKey"]
    app.state.ed25519PublicKey = edPubRes.json()["publicKey"]

    async def refreshKeys():
        while True:
            await asyncio.sleep(21600)
            async with httpx.AsyncClient() as c:
                priv = await c.get(f"{KMS_URL}/private-key")
                pub = await c.get(f"{KMS_URL}/public-key")
                edPriv = await c.get(f"{KMS_URL}/ed25519-private-key")
                edPub = await c.get(f"{KMS_URL}/ed25519-public-key")
            if (
                priv.json()["privateKey"] != app.state.privateKey or
                pub.json()["publicKey"] != app.state.publicKey or
                edPriv.json()["privateKey"] != app.state.ed25519PrivateKey or
                edPub.json()["publicKey"] != app.state.ed25519PublicKey
            ):
                app.state.privateKey = priv.json()["privateKey"]
                app.state.publicKey = pub.json()["publicKey"]
                app.state.ed25519PrivateKey = edPriv.json()["privateKey"]
                app.state.ed25519PublicKey = edPub.json()["publicKey"]

    asyncio.create_task(refreshKeys())

    def _update(_msg: str):
        loop.call_soon_threadsafe(enforcer.load_policy)

    def _on_change(_msg: str):
        asyncio.create_task(adapter.load_policy(enforcer.get_model()))

    watcher.set_update_callback(_on_change)
    watcher.set_update_callback(_update)

    try:
        yield
    finally:
        await app.state.db_pool.close()
        await app.state.redis.close()
        print("DB pool closed")



app = FastAPI(lifespan=lifespan, dependencies=[Depends(authorize)])
origins = [
    "http://localhost:3000",
    # add any other domains you serve your SPA from
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,            # <-- your React app origin
    allow_credentials=True,           # <-- if you send cookies or Authorization headers
    allow_methods=["*"],              # <-- GET, POST, PUT, DELETE, OPTIONS, etc
    allow_headers=["*"],              # <-- allow all request headers
)

def get_db_pool(request: Request) -> Pool:
    return request.app.state.db_pool


async def get_conn(db_pool: Pool = Depends(get_db_pool)):
    async with db_pool.acquire() as conn:
        yield conn



@app.get("/auth/public-key")
async def getPublicKeyEndpoint(request: Request):
    return {"public_key": request.app.state.publicKey}


@app.get("/auth/ed25519-public-key")
async def getEd25519PublicKey(request: Request):
    return {"public_key": request.app.state.ed25519PublicKey}


@app.get("/auth/validate-magic-link")
async def validateMagicLink(token: str, request: Request, conn: Connection = Depends(get_conn)):
    try:
        payload = decodeJwtRs256(token, request.app.state.publicKey)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid")

    uuid_str = payload.get("uuid")
    if not uuid_str or not await isUUIDv4(uuid_str):
        raise HTTPException(status_code=400, detail="invalid")

    row = await conn.fetchrow(
        "SELECT consumed, expires_at FROM magic_link WHERE uuid=$1",
        UUID(uuid_str),
    )
    if (
        not row
        or row["consumed"]
        or row["expires_at"] < datetime.now(timezone.utc)
    ):
        raise HTTPException(status_code=400, detail="invalid")

    return {
        "userEmail": payload.get("userEmail"),
    }


async def uploadDocument(fileBlob: bytes) -> dict:
    """Placeholder for uploading files to storage bucket."""
    return {
        "file_name": "placeholder.pdf",
        "file_url": "https://example.com/placeholder.pdf",
        "file_extension": "pdf",
        "file_size": len(fileBlob or b""),
    }


################################################################################
# TODO:                           HEADER ENDPOINTS                             #
################################################################################

@app.get("/global-search")
async def globalSearch(
        q: str = Query(..., description="Search query string"),
        conn: Connection = Depends(get_conn)
):
    globalSearchSql = """
        SELECT source_table, record_id, search_text, is_deleted
          FROM global_search
         WHERE is_deleted = FALSE
           AND length($1::text) >= 3
           AND search_text ILIKE '%' || $1 || '%'
         ORDER BY source_table, record_id
         LIMIT 10;
    """

    try:
        results = await conn.fetch(globalSearchSql, q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"results": [dict(r) for r in results]}


@app.get("/get-notifications")
async def getNotifications(
        size: int = Query(..., gt=0, description="Number of notifications per page"),
        last_seen_created_at: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        last_seen_id: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple notifications share the same timestamp"
        ),
        conn: Connection = Depends(get_conn)
):
    # parse or default to now
    if last_seen_created_at:
        try:
            dt = datetime.fromisoformat(last_seen_created_at)
            cursor_ts = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(400, detail=f"Invalid timestamp: '{last_seen_created_at}'")
    else:
        cursor_ts = datetime.now(timezone.utc)

    max_uuid = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    cursor_id = last_seen_id or max_uuid

    sql = """
    SELECT
      n.*,
      COUNT(*) OVER() AS total_count
    FROM notification n
    WHERE (n.created_at, n.id) < ($1::timestamptz, $2::uuid)
    ORDER BY n.created_at DESC, n.id DESC
    LIMIT $3;
    """

    try:
        rows = await conn.fetch(sql, cursor_ts, cursor_id, size)
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    total = rows[0]["total_count"] if rows else 0

    if rows:
        last = rows[-1]
        next_ts = last["created_at"].isoformat()
        next_id = str(last["id"])
    else:
        next_ts = None
        next_id = None

    return {
        "notifications": [dict(r) for r in rows],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }


@app.get("/get-profile-details")
async def getProfileDetails(
        email: str = Query(..., description="Email of the user whose profile to fetch"),
        conn: Connection = Depends(get_conn)
):
    sql = """
            SELECT first_name, hex_color
            FROM "user"
            WHERE email = $1
              AND is_deleted = FALSE
            LIMIT 1;
        """

    row = await conn.fetchrow(sql, email)
    if not row:
        raise HTTPException(status_code=404, detail=f"User {email} not found")

    return {
        "first_name": row["first_name"],
        "hex_color": row["hex_color"],
    }


@app.get("/project-assessments/{project_id}")
async def getProjectAssessments(project_id: str, db_pool: Pool = Depends(get_db_pool)):
    if not await isUUIDv4(project_id):
        raise HTTPException(status_code=400, detail="Invalid project id")

    sql = """
    SELECT
      p.id                     AS project_id,
      p.visit_notes,
      p.planned_resolution,
      p.material_parts_needed
    FROM project p
    WHERE
      p.id          = $1
      AND p.is_deleted = FALSE;
    """

    async with db_pool.acquire() as conn:
        rec = await conn.fetchrow(sql, UUID(project_id))
    if not rec:
        raise HTTPException(status_code=404, detail="Not found")
    return dict(rec)


################################################################################
# TODO:                         DASHBOARD ENDPOINTS                            #
################################################################################

@app.get("/get-dashboard-metrics")
async def getDashboardMetrics(conn: Connection = Depends(get_conn)):
    sql = "SELECT * FROM overall_aggregates;"

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"metrics": [dict(r) for r in rows]}


@app.get("/get-calendar-events")
async def getCalendarEvents(
        month: int = Query(
            ...,
            ge=1,
            le=12,
            description="Month index (1–12) to fetch calendar events for"
        ),
        conn: Connection = Depends(get_conn)
):
    sql = """
        SELECT
          p.*,
          COALESCE(p.scheduled_date, p.due_date) AS event_date
        FROM project p
        WHERE
          p.is_deleted = FALSE
          AND (
            (p.scheduled_date IS NOT NULL AND EXTRACT(MONTH FROM p.scheduled_date) = $1)
            OR
            (p.scheduled_date IS NULL     AND EXTRACT(MONTH FROM p.due_date)       = $1)
          )
        ORDER BY event_date, p.id;
    """

    try:
        records = await conn.fetch(sql, month)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # If you want to serialize `event_date` as ISO strings, convert each row to dict
    events = []
    for r in records:
        d = dict(r)
        # asyncpg returns dates as datetime.date, so cast to ISO string:
        d["event_date"] = d["event_date"].isoformat()
        events.append(d)

    return {"events": events}


################################################################################
# TODO:                         PROJECTS PAGE ENDPOINTS                        #
################################################################################


@app.get("/get-projects")
async def getProjects(
        size: int = Query(..., gt=0, description="Number of projects per page"),
        last_seen_created_at: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        last_seen_id: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn)
):
    # 1) parse the timestamp (or default to now)
    if last_seen_created_at:
        try:
            dt = datetime.fromisoformat(last_seen_created_at)
            cursor_ts = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timestamp: '{last_seen_created_at}'"
            )
    else:
        cursor_ts = datetime.now(timezone.utc)

    # 2) default the ID cursor to the MAX‐UUID when not provided
    max_uuid = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    cursor_id = last_seen_id or max_uuid

    # 3) tuple‐comparison + tie‐break by id
    sql = """
    SELECT
      p.*,
      c.company_name,
      s.value AS status_value,
      COUNT(*) OVER() AS total_count
    FROM project p
    JOIN client  c ON c.id = p.client_id
    JOIN status  s ON s.id = p.status_id AND s.category = 'project'
    WHERE
      (p.created_at, p.id) < ($1::timestamptz, $2::uuid)
      AND p.is_deleted = FALSE
    ORDER BY p.created_at DESC, p.id DESC
    LIMIT $3;
    """

    try:
        rows = await conn.fetch(sql, cursor_ts, cursor_id, size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    total = rows[0]["total_count"] if rows else 0

    # Build the next‐page cursors from the last row
    if rows:
        last = rows[-1]
        next_ts = last["created_at"].isoformat()
        next_id = str(last["id"])
    else:
        next_ts = None
        next_id = None

    return {
        "projects": [dict(r) for r in rows],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }


@app.get("/get-project-statuses")
async def getProjectStatuses(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, value, color
            FROM status
            WHERE category = 'project'
              AND is_deleted = FALSE
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"project_statuses": [dict(r) for r in rows]}


@app.get("/get-project-types")
async def getProjectTypes(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, value
            FROM project_type
            WHERE is_deleted = FALSE
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"project_types": [dict(r) for r in rows]}


@app.get("/get-project-trades")
async def getProjectTrades(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, value
            FROM project_trade
            WHERE is_deleted = FALSE
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"project_trades": [dict(r) for r in rows]}


@app.get("/get-project-priorities")
async def getProjectPriorities(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, value, color
            FROM project_priority
            WHERE is_deleted = FALSE
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"project_priorities": [dict(r) for r in rows]}


@app.post("/create-new-project")
async def createNewProject(
        payload: dict = Body(...),
        conn: Connection = Depends(get_conn)
):
    clientId = payload.get("client")
    businessName = payload.get("businessName")
    dateReceived = payload.get("dateReceived")
    priorityValue = payload.get("priority")
    dueDate = payload.get("dueDate")
    tradeValue = payload.get("trade")
    nte = payload.get("nte")
    assigneeId = payload.get("assignee")
    address1 = payload.get("address1")
    address2 = payload.get("address2")
    city = payload.get("city")
    stateName = payload.get("state")
    zipCode = payload.get("zipCode")
    scopeOfWork = payload.get("scopeOfWork")
    specialNotes = payload.get("specialNotes")

    if not all([clientId, businessName, dateReceived]):
        raise HTTPException(status_code=400, detail="Missing required fields")

    address = f"{address1} {address2 or ''} {city} {stateName} {zipCode}".strip()

    async with conn.transaction():
        priorityId = await conn.fetchval(
            "SELECT id FROM project_priority WHERE value=$1 AND is_deleted=FALSE LIMIT 1;",
            priorityValue,
        )
        tradeId = await conn.fetchval(
            "SELECT id FROM project_trade WHERE value=$1 AND is_deleted=FALSE LIMIT 1;",
            tradeValue,
        )
        stateId = await conn.fetchval(
            "SELECT id FROM state WHERE name=$1 AND is_deleted=FALSE LIMIT 1;",
            stateName,
        )
        typeId = await conn.fetchval(
            "SELECT id FROM project_type WHERE is_deleted=FALSE LIMIT 1;"
        )
        statusId = await conn.fetchval(
            "SELECT id FROM status WHERE category='project' AND value='Open' AND is_deleted=FALSE LIMIT 1;"
        )
        projectId = await conn.fetchval(
            """
            INSERT INTO project (
              client_id, priority_id, type_id, address, address_line1, address_line2,
              city, state_id, zip_code, trade_id, status_id, nte, business_name,
              due_date, date_received, scope_of_work, special_notes, visit_notes,
              planned_resolution, material_parts_needed, assignee_id
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,'','','',$18
            ) RETURNING id;
            """,
            clientId,
            priorityId,
            typeId,
            address,
            address1,
            address2,
            city,
            stateId,
            zipCode,
            tradeId,
            statusId,
            nte,
            businessName,
            dueDate,
            dateReceived,
            scopeOfWork,
            specialNotes,
            assigneeId,
        )

    return {"projectId": projectId}


################################################################################
# TODO:                        PROJECT VIEW ENDPOINTS                          #
################################################################################
@app.get("/fetch-project")
async def fetchProject(
        project_id: str = Query(..., description="Project UUID"),
        conn: Connection = Depends(get_conn)
):
    sql = """
        SELECT
          p.id,
          p.po_number,

          -- The “client” for a project is actually stored as a user → then join client:
          p.client_id                            AS client_user_id,
          cu.first_name || ' ' || cu.last_name   AS client_user_name,
          cut.name                               AS client_user_type,
          c.id                                    AS client_id,
          c.company_name                          AS client_company_name,

          p.business_name,
          p.date_received,

          p.priority_id,
          pp.value        AS priority_value,
          pp.color        AS priority_color,

          p.type_id,
          pt.value        AS type_value,

          p.address,
          p.address_line1,
          p.address_line2,
          p.city,

          p.state_id,
          st.name         AS state_name,

          p.zip_code,

          p.trade_id,
          tr.value        AS trade_value,
          tr.color        AS trade_color,

          p.status_id,
          s.value         AS status_value,
          s.color         AS status_color,

          p.nte,
          p.due_date,

          p.scope_of_work,
          p.special_notes,
          p.visit_notes,
          p.planned_resolution,
          p.material_parts_needed,

          p.assignee_id,
          au.first_name || ' ' || au.last_name AS assignee_name,

          p.created_at,
          p.updated_at,
          p.is_deleted
        FROM project p

          JOIN "user" cu
            ON cu.id = p.client_id
           AND cu.is_deleted = FALSE

          LEFT JOIN client c
            ON c.id = cu.client_id
           AND c.is_deleted = FALSE
           AND cut.name = 'client'

          JOIN project_priority pp
            ON pp.id = p.priority_id
           AND pp.is_deleted = FALSE

          JOIN project_type pt
            ON pt.id = p.type_id
           AND pt.is_deleted = FALSE

          JOIN state st
            ON st.id = p.state_id
           AND st.is_deleted = FALSE

          JOIN project_trade tr
            ON tr.id = p.trade_id
           AND tr.is_deleted = FALSE

          JOIN status s
            ON s.id = p.status_id
           AND s.category = 'project'
           AND s.is_deleted = FALSE

          JOIN "user" au
            ON au.id = p.assignee_id
           AND au.is_deleted = FALSE

        WHERE
          p.id = $1
          AND p.is_deleted = FALSE;
        """
    try:
        row = await conn.fetchrow(sql, project_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"project": dict(row)}


@app.get("/get-messages")
async def getMessages(
        projectId: str = Query(..., description="Project UUID"),
        size: int = Query(..., gt=0, description="Number of messages to return"),
        last_seen_created_at: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        last_seen_id: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn)
):
    if last_seen_created_at:
        try:
            dt = datetime.fromisoformat(last_seen_created_at)
            cursor_ts = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timestamp: '{last_seen_created_at}'"
            )
    else:
        cursor_ts = datetime.now(timezone.utc)

    max_uuid = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    cursor_id = last_seen_id or max_uuid

    sql = """
            SELECT
              m.id,
              m.created_at,
              m.updated_at,
              m.content,
              m.sender_id,
              u.first_name  AS sender_first_name,
              u.last_name   AS sender_last_name,
              ut.name       AS sender_type,
              m.file_attachment_id,
              COUNT(*) OVER() AS total_count
            FROM message m
            JOIN project p
              ON p.id = m.project_id
             AND p.is_deleted = FALSE
            JOIN "user" u
              ON u.id = m.sender_id
             AND u.is_deleted = FALSE
            JOIN user_type ut
              ON ut.id = u.type_id
             AND ut.is_deleted = FALSE
            WHERE
              m.is_deleted    = FALSE
              AND m.project_id = $1
              AND (m.created_at, m.id) < ($2::timestamptz, $3::uuid)
            ORDER BY m.created_at DESC, m.id DESC
            LIMIT $4;
        """

    try:
        rows = await conn.fetch(sql, projectId, cursor_ts, cursor_id, size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    total = rows[0]["total_count"] if rows else 0

    if rows:
        last = rows[-1]
        next_ts = last["created_at"].isoformat()
        next_id = str(last["id"])
    else:
        next_ts = None
        next_id = None

    return {
        "messages": [dict(r) for r in rows],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }


@app.get("/fetch-project-quotes")
async def fetchProjectQuotesEndpoint(
        project_id: str = Query(..., description="Project UUID"),
        size: int = Query(..., gt=0, description="Number of quotes to return"),
        last_seen_created_at: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        last_seen_id: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn)
):
    if last_seen_created_at:
        try:
            dt = datetime.fromisoformat(last_seen_created_at)
            cursor_ts = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timestamp: '{last_seen_created_at}'"
            )
    else:
        cursor_ts = datetime.now(timezone.utc)

    max_uuid = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    cursor_id = last_seen_id or max_uuid

    sql = """
        SELECT
          q.id                 AS quote_id,
          q.number             AS number,
          q.created_at         AS date_created,
          q.amount             AS amount,
          s.value              AS status_value,
          COUNT(*) OVER()      AS total_count
        FROM quote q
        JOIN status s
          ON s.id = q.status_id
         AND s.category = 'quote'
         AND s.is_deleted = FALSE
        WHERE
          q.project_id = $1
          AND q.is_deleted = FALSE
          AND (q.created_at, q.id) < ($2::timestamptz, $3::uuid)
        ORDER BY q.created_at DESC, q.id DESC
        LIMIT $4;
        """

    try:
        rows = await conn.fetch(sql, project_id, cursor_ts, cursor_id, size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    total = rows[0]["total_count"] if rows else 0

    if rows:
        last = rows[-1]
        next_ts = last["created_at"].isoformat()
        next_id = str(last["id"])
    else:
        next_ts = None
        next_id = None

    return {
        "quotes": [
            {
                "quote_id": r["quote_id"],
                "number": r["number"],
                "date_created": r["date_created"],
                "amount": r["amount"],
                "status": r["status_value"]
            }
            for r in rows
        ],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }


@app.get("/fetch-project-documents")
async def fetchProjectDocumentsEndpoint(
        project_id: str = Query(..., description="Project UUID"),
        size: int = Query(..., gt=0, description="Number of documents to return"),
        last_seen_created_at: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        last_seen_id: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn)
):
    if last_seen_created_at:
        try:
            dt = datetime.fromisoformat(last_seen_created_at)
            cursor_ts = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timestamp: '{last_seen_created_at}'"
            )
    else:
        cursor_ts = datetime.now(timezone.utc)

    max_uuid = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    cursor_id = last_seen_id or max_uuid

    sql = """
        SELECT
          d.id                  AS document_id,
          d.file_name           AS file_name,
          d.file_extension      AS type,
          d.document_type       AS document_type,
          d.created_at          AS date_uploaded,
          COUNT(*) OVER()       AS total_count
        FROM document d
        WHERE
          d.project_id = $1
          AND d.is_deleted = FALSE
          AND (d.created_at, d.id) < ($2::timestamptz, $3::uuid)
        ORDER BY d.created_at DESC, d.id DESC
        LIMIT $4;
        """

    try:
        rows = await conn.fetch(sql, project_id, cursor_ts, cursor_id, size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    total = rows[0]["total_count"] if rows else 0

    if rows:
        last = rows[-1]
        next_ts = last["created_at"].isoformat()
        next_id = str(last["id"])
    else:
        next_ts = None
        next_id = None

    return {
        "documents": [
            {
                "document_id": r["document_id"],
                "title": r["file_name"],
                "type": r["type"],
                "document_type": r["document_type"],
                "date_uploaded": r["date_uploaded"]
            }
            for r in rows
        ],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }


################################################################################
# TODO:                         CLIENTS PAGE ENDPOINTS                         #
################################################################################

@app.get("/get-clients")
async def getClients(
        size: int = Query(..., gt=0, description="Number of clients per page"),
        last_seen_created_at: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        last_seen_id: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn)
):
    if last_seen_created_at:
        try:
            dt = datetime.fromisoformat(last_seen_created_at)
            cursor_ts = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timestamp: '{last_seen_created_at}'"
            )
    else:
        cursor_ts = datetime.now(timezone.utc)

    max_uuid = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    cursor_id = last_seen_id or max_uuid

    sql = """
            SELECT
              c.id,
              c.company_name,
              ct.value AS type_value,
              c.status_id,
              s.value AS status_value,
              COALESCE(ca.total_collected, 0) AS total_revenue,
              COUNT(*) OVER() AS total_count
            FROM client c
            JOIN status s
              ON s.id = c.status_id
             AND s.category = 'client'
             AND s.is_deleted = FALSE
            JOIN client_type ct
              ON ct.id = c.type_id
             AND ct.is_deleted = FALSE
            LEFT JOIN client_aggregates ca
              ON ca.client_id = c.id
            WHERE (c.created_at, c.id) < ($1::timestamptz, $2::uuid)
              AND c.is_deleted = FALSE
            ORDER BY c.created_at DESC, c.id DESC
            LIMIT $3;
        """

    try:
        rows = await conn.fetch(sql, cursor_ts, cursor_id, size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    total = rows[0]["total_count"] if rows else 0

    if rows:
        last = rows[-1]
        next_ts = last["created_at"].isoformat()
        next_id = str(last["id"])
    else:
        next_ts = None
        next_id = None

    return {
        "clients": [dict(r) for r in rows],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }


@app.get("/get-client-types")
async def getClientTypes(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, value
            FROM client_type
            WHERE is_deleted = FALSE
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"client_types": [dict(r) for r in rows]}


@app.get("/get-client-statuses")
async def getClientStatuses(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, value, color
            FROM status
            WHERE category = 'client'
              AND is_deleted = FALSE
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"client_statuses": [dict(r) for r in rows]}


@app.post("/create-new-client")
async def createNewClient(
        payload: dict = Body(...),
        conn: Connection = Depends(get_conn)
):
    companyName = payload.get("companyName")
    pocFirstName = payload.get("POCFirstName")
    pocLastName = payload.get("POCLastName")
    clientType = payload.get("type")
    onboardingEmail = payload.get("onBoardingEmail")
    phoneNumber = payload.get("phoneNumber")
    accountingEmail = payload.get("accountingEmail")
    accountingPhone = payload.get("accountingPhoneNumber")
    payTerms = payload.get("payterms")
    address1 = payload.get("address1")
    address2 = payload.get("address2")
    city = payload.get("city")
    stateName = payload.get("state")
    zipCode = payload.get("zipCode")
    tripRate = payload.get("tripRate")

    if not all([companyName, pocFirstName, pocLastName]):
        raise HTTPException(status_code=400, detail="Missing required fields")

    async with conn.transaction():
        typeId = await conn.fetchval(
            "SELECT id FROM client_type WHERE value=$1 AND is_deleted=FALSE LIMIT 1;",
            clientType,
        )
        stateId = await conn.fetchval(
            "SELECT id FROM state WHERE name=$1 AND is_deleted=FALSE LIMIT 1;",
            stateName,
        )
        statusId = await conn.fetchval(
            "SELECT id FROM status WHERE category='client' AND value='Active' AND is_deleted=FALSE LIMIT 1;"
        )
        clientId = await conn.fetchval(
            """
            INSERT INTO client (
              company_name, poc_first_name, poc_last_name, type_id, status_id,
              address_line_1, address_line_2, city, state_id, zip_code,
              general_onboarding_email, phone_number_main_line, accounting_email,
              accounting_phone_number, pay_terms, trip_rate, updates, special_notes
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'',''
            ) RETURNING id;
            """,
            companyName,
            pocFirstName,
            pocLastName,
            typeId,
            statusId,
            address1,
            address2,
            city,
            stateId,
            zipCode,
            onboardingEmail,
            phoneNumber,
            accountingEmail,
            accountingPhone,
            payTerms,
            tripRate,
        )

        rates = {
            "handyman": payload.get("hourlyRateHandyMan"),
            "helper": payload.get("hourlyRateHelper"),
            "electrical": payload.get("hourlyRateElectrical"),
            "plumbing": payload.get("hourlyRatePlumbing"),
            "hvac": payload.get("hourlyRateHVAC"),
        }
        for rateType, amount in rates.items():
            if not amount:
                continue
            await conn.execute(
                "INSERT INTO client_rate (client_id, rate_type, rate_amount) VALUES ($1,$2,$3);",
                clientId,
                rateType,
                amount,
            )

    return {"clientId": clientId}


################################################################################
# TODO:                         CLIENTS VIEW ENDPOINTS                         #
################################################################################

@app.get("/fetch-client")
async def fetchClient(
        client_id: str = Query(..., description="Client UUID"),
        conn: Connection = Depends(get_conn)
):
    sql = """
        SELECT
          c.id,
          c.company_name,
          c.address_line_1,
          c.city,
          st.name  AS state_name,
          c.status_id,
          s.value  AS status_value,
          c.zip_code,
          c.updates,
          c.special_notes
        FROM client c
        JOIN state st ON st.id = c.state_id AND st.is_deleted = FALSE
        JOIN status s ON s.id = c.status_id AND s.category = 'client' AND s.is_deleted = FALSE
        WHERE c.id = $1 AND c.is_deleted = FALSE;
    """

    try:
        row = await conn.fetchrow(sql, client_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not row:
        raise HTTPException(status_code=404, detail=f"Client {client_id} not found")

    return {"client": dict(row)}


@app.get("/fetch-client-invoices")
async def fetchClientInvoices(
        client_id: str = Query(..., description="Client UUID"),
        size: int = Query(..., gt=0, description="Number of invoices to return"),
        last_seen_created_at: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        last_seen_id: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn)
):
    if last_seen_created_at:
        try:
            dt = datetime.fromisoformat(last_seen_created_at)
            cursor_ts = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timestamp: '{last_seen_created_at}'"
            )
    else:
        cursor_ts = datetime.now(timezone.utc)

    max_uuid = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    cursor_id = last_seen_id or max_uuid

    sql = """
        SELECT
          i.id            AS invoice_id,
          i.number        AS number,
          i.created_at    AS date_created,
          i.issuance_date AS issuance_date,
          i.amount        AS amount,
          s.value         AS status_value,
          COUNT(*) OVER() AS total_count
        FROM invoice i
        JOIN status s ON s.id = i.status_id AND s.category = 'invoice' AND s.is_deleted = FALSE
        WHERE
          i.client_id = $1
          AND i.is_deleted = FALSE
          AND (i.created_at, i.id) < ($2::timestamptz, $3::uuid)
        ORDER BY i.created_at DESC, i.id DESC
        LIMIT $4;
    """

    try:
        rows = await conn.fetch(sql, client_id, cursor_ts, cursor_id, size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    total = rows[0]["total_count"] if rows else 0

    if rows:
        last = rows[-1]
        next_ts = last["created_at"].isoformat()
        next_id = str(last["id"])
    else:
        next_ts = None
        next_id = None

    return {
        "invoices": [dict(r) for r in rows],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }


@app.get("/fetch-client-onboarding-documents")
async def fetchClientOnboardingDocuments(
        client_id: str = Query(..., description="Client UUID"),
        size: int = Query(..., gt=0, description="Number of documents to return"),
        last_seen_created_at: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        last_seen_id: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn)
):
    if last_seen_created_at:
        try:
            dt = datetime.fromisoformat(last_seen_created_at)
            cursor_ts = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timestamp: '{last_seen_created_at}'"
            )
    else:
        cursor_ts = datetime.now(timezone.utc)

    max_uuid = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    cursor_id = last_seen_id or max_uuid

    sql = """
        SELECT
          d.id             AS document_id,
          d.file_name      AS file_name,
          d.file_extension AS type,
          d.document_type  AS document_type,
          d.created_at     AS date_uploaded,
          COUNT(*) OVER()  AS total_count
        FROM document d
        WHERE
          d.client_id = $1
          AND d.purpose = 'onboarding_paperwork'
          AND d.is_deleted = FALSE
          AND (d.created_at, d.id) < ($2::timestamptz, $3::uuid)
        ORDER BY d.created_at DESC, d.id DESC
        LIMIT $4;
    """

    try:
        rows = await conn.fetch(sql, client_id, cursor_ts, cursor_id, size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    total = rows[0]["total_count"] if rows else 0

    if rows:
        last = rows[-1]
        next_ts = last["created_at"].isoformat()
        next_id = str(last["id"])
    else:
        next_ts = None
        next_id = None

    return {
        "documents": [
            {
                "document_id": r["document_id"],
                "title": r["file_name"],
                "type": r["type"],
                "document_type": r["document_type"],
                "date_uploaded": r["date_uploaded"]
            }
            for r in rows
        ],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }


@app.get("/get-insurance-documents")
async def getInsuranceDocuments(
        client_id: str = Query(..., description="Client UUID"),
        size: int = Query(..., gt=0, description="Number of documents to return"),
        last_seen_created_at: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        last_seen_id: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn)
):
    if last_seen_created_at:
        try:
            dt = datetime.fromisoformat(last_seen_created_at)
            cursor_ts = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timestamp: '{last_seen_created_at}'"
            )
    else:
        cursor_ts = datetime.now(timezone.utc)

    max_uuid = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    cursor_id = last_seen_id or max_uuid

    sql = """
        SELECT
          d.id             AS document_id,
          d.file_name      AS file_name,
          d.file_extension AS type,
          d.document_type  AS document_type,
          d.created_at     AS date_uploaded,
          COUNT(*) OVER()  AS total_count
        FROM document d
        WHERE
          d.client_id = $1
          AND d.purpose = 'insurance'
          AND d.is_deleted = FALSE
          AND (d.created_at, d.id) < ($2::timestamptz, $3::uuid)
        ORDER BY d.created_at DESC, d.id DESC
        LIMIT $4;
    """

    rows = await conn.fetch(sql, client_id, cursor_ts, cursor_id, size)

    total = rows[0]["total_count"] if rows else 0

    if rows:
        last = rows[-1]
        next_ts = last["created_at"].isoformat()
        next_id = str(last["id"])
    else:
        next_ts = None
        next_id = None

    return {
        "documents": [
            {
                "document_id": r["document_id"],
                "title": r["file_name"],
                "type": r["type"],
                "document_type": r["document_type"],
                "date_uploaded": r["date_uploaded"]
            }
            for r in rows
        ],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }


@app.get("/fetch-client-projects")
async def fetchClientProjects(
        client_id: str = Query(..., description="Client UUID"),
        size: int = Query(..., gt=0, description="Number of projects to return"),
        last_seen_created_at: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        last_seen_id: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn)
):
    if last_seen_created_at:
        try:
            dt = datetime.fromisoformat(last_seen_created_at)
            cursor_ts = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timestamp: '{last_seen_created_at}'"
            )
    else:
        cursor_ts = datetime.now(timezone.utc)

    max_uuid = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    cursor_id = last_seen_id or max_uuid

    sql = """
        SELECT
          p.id            AS project_id,
          p.business_name AS business_name,
          p.created_at    AS date_created,
          p.due_date      AS due_date,
          s.value         AS status_value,
          COUNT(*) OVER() AS total_count
        FROM project p
        JOIN status s ON s.id = p.status_id AND s.category = 'project' AND s.is_deleted = FALSE
        WHERE
          p.client_id = $1
          AND s.value = 'Open'
          AND p.is_deleted = FALSE
          AND (p.created_at, p.id) < ($2::timestamptz, $3::uuid)
        ORDER BY p.created_at DESC, p.id DESC
        LIMIT $4;
    """

    try:
        rows = await conn.fetch(sql, client_id, cursor_ts, cursor_id, size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    total = rows[0]["total_count"] if rows else 0
    if rows:
        last = rows[-1]
        next_ts = last["created_at"].isoformat()
        next_id = str(last["id"])
    else:
        next_ts = None
        next_id = None

    return {
        "projects": [dict(r) for r in rows],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }


################################################################################
# TODO:                         BILLING ENDPOINTS                              #
################################################################################


@app.get("/get-billings")
async def getBillings(
        size: int = Query(..., gt=0, description="Number of invoices per page"),
        lastSeenCreatedAt: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        lastSeenId: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn)
):
    if lastSeenCreatedAt:
        try:
            dt = datetime.fromisoformat(lastSeenCreatedAt)
            cursorTs = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timestamp: '{lastSeenCreatedAt}'"
            )
    else:
        cursorTs = datetime.now(timezone.utc)

    maxUuid = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    cursorId = lastSeenId or maxUuid

    sql = """
        SELECT
          i.id,
          i.number,
          i.issuance_date,
          i.due_date,
          i.amount,
          s.value        AS status_value,
          d.file_url,
          d.file_name,
          d.file_extension,
          d.document_type,
          i.created_at,
          COUNT(*) OVER() AS total_count
        FROM invoice i
        JOIN status s
          ON s.id = i.status_id
         AND s.category = 'billing'
         AND s.is_deleted = FALSE
        LEFT JOIN document d
          ON d.id = i.file_id
         AND d.is_deleted = FALSE
        WHERE (i.created_at, i.id) < ($1::timestamptz, $2::uuid)
          AND i.is_deleted = FALSE
        ORDER BY i.created_at DESC, i.id DESC
        LIMIT $3;
    """

    try:
        rows = await conn.fetch(sql, cursorTs, cursorId, size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    total = rows[0]["total_count"] if rows else 0

    if rows:
        last = rows[-1]
        nextTs = last["created_at"].isoformat()
        nextId = str(last["id"])
    else:
        nextTs = None
        nextId = None

    return {
        "billings": [dict(r) for r in rows],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": nextTs,
        "last_seen_id": nextId,
    }


@app.get("/get-billing-statuses")
async def getBillingStatuses(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, value, color
            FROM status
            WHERE category = 'billing'
              AND is_deleted = FALSE
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"billing_statuses": [dict(r) for r in rows]}


@app.get("/get-passwords")
async def getPasswords(
        clientId: str = Query(..., description="Client UUID"),
        size: int = Query(..., gt=0, description="Number of passwords per page"),
        lastSeenCreatedAt: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        lastSeenUserId: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn)
):
    if lastSeenCreatedAt:
        try:
            dt = datetime.fromisoformat(lastSeenCreatedAt)
            cursorTs = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timestamp: '{lastSeenCreatedAt}'"
            )
    else:
        cursorTs = datetime.now(timezone.utc)

    maxUuid = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    cursorId = lastSeenUserId or maxUuid

    sql = """
        SELECT
          p.user_id,
          p.encrypted_password,
          p.iv,
          p.salt,
          p.kdf_params,
          p.created_at,
          COUNT(*) OVER() AS total_count
        FROM client_password p
        WHERE p.client_id = $1
          AND (p.created_at, p.user_id) < ($2::timestamptz, $3::uuid)
        ORDER BY p.created_at DESC, p.user_id DESC
        LIMIT $4;
    """

    rows = await conn.fetch(sql, clientId, cursorTs, cursorId, size)

    total = rows[0]["total_count"] if rows else 0
    if rows:
        last = rows[-1]
        nextTs = last["created_at"].isoformat()
        nextId = str(last["user_id"])
    else:
        nextTs = None
        nextId = None

    return {
        "passwords": [dict(r) for r in rows],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": nextTs,
        "last_seen_user_id": nextId,
    }


@app.post("/create-new-invoice")
async def createNewInvoice(
        payload: dict = Body(...),
        conn: Connection = Depends(get_conn)
):
    clientId = payload.get("clientId")
    amount = payload.get("amount")
    dueDate = payload.get("dueDate")
    issuanceDate = payload.get("issuanceDate")
    fileBlob = payload.get("fileBlob", b"")

    if not clientId or not isUUIDv4(clientId):
        raise HTTPException(status_code=400, detail="Invalid clientId")

    docInfo = await uploadDocument(fileBlob)

    async with conn.transaction():
        documentId = await conn.fetchval(
            """
            INSERT INTO document (
              file_name, file_url, file_extension, file_size, document_type, client_id, purpose
            ) VALUES ($1,$2,$3,$4,$5,$6,'invoice') RETURNING id;
            """,
            docInfo["file_name"],
            docInfo["file_url"],
            docInfo["file_extension"],
            docInfo["file_size"],
            docInfo["file_extension"],
            clientId,
        )

        statusId = await conn.fetchval(
            "SELECT id FROM status WHERE category='billing' AND value='Pending' AND is_deleted=FALSE LIMIT 1;"
        )

        invoiceId = await conn.fetchval(
            """
            INSERT INTO invoice (
              due_date, issuance_date, amount, client_id, status_id, file_id
            ) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id;
            """,
            dueDate,
            issuanceDate,
            amount,
            clientId,
            statusId,
            documentId,
        )

    return {"invoiceId": invoiceId}


################################################################################
# TODO:                         QUOTE ENDPOINTS                              #
################################################################################

@app.post("/create-new-quote")
async def createNewQuote(
        payload: dict = Body(...),
        conn: Connection = Depends(get_conn)
):
    projectId = payload.get("projectId")
    clientId = payload.get("clientId")
    amount = payload.get("amount")
    fileBlob = payload.get("fileBlob", b"")

    if not projectId or not isUUIDv4(projectId):
        raise HTTPException(status_code=400, detail="Invalid projectId")
    if not clientId or not isUUIDv4(clientId):
        raise HTTPException(status_code=400, detail="Invalid clientId")

    docInfo = await uploadDocument(fileBlob)

    async with conn.transaction():
        documentId = await conn.fetchval(
            """
            INSERT INTO document (
              file_name, file_url, file_extension, file_size, document_type, project_id, client_id, purpose
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,'quote') RETURNING id;
            """,
            docInfo["file_name"],
            docInfo["file_url"],
            docInfo["file_extension"],
            docInfo["file_size"],
            docInfo["file_extension"],
            projectId,
            clientId,
        )

        statusId = await conn.fetchval(
            "SELECT id FROM status WHERE category='quote' AND value='Pending' AND is_deleted=FALSE LIMIT 1;"
        )

        quoteId = await conn.fetchval(
            """
            INSERT INTO quote (
              amount, project_id, client_id, status_id, file_id
            ) VALUES ($1,$2,$3,$4,$5) RETURNING id;
            """,
            amount,
            projectId,
            clientId,
            statusId,
            documentId,
        )

    return {"quoteId": quoteId}


@app.get("/get-invoice")
async def getInvoice(
        id: str = Query(..., description="Invoice UUID"),
        conn: Connection = Depends(get_conn)
):
    sql = """
        SELECT i.*, d.file_url, d.document_type
          FROM invoice i
          LEFT JOIN document d ON d.id = i.file_id AND d.is_deleted = FALSE
         WHERE i.id = $1 AND i.is_deleted = FALSE
         LIMIT 1;
    """
    row = await conn.fetchrow(sql, id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Invoice {id} not found")
    return {"invoice": dict(row)}


@app.get("/get-quote")
async def getQuote(
        id: str = Query(..., description="Quote UUID"),
        conn: Connection = Depends(get_conn)
):
    sql = """
        SELECT q.*, d.file_url, d.document_type
          FROM quote q
          LEFT JOIN document d ON d.id = q.file_id AND d.is_deleted = FALSE
         WHERE q.id = $1 AND q.is_deleted = FALSE
         LIMIT 1;
    """
    row = await conn.fetchrow(sql, id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Quote {id} not found")
    return {"quote": dict(row)}


################################################################################
# TODO:                         PROFILE ENDPOINTS                              #
################################################################################

@app.get("/get-onboarding-data")
async def getOnboardingData(
        clientId: str = Query(..., description="Client UUID"),
        conn: Connection = Depends(get_conn)
):
    if not clientId or not isUUIDv4(clientId):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid UUIDv4 (must be lowercase-hyphenated): {clientId}"
        )

    generalSql = """
        SELECT satellite_office_address, organization_type, establishment_year,
               annual_revenue, accepted_payment_methods, naics_code, duns_number
          FROM client_onboarding_general
         WHERE client_id = $1 AND is_deleted = FALSE
         LIMIT 1;
    """
    serviceSql = """
        SELECT coverage_area, admin_staff_count, field_staff_count, licenses,
               working_hours, covers_after_hours, covers_weekend_calls
          FROM client_onboarding_service
         WHERE client_id = $1 AND is_deleted = FALSE
         LIMIT 1;
    """
    contactSql = """
        SELECT dispatch_supervisor, field_supervisor, management_supervisor,
               regular_hours_contact, emergency_hours_contact
          FROM client_onboarding_contact
         WHERE client_id = $1 AND is_deleted = FALSE
         LIMIT 1;
    """
    loadSql = """
        SELECT avg_monthly_tickets_last4, po_source_split, monthly_po_capacity
          FROM client_onboarding_load
         WHERE client_id = $1 AND is_deleted = FALSE
         LIMIT 1;
    """
    tradeSql = """
        SELECT pt.value AS trade, tc.coverage_level
          FROM client_trade_coverage tc
          JOIN project_trade pt ON pt.id = tc.project_trade_id
         WHERE tc.client_id = $1
           AND tc.is_deleted = FALSE
           AND pt.is_deleted = FALSE;
    """
    pricingSql = """
        SELECT item_label, regular_hours_rate, after_hours_rate, is_custom
          FROM client_pricing_structure
         WHERE client_id = $1 AND is_deleted = FALSE;
    """
    refsSql = """
        SELECT company_name, contact_name, contact_email, contact_phone
          FROM client_references
         WHERE client_id = $1 AND is_deleted = FALSE;
    """

    try:
        generalRow = await conn.fetchrow(generalSql, clientId)
        serviceRow = await conn.fetchrow(serviceSql, clientId)
        contactRow = await conn.fetchrow(contactSql, clientId)
        loadRow = await conn.fetchrow(loadSql, clientId)
        tradeRows = await conn.fetch(tradeSql, clientId)
        pricingRows = await conn.fetch(pricingSql, clientId)
        refsRows = await conn.fetch(refsSql, clientId)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "general": dict(generalRow) if generalRow else {},
        "service": dict(serviceRow) if serviceRow else {},
        "contact": dict(contactRow) if contactRow else {},
        "load": dict(loadRow) if loadRow else {},
        "tradeCoverage": [dict(r) for r in tradeRows],
        "pricing": [dict(r) for r in pricingRows],
        "references": [dict(r) for r in refsRows],
    }


################################################################################
# TODO:                         ONBOARDING ENDPOINTS                           #
################################################################################


@app.post("/save-onboarding-data")
async def saveOnboardingData(
        payload: dict = Body(...),
        conn: Connection = Depends(get_conn)
):
    clientId = payload.get("clientId")

    if not clientId or not isUUIDv4(clientId):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid UUIDv4 (must be lowercase-hyphenated): {clientId}"
        )

    general = payload.get("general", {})
    service = payload.get("service", {})
    contact = payload.get("contact", {})
    loadInfo = payload.get("load", {})
    tradeCoverage = payload.get("tradeCoverage", [])
    pricing = payload.get("pricing", [])
    references = payload.get("references", [])

    async with conn.transaction():
        await conn.execute(
            """
            INSERT INTO client_onboarding_general (
              client_id, satellite_office_address, organization_type,
              establishment_year, annual_revenue, accepted_payment_methods,
              naics_code, duns_number
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8);
            """,
            clientId,
            general.get("satelliteOfficeAddress"),
            general.get("organizationType"),
            general.get("establishmentYear"),
            general.get("annualRevenue"),
            general.get("paymentMethods"),
            general.get("naicsCode"),
            general.get("dunsNumber"),
        )

        await conn.execute(
            """
            INSERT INTO client_onboarding_service (
              client_id, coverage_area, admin_staff_count, field_staff_count,
              licenses, working_hours, covers_after_hours, covers_weekend_calls
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8);
            """,
            clientId,
            service.get("coverageArea"),
            service.get("adminStaffCount"),
            service.get("fieldStaffCount"),
            service.get("licenses"),
            service.get("workingHours"),
            bool(service.get("coversAfterHours")),
            bool(service.get("coversWeekendCalls")),
        )

        await conn.execute(
            """
            INSERT INTO client_onboarding_contact (
              client_id, dispatch_supervisor, field_supervisor, management_supervisor,
              regular_hours_contact, emergency_hours_contact
            ) VALUES ($1,$2,$3,$4,$5,$6);
            """,
            clientId,
            contact.get("dispatchSupervisor"),
            contact.get("fieldSupervisor"),
            contact.get("managementSupervisor"),
            contact.get("regularContact"),
            contact.get("emergencyContact"),
        )

        await conn.execute(
            """
            INSERT INTO client_onboarding_load (
              client_id, avg_monthly_tickets_last4, po_source_split, monthly_po_capacity
            ) VALUES ($1,$2,$3,$4);
            """,
            clientId,
            loadInfo.get("averageMonthlyTickets"),
            loadInfo.get("poSourceSplit"),
            loadInfo.get("monthlyPOCapacity"),
        )

        for tc in tradeCoverage:
            tradeValue = tc.get("trade")
            coverageLevel = tc.get("coverageLevel")
            if not tradeValue or not coverageLevel:
                continue
            tradeId = await conn.fetchval(
                "SELECT id FROM project_trade WHERE value=$1 AND is_deleted=FALSE LIMIT 1;",
                tradeValue,
            )
            if tradeId:
                await conn.execute(
                    """
                    INSERT INTO client_trade_coverage (
                      client_id, project_trade_id, coverage_level
                    ) VALUES ($1,$2,$3);
                    """,
                    clientId,
                    tradeId,
                    coverageLevel.upper(),
                )

        for price in pricing:
            if not price.get("label"):
                continue
            await conn.execute(
                """
                INSERT INTO client_pricing_structure (
                  client_id, item_label, regular_hours_rate, after_hours_rate, is_custom
                ) VALUES ($1,$2,$3,$4,$5);
                """,
                clientId,
                price.get("label"),
                price.get("regular"),
                price.get("after"),
                bool(price.get("isCustom")),
            )

        for ref in references:
            if not any(ref.values()):
                continue
            await conn.execute(
                """
                INSERT INTO client_references (
                  client_id, company_name, contact_name, contact_email, contact_phone
                ) VALUES ($1,$2,$3,$4,$5);
                """,
                clientId,
                ref.get("company"),
                ref.get("contact"),
                ref.get("email"),
                ref.get("phone"),
            )

    return {"status": "success"}


@app.post("/update-insurance-data")
async def updateInsuranceData(
        payload: dict = Body(...),
        conn: Connection = Depends(get_conn)
):
    clientId = payload.get("clientId")
    provider = payload.get("provider")
    policyNumber = payload.get("policyNumber")
    coverageAmount = payload.get("coverageAmount")
    startDate = payload.get("startDate")
    endDate = payload.get("endDate")

    if not clientId or not isUUIDv4(clientId):
        raise HTTPException(status_code=400, detail="Invalid clientId")

    insuranceId = await conn.fetchval(
        """
        INSERT INTO insurance (
          client_id, provider, policy_number, coverage_amount, start_date, end_date
        ) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id;
        """,
        clientId,
        provider,
        policyNumber,
        coverageAmount,
        startDate,
        endDate,
    )

    return {"insuranceId": insuranceId}


@app.post("/auth/invite")
async def inviteUser(
        request: Request,
        payload: dict = Body(),
        conn: Connection = Depends(get_conn)
):
    email_to_invite = payload.get("emailToInvite")
    account_type = payload.get("accountType")
    first_name = payload.get("firstName", "")
    last_name = payload.get("lastName", "")

    if not email_to_invite:
        raise HTTPException(status_code=400, detail="Invalid email")
    if not account_type:
        raise HTTPException(status_code=400, detail="Invalid account type")

    hex_color = f"#{random.randint(0, 0xFFFFFF):06x}"
    await conn.fetchrow(
        "INSERT INTO \"user\" (email, first_name, last_name, hex_color, is_active, is_client) "
        "VALUES ($1,$2,$3,$4,FALSE,$5) RETURNING id",
        email_to_invite,
        first_name,
        last_name,
        hex_color,
        account_type.startswith("Client")
    )

    await createMagicLink(
        conn,
        request.app.state.privateKey,
        purpose="signup",
        recipientEmail=email_to_invite,
        ttlHours=24,
    )

    await conn.execute(
        "INSERT INTO casbin_rule (ptype, subject, domain, object, action) VALUES ('g',$1,$2,'*','')",
        email_to_invite,
        account_type.replace(' ', '_').lower()
    )

    return {"status": "link sent"}


@app.put("/update-user")
async def updateUser(payload: dict = Body(), conn: Connection = Depends(get_conn)):
    user_id = payload.get("userId")
    if not user_id or not await isUUIDv4(user_id):
        raise HTTPException(status_code=400, detail="Invalid userId")

    email = payload.get("email")
    first_name = payload.get("firstName")
    last_name = payload.get("lastName")
    role = payload.get("role")

    updates = []
    params = []
    if email:
        updates.append(f"email=${len(params)+1}")
        params.append(email)
    if first_name:
        updates.append(f"first_name=${len(params)+1}")
        params.append(first_name)
    if last_name:
        updates.append(f"last_name=${len(params)+1}")
        params.append(last_name)

    if updates:
        await conn.execute(
            f"UPDATE \"user\" SET {', '.join(updates)} WHERE id=${len(params)+1}",
            *params,
            UUID(user_id),
        )

    if role:
        subj = email if email else await conn.fetchval('SELECT email FROM "user" WHERE id=$1', UUID(user_id))
        await conn.execute(
            "DELETE FROM casbin_rule WHERE ptype='g' AND subject=$1",
            subj,
        )
        await conn.execute(
            "INSERT INTO casbin_rule (ptype, subject, domain, object, action) VALUES ('g',$1,$2,'*','')",
            subj,
            role.replace(' ', '_').lower(),
        )

    return {"status": "updated"}


@app.post("/auth/login")
async def login(request: Request, data: dict = Depends(decryptPayload()), conn: Connection = Depends(get_conn),
                enforcer: SyncedEnforcer = Depends(getEnforcer)):
    email = data.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="Email required")
    row = await conn.fetchrow(
        "SELECT id, is_blacklisted FROM \"user\" WHERE email=$1 AND is_deleted=FALSE",
        email,
    )
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    if row["is_blacklisted"]:
        raise HTTPException(status_code=403, detail="blacklisted")
    userId = row["id"]
    token = await createMagicLink(
        conn,
        userId,
        request.app.state.privateKey,
        purpose="login",
        recipientEmail=email,
        ttlHours=1,
    )

    link = f"/set-recovery-phrase?token={token}"
    await conn.execute(
        "INSERT INTO notification (triggered_by_category, triggered_by_id, content) VALUES ($1,$2,$3)",
        "auth",
        userId,
        link,
    )

    return {"status": "link sent"}



@app.post("/auth/set-recovery-phrase")
async def setRecoveryPhrase(request: Request, data: dict = Depends(decryptPayload(True)),
                            conn: Connection = Depends(get_conn)):
    token_str = data.get("token")
    if not token_str:
        raise HTTPException(status_code=400, detail="missing token")
    try:
        token_payload = decodeJwtRs256(token_str, request.app.state.publicKey)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid token")

    link_uuid = token_payload.get("uuid")
    if not link_uuid or not await isUUIDv4(link_uuid):
        raise HTTPException(status_code=400, detail="invalid token")

    row = await conn.fetchrow(
        "SELECT consumed, expires_at FROM magic_link WHERE uuid=$1",
        UUID(link_uuid),
    )
    if not row or row["consumed"] or row["expires_at"] < datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="invalid token")

    # When called with encrypted data, finalize recovery setup
    userEmail = data.get("userEmail")
    if userEmail:
        digest_b64 = data.get("digest")
        salt_b64 = data.get("salt")
        params_b64 = data.get("kdfParams")
        iv = data.get("_nonce")
        clientPub = data.get("_client_pub")
        if not userEmail:
            raise HTTPException(status_code=400, detail="Missing user's email")
        if not digest_b64:
            raise HTTPException(status_code=400, detail="Missing digest")

        salt = base64.b64decode(salt_b64)
        params = json.loads(base64.b64decode(params_b64).decode())
        digest = base64.b64decode(digest_b64)

        await conn.execute(
            """
            INSERT INTO user_recovery_params (user_email, iv, salt, kdf_params, digest)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (user_email)
            DO UPDATE SET iv=EXCLUDED.iv, salt=EXCLUDED.salt,
              kdf_params=EXCLUDED.kdf_params, digest=EXCLUDED.digest, updated_at=now()
            """,
            userEmail,
            iv,
            salt,
            json.dumps(params),
            digest,
        )


        await conn.execute(
            "INSERT INTO user_key (user_email, purpose, public_key) VALUES ($1,'sig',$2) ON CONFLICT (user_email, purpose) DO UPDATE SET public_key=EXCLUDED.public_key",
            userEmail,
            clientPub,
        )

        await conn.execute(
            "UPDATE \"user\" SET is_active=TRUE, has_set_recovery_phrase=TRUE WHERE email=$1",
            userEmail,
        )


        jti = str(uuid4())
        exp_dt = datetime.now(timezone.utc) + timedelta(minutes=5)
        await conn.execute(
            "INSERT INTO jwt_token (jti, user_email, expires_at, revoked) VALUES ($1,$2,$3,FALSE)",
            jti,
            userEmail,
            exp_dt,
        )
        redis = request.app.state.redis
        await redis.sadd("active_jtis", jti)
        await redis.publish("jwt_updates", f"add:{jti}")
        await conn.execute("UPDATE magic_link SET consumed=TRUE WHERE uuid=$1", UUID(link_uuid))
        next_payload = {
            "next_step": "/onboarding",
            "exp": int((datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()),
        }
        nav_token = generateJwtRs256(next_payload, request.app.state.privateKey)
        session_token = jwt.encode({"sub": userEmail, "jti": jti, "exp": exp_dt}, SECRET_KEY, algorithm="HS256")
        payload = {"token": nav_token}
        if clientPub is not None:
            payload = encryptForClient(payload, clientPub, request.app)
        response = JSONResponse(payload)
        response.set_cookie("session", session_token, httponly=True, secure=True)
        return response



@app.get("/auth/get-recovery-params")
async def getRecoveryParams(email: str, conn: Connection = Depends(get_conn)):
    row = await conn.fetchrow(
        """
        SELECT u.id, p.salt, p.kdf_params
          FROM "user" u
          JOIN user_recovery_params p ON p.user_email = u.email
         WHERE u.email=$1 AND u.is_deleted=FALSE
        """,
        email,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Not found")
    return {
        "userId": str(row["id"]),
        "salt": base64.b64encode(row["salt"]).decode(),
        "kdfParams": base64.b64encode(row["kdf_params"].encode()).decode(),
    }


@app.post("/auth/update-client-key")
async def updateClientKey(request: Request, data: dict = Depends(decryptPayload()),
                          conn: Connection = Depends(get_conn)):
    clientPub = data.get("_client_pub")
    iv = data.get("_nonce")
    userEmail = data.get("userEmail")
    digest_b64 = data.get("digest")
    if not userEmail:
        raise HTTPException(status_code=400, detail="Missing user's email")
    if not digest_b64:
        raise HTTPException(status_code=400, detail="Missing digest")

    digest = base64.b64decode(digest_b64)

    row = await conn.fetchrow(
        "SELECT digest FROM user_recovery_params WHERE user_email=$1",
        userEmail,
    )
    if not row or row["digest"] != digest:
        raise HTTPException(status_code=403, detail="Invalid recovery phrase")

    await conn.execute(
        "INSERT INTO user_key (user_email, purpose, public_key) VALUES ($1,'sig',$2) ON CONFLICT (user_email, purpose) DO UPDATE SET public_key=EXCLUDED.public_key",
        userEmail,
        clientPub,
    )
    return {"status": "ok"}


@app.post("/auth/complete-onboarding")
async def completeOnboarding(payload: dict = Body(), conn: Connection = Depends(get_conn),
                             enforcer: SyncedEnforcer = Depends(getEnforcer)):
    userId = payload.get("userId")
    if not userId or not isUUIDv4(userId):
        raise HTTPException(status_code=400, detail="Invalid userId")
    await conn.execute(
        "UPDATE \"user\" SET onboarding_done=TRUE WHERE id=$1",
        userId,
    )
    return {"status": "ok"}


@app.post("/auth/revoke")
async def revokeToken(request: Request, conn: Connection = Depends(get_conn)):
    token = request.cookies.get("session")
    if not token:
        raise HTTPException(status_code=401, detail="no token")
    try:
        data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except Exception:
        raise HTTPException(status_code=401, detail="invalid")
    jti = data.get("jti")
    if not jti:
        raise HTTPException(status_code=400, detail="bad token")
    await conn.execute("UPDATE jwt_token SET revoked=TRUE WHERE jti=$1", jti)
    redis = request.app.state.redis
    await redis.srem("active_jtis", jti)
    await redis.publish("jwt_updates", f"remove:{jti}")
    resp = JSONResponse({"status": "revoked"})
    resp.delete_cookie("session")
    return resp


@app.post("/admin/blacklist")
async def blacklistUser(email: str = Body(..., embed=True), request: Request = None,
                        conn: Connection = Depends(get_conn)):
    await conn.execute("UPDATE \"user\" SET is_blacklisted=TRUE WHERE email=$1", email)
    redis = request.app.state.redis
    await redis.sadd("blacklisted_users", email)
    await redis.publish("blacklist_updates", f"add:{email}")
    return {"status": "ok"}


@app.post("/admin/unblacklist")
async def unblacklistUser(email: str = Body(..., embed=True), request: Request = None,
                          conn: Connection = Depends(get_conn)):
    await conn.execute("UPDATE \"user\" SET is_blacklisted=FALSE WHERE email=$1", email)
    redis = request.app.state.redis
    await redis.srem("blacklisted_users", email)
    await redis.publish("blacklist_updates", f"remove:{email}")
    return {"status": "ok"}


@app.get("/connection-test")
async def connectionTest():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
