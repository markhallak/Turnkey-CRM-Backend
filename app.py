import asyncio
import base64
import json
import os
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import httpx
import jwt
import nacl.bindings
import uvicorn
from asyncpg import create_pool, Pool, Connection
from casbin import SyncedEnforcer, AsyncEnforcer
from casbin_async_sqlalchemy_adapter import Adapter
from casbin_redis_watcher import new_watcher, WatcherOptions
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from databases import Database
from fastapi import FastAPI, HTTPException, Query, Body, Request, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import declarative_base

from constants import ASYNCPG_URL, SECRET_KEY, REDIS_URL, KMS_URL
from util import isUUIDv4, createMagicLink, generateJwtRs256, decodeJwtRs256


class SimpleUser:
    def __init__(self, id: UUID, email: str, first_name: str, last_name: str, setup_done: bool, onboarding_done: bool, is_client: bool):
        self.id = id
        self.email = email
        self.firstName = first_name
        self.lastName = last_name
        self.setup_done = setup_done
        self.onboarding_done = onboarding_done
        self.is_client = is_client


async def getCurrentUser(request: Request) -> SimpleUser | None:
    token = request.cookies.get("session")

    if not token:
        return None
    try:
        data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except Exception as e:
        print("EXCEPTION: ", e)
        return None

    jti = data.get("jti")
    email = data.get("sub")
    if not jti or not email:
        return None
    redis = request.app.state.redis
    if not await redis.sismember("active_jtis", jti):
        return None
    if await redis.sismember("blacklisted_users", email):
        return None

    async with request.app.state.db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, first_name, last_name, has_set_recovery_phrase, onboarding_done, is_client FROM \"user\" WHERE email=$1",
            email,
        )
        if row:
            return SimpleUser(
                row["id"],
                email,
                row["first_name"],
                row["last_name"],
                row["has_set_recovery_phrase"],
                row["onboarding_done"],
                row["is_client"],
            )
    return None


def getEnforcer(request: Request) -> SyncedEnforcer:
    return request.app.state.enforcer


BYPASS_SESSION = True


async def authorize(request: Request, user: SimpleUser = Depends(getCurrentUser),
                    enforcer: SyncedEnforcer = Depends(getEnforcer)):
    path = request.url.path

    if (
            not BYPASS_SESSION
            and not path.startswith("/auth/ed25519")
            and not path.startswith("/auth/public-key")
            and not path.startswith("/auth/login")
            and not path.startswith("/auth/validate-signup-token")
            and not path.startswith("/auth/validate-login-token")
            and not path.startswith("/auth/me")
            and not path.startswith("/auth/set-recovery-phrase")
            and not path.startswith("/auth/refresh-session")
            and not path.startswith("/connection-test")
    ):
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthenticated")

        token = request.cookies.get("session")
        if token:
            try:
                data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"], options={"verify_exp": False})
                jti = data.get("jti")
                if jti and await request.app.state.redis.sismember("blacklisted_jtis", jti):
                    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="revoked")
            except Exception:
                pass

        if not user.setup_done and path != "/set-recovery-phrase":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="set-recovery-phrase")

        if user.setup_done and not user.onboarding_done and path != "/onboarding":
            if user.is_client:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="onboarding")

        perms = await enforcer.get_policy()  # [[sub, dom, obj, act], …]

        # 2. every “g” (role-mapping) rule it knows
        groups = await enforcer.get_grouping_policy()  # [[sub, role, dom], …]

        # 3. the roles this particular user has in the “*” domain
        roles = await enforcer.get_roles_for_user_in_domain(user.email, "*")

        # 4. all permissions this user already enjoys
        user_perms = await enforcer.get_permissions_for_user(user.email)
        print("PERMS: ", perms)
        print("GROUPS: ", groups)
        print("ROLES: ", roles)
        print("USER PERMS: ", user_perms)

        sub = str(user.email)
        domain = "*"
        obj = path
        act = request.method.lower()

        if not enforcer.enforce(sub, domain, obj, act):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    return True


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

    def defaultEncoder(o):
        if isinstance(o, Decimal):
            return float(o)
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

    cipher = aes.encrypt(iv, json.dumps(data, default=defaultEncoder).encode(), None)
    return {
        "nonce": base64.b64encode(iv).decode(),
        "ciphertext": base64.b64encode(cipher).decode(),
    }


async def encryptForUser(data: dict, email: str, conn: Connection, app: FastAPI) -> dict:
    row = await conn.fetchrow(
        "SELECT public_key FROM user_key WHERE user_email=$1 AND purpose='sig'",
        email,
    )
    if not row:
        return data
    return encryptForClient(data, row["public_key"], app)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async_url = ASYNCPG_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(
        async_url,
        pool_size=5,
    )

    Base = declarative_base()

    class CasbinRule(Base):
        __tablename__ = "casbin_rule"  # ← exactly the table already in Postgres
        id = Column(Integer, primary_key=True)
        ptype = Column(String(100), nullable=False)
        v0 = Column(String(255))
        v1 = Column(String(255))
        v2 = Column(String(255))
        v3 = Column(String(255))
        v4 = Column(String(255))
        v5 = Column(String(255))

    adapter = Adapter(engine, db_class=CasbinRule)
    enforcer = AsyncEnforcer("model.conf", adapter)
    enforcer.enable_auto_save(True)
    await enforcer.load_policy()
    enforcer.build_role_links()

    opts = WatcherOptions()
    opts.host = "localhost"
    opts.port = "6379"
    watcher = new_watcher(opts)
    loop = asyncio.get_running_loop()
    watcher.set_update_callback(lambda _: asyncio.run_coroutine_threadsafe(
        enforcer.load_policy(), loop
    ))
    enforcer.set_watcher(watcher)

    app.state.casbin_watcher = watcher
    app.state.enforcer = enforcer

    app.state.db_pool: Pool = await create_pool(
        dsn=ASYNCPG_URL, min_size=5, max_size=20
    )
    print("DB pool created")

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    app.state.redis = redis
    async with app.state.db_pool.acquire() as c:
        rows = await c.fetch("SELECT jti FROM jwt_token WHERE revoked=FALSE AND expires_at>NOW()")
        if rows:
            await redis.sadd("active_jtis", *[str(r["jti"]) for r in rows])
        rows = await c.fetch("SELECT email FROM \"user\" WHERE is_blacklisted=TRUE")
        if rows:
            await redis.sadd("blacklisted_users", *[str(r["email"]) for r in rows])

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
    allow_origins=origins,  # <-- your React app origin
    allow_credentials=True,  # <-- if you send cookies or Authorization headers
    allow_methods=["*"],  # <-- GET, POST, PUT, DELETE, OPTIONS, etc
    allow_headers=["*"],  # <-- allow all request headers
)


def get_db_pool(request: Request) -> Pool:
    return request.app.state.db_pool


async def get_conn(db_pool: Pool = Depends(get_db_pool)):
    async with db_pool.acquire() as conn:
        yield conn


@app.post("/auth/public-key")
async def getPublicKeyEndpoint(request: Request):
    return {"public_key": request.app.state.publicKey}


@app.post("/auth/ed25519-public-key")
async def getEd25519PublicKey(request: Request):
    return {"public_key": request.app.state.ed25519PublicKey}


@app.get("/auth/me")
async def whoami(user: SimpleUser = Depends(getCurrentUser)):
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="unauthenticated",
        )

    return {
        "email": user.email,
        "firstName": user.firstName,
        "lastName": user.lastName,
        "setup_done": user.setup_done,
        "onboarding_done": user.onboarding_done,
        "is_client": user.is_client
    }


@app.post("/auth/validate-signup-token")
async def validateSignupToken(request: Request, data: dict = Depends(decryptPayload()),
                              conn: Connection = Depends(get_conn)):
    token = data.get("token")

    try:
        payload = decodeJwtRs256(token, request.app.state.publicKey)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid token")

    uuid_str = payload.get("uuid")
    if not uuid_str or not await isUUIDv4(uuid_str):
        raise HTTPException(status_code=400, detail="invalid token uuid")

    row = await conn.fetchrow(
        "SELECT consumed, expires_at FROM magic_link WHERE uuid=$1",
        UUID(uuid_str),
    )
    if (
            not row
            or row["consumed"]
            or row["expires_at"] < datetime.now(timezone.utc)
    ):
        raise HTTPException(status_code=400, detail="invalid sql data")

    return {
        "userEmail": payload.get("userEmail"),
        "firstName": payload.get("firstName"),
        "lastName": payload.get("lastName"),
        "accountType": payload.get("accountType"),
    }


@app.post("/auth/validate-login-token")
async def validateLoginToken(
        request: Request,
        data: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn),
):
    token_str = data.get("token")
    try:
        payload = decodeJwtRs256(token_str, request.app.state.publicKey)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid token")

    link_uuid = payload.get("uuid")
    if not link_uuid or not await isUUIDv4(link_uuid):
        raise HTTPException(status_code=400, detail="invalid token")

    row = await conn.fetchrow(
        "SELECT consumed, expires_at, user_email FROM magic_link WHERE uuid=$1",
        UUID(link_uuid),
    )
    if not row or row["consumed"] or row["expires_at"] < datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="invalid token")

    userEmail = row["user_email"]
    jti = str(uuid4())
    exp_dt = datetime.now(timezone.utc) + timedelta(minutes=60)
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
        "next_step": "/dashboard",
        "exp": int((datetime.now(timezone.utc) + timedelta(minutes=(60 * 24))).timestamp()),
    }
    nav_token = generateJwtRs256(next_payload, request.app.state.privateKey)
    session_token = jwt.encode({"sub": userEmail, "jti": jti, "exp": exp_dt}, SECRET_KEY, algorithm="HS256")
    response = JSONResponse({"token": nav_token})
    response.set_cookie(
        "session",
        session_token,
        httponly=True,
        secure=True,
        samesite="none",
        domain="localhost",
        path="/",
        max_age=60 * 60 * 24,
    )

    return response


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

@app.post("/global-search")
async def globalSearch(
        request: Request,
        data: dict = Depends(decryptPayload()),
        q: str | None = Query(None, description="Search query string"),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
):
    term = data.get("q") or q
    if not term:
        raise HTTPException(status_code=400, detail="Missing search term")
    globalSearchSql = """
        SELECT source_table, record_id, search_text, is_deleted
          FROM global_search
         WHERE  length($1::text) >= 3
           AND search_text ILIKE '%' || $1 || '%'
         ORDER BY source_table, record_id
         LIMIT 10;
    """

    try:
        results = await conn.fetch(globalSearchSql, term)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    payload = {"results": [dict(r) for r in results]}
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-account-manager-client-relations")
async def getAccountManagerClientRelations(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT amc.account_manager_email, amc.client_id, c.company_name
              FROM account_manager_client amc
              JOIN client c ON c.id = amc.client_id 
             ORDER BY amc.account_manager_email;
        """
    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"relations": [dict(r) for r in rows]}


@app.post("/create-account-manager-client-relation")
async def createAccountManagerClientRelation(payload: dict = Body(), conn: Connection = Depends(get_conn)):
    email = payload.get("account_manager_email")
    clientId = payload.get("client_id")
    if not email or not clientId:
        raise HTTPException(status_code=400, detail="Invalid data")
    sql = "INSERT INTO account_manager_client (account_manager_email, client_id) VALUES ($1,$2) ON CONFLICT DO NOTHING;"
    try:
        await conn.execute(sql, email, UUID(clientId))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "created"}


@app.post("/delete-account-manager-client-relation")
async def deleteAccountManagerClientRelation(payload: dict = Body(), conn: Connection = Depends(get_conn)):
    email = payload.get("account_manager_email")
    clientId = payload.get("client_id")
    if not email or not clientId:
        raise HTTPException(status_code=400, detail="Invalid data")
    sql = "DELETE FROM account_manager_client WHERE account_manager_email=$1 AND client_id=$2;"
    try:
        await conn.execute(sql, email, UUID(clientId))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "deleted"}


@app.post("/send-message")
async def sendMessage(
        request: Request,
        data: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
):
    project_id = data.get("projectId")
    content = data.get("content")
    if not project_id or not content:
        raise HTTPException(status_code=400, detail="invalid params")
    try:
        msg_id = await conn.fetchval(
            "INSERT INTO message (project_id, sender_id, content) VALUES ($1,$2,$3) RETURNING id",
            UUID(project_id), user.id, content
        )
        created_at = await conn.fetchval(
            "SELECT created_at FROM message WHERE id=$1", msg_id
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    payload = {"messageId": str(msg_id), "created_at": created_at.isoformat()}
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-notifications")
async def getNotifications(
        request: Request,
        data: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
):
    last_seen_created_at = data.get("last_seen_created_at")
    last_seen_id = data.get("last_seen_id")
    size = data.get("size", 10)

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

    payload = {
        "notifications": [dict(r) for r in rows],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-profile-details")
async def getProfileDetails(
        request: Request,
        data: dict = Depends(decryptPayload()),
        user: SimpleUser = Depends(getCurrentUser),
        conn: Connection = Depends(get_conn)
):
    email = user.email if user else None
    if not email:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthenticated")

    sql = """
            SELECT first_name, hex_color
            FROM "user"
            WHERE email = $1
              
            LIMIT 1;
        """

    row = await conn.fetchrow(sql, email)
    if not row:
        raise HTTPException(status_code=404, detail=f"User {email} not found")

    payload = {
        "first_name": row["first_name"],
        "hex_color": row["hex_color"],
    }
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/project-assessments/{project_id}")
async def getProjectAssessments(
        request: Request,
        project_id: str,
        db_pool: Pool = Depends(get_db_pool),
        user: SimpleUser = Depends(getCurrentUser)
):
    if not await isUUIDv4(project_id):
        raise HTTPException(status_code=400, detail="Invalid project id")

    sql = """
    SELECT
      p.id                     AS project_id,
      p.visit_notes,
      p.planned_resolution,
      p.material_parts_needed
    FROM project p
    WHERE p.id          = $1
    """

    async with db_pool.acquire() as conn:
        rec = await conn.fetchrow(sql, UUID(project_id))
    if not rec:
        raise HTTPException(status_code=404, detail="Not found")
    payload = dict(rec)
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


################################################################################
# TODO:                         DASHBOARD ENDPOINTS                            #
################################################################################

@app.post("/get-dashboard-metrics")
async def getDashboardMetrics(
        request: Request,
        data: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)):
    sql = "SELECT * FROM overall_aggregates;"

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    payload = {"metrics": [dict(r) for r in rows]}
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-calendar-events")
async def getCalendarEvents(
        request: Request,
        data: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
):
    month = data["month"]

    if month < 1 or month > 12:
        raise Exception()

    sql = """
        SELECT
          p.*,
          COALESCE(p.scheduled_date, p.due_date) AS event_date
        FROM project p
        WHERE  (
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

    payload = {"events": events}
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


################################################################################
# TODO:                         PROJECTS PAGE ENDPOINTS                        #
################################################################################


@app.post("/get-projects")
async def getProjects(
        request: Request,
        data: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
):
    size = data.get("size")
    last_seen_created_at = data.get("last_seen_created_at")
    last_seen_id = data.get("last_seen_id")
    if not size:
        raise HTTPException(status_code=400, detail="size required")
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
      (p.created_at, p.id) < ($1::timestamptz, $2::uuid)
      
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

    payload = {
        "projects": [dict(r) for r in rows],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-project-statuses")
async def getProjectStatuses(
        request: Request,
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)):
    sql = """
            SELECT id, value, color
            FROM status
            WHERE category = 'project'
              
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    payload = {"project_statuses": [dict(r) for r in rows]}
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-project-types")
async def getProjectTypes(
        request: Request,
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)):
    sql = """
            SELECT id, value
            FROM project_type
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    payload = {"project_types": [dict(r) for r in rows]}
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-project-trades")
async def getProjectTrades(
        request: Request,
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)):
    sql = """
            SELECT id, value
            FROM project_trade
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    payload = {"project_trades": [dict(r) for r in rows]}
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-project-priorities")
async def getProjectPriorities(
        request: Request,
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)):
    sql = """
            SELECT id, value, color
            FROM project_priority
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    payload = {"project_priorities": [dict(r) for r in rows]}
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-all-client-admins")
async def getAllClientAdmins(
        request: Request,
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)):
    sql = """
            SELECT u.id, u.email, c.company_name
              FROM "user" u
              JOIN casbin_rule r
                ON r.ptype = 'g'
               AND r.subject = u.email
               AND r.domain = 'client_admin'
              JOIN client c ON c.id = u.client_id
               
             ORDER BY c.company_name;
        """
    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    payload = {"client_admins": [dict(r) for r in rows]}
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-account-managers")
async def getAccountManagers(
        request: Request,
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)):
    sql = """
            SELECT u.id, u.email, u.first_name, u.last_name
              FROM "user" u
              JOIN casbin_rule r
                ON r.ptype = 'g'
               AND r.subject = u.email
               AND r.domain = 'employee_account_manager'
             WHERE  u.is_active = TRUE
             ORDER BY u.first_name;
        """
    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    payload = {"account_managers": [dict(r) for r in rows]}
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-states")
async def getStates(
        request: Request,
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)):
    sql = """
            SELECT id, name
              FROM state
             ORDER BY name;
        """
    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    payload = {"states": [dict(r) for r in rows]}
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


# List all users for admin page
@app.post("/get-users")
async def getUsers(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, email, first_name, last_name
              FROM "user"
             ORDER BY first_name;
        """
    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"users": [dict(r) for r in rows]}


@app.post("/create-new-project")
async def createNewProject(
        request: Request,
        payload: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
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
            "SELECT id FROM project_priority WHERE value=$1  LIMIT 1;",
            priorityValue,
        )
        tradeId = await conn.fetchval(
            "SELECT id FROM project_trade WHERE value=$1  LIMIT 1;",
            tradeValue,
        )
        stateId = await conn.fetchval(
            "SELECT id FROM state WHERE name=$1  LIMIT 1;",
            stateName,
        )
        typeId = await conn.fetchval(
            "SELECT id FROM project_type WHERE LIMIT 1;"
        )
        statusId = await conn.fetchval(
            "SELECT id FROM status WHERE category='project' AND value='Open'  LIMIT 1;"
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

    payloadRes = {"projectId": projectId}
    if user:
        payloadRes = await encryptForUser(payloadRes, user.email, conn, request.app)
    return payloadRes


################################################################################
# TODO:                        PROJECT VIEW ENDPOINTS                          #
################################################################################
@app.post("/fetch-project")
async def fetchProject(
        request: Request,
        project_id: str = Query(..., description="Project UUID"),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser),
        enforcer: SyncedEnforcer = Depends(getEnforcer)
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
           

          LEFT JOIN client c
            ON c.id = cu.client_id
           
           AND cut.name = 'client'

          JOIN project_priority pp
            ON pp.id = p.priority_id
           

          JOIN project_type pt
            ON pt.id = p.type_id
           

          JOIN state st
            ON st.id = p.state_id
           

          JOIN project_trade tr
            ON tr.id = p.trade_id
           

          JOIN status s
            ON s.id = p.status_id
           AND s.category = 'project'
           

          JOIN "user" au
            ON au.id = p.assignee_id
          WHERE p.id = $1
        """
    try:
        row = await conn.fetchrow(sql, project_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    project = dict(row)
    roles = await enforcer.get_roles_for_user_in_domain(user.email, "*") if user else []
    if "client_technician" in roles:
        project.pop("nte", None)

    payload = {"project": project}
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-messages")
async def getMessages(
        request: Request,
        data: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
):
    projectId = data.get("projectId")
    size = data.get("size")
    last_seen_created_at = data.get("last_seen_created_at")
    last_seen_id = data.get("last_seen_id")
    if not projectId or not size:
        raise HTTPException(status_code=400, detail="invalid params")
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
             
            JOIN "user" u
              ON u.id = m.sender_id
             
            JOIN user_type ut
              ON ut.id = u.type_id
             
            WHERE  m.project_id = $1
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

    payload = {
        "messages": [dict(r) for r in rows],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/fetch-project-quotes")
async def fetchProjectQuotesEndpoint(
        request: Request,
        data: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
):
    project_id = data.get("project_id")
    size = data.get("size")
    last_seen_created_at = data.get("last_seen_created_at")
    last_seen_id = data.get("last_seen_id")
    if not project_id or not size:
        raise HTTPException(status_code=400, detail="invalid params")
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
         
          q.project_id = $1
          
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

    payload = {
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
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/fetch-project-documents")
async def fetchProjectDocumentsEndpoint(
        request: Request,
        data: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
):
    project_id = data.get("project_id")
    size = data.get("size")
    last_seen_created_at = data.get("last_seen_created_at")
    last_seen_id = data.get("last_seen_id")
    if not project_id or not size:
        raise HTTPException(status_code=400, detail="invalid params")
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
          d.project_id = $1
          
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

    payload = {
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
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


################################################################################
# TODO:                         CLIENTS PAGE ENDPOINTS                         #
################################################################################

@app.post("/get-clients")
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
             
            JOIN client_type ct
              ON ct.id = c.type_id
             
            LEFT JOIN client_aggregates ca
              ON ca.client_id = c.id
            WHERE (c.created_at, c.id) < ($1::timestamptz, $2::uuid)
              
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


@app.post("/get-client-types")
async def getClientTypes(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, value
            FROM client_type
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"client_types": [dict(r) for r in rows]}


@app.post("/get-client-statuses")
async def getClientStatuses(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, value, color
            FROM status
            WHERE category = 'client'
              
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
            "SELECT id FROM client_type WHERE value=$1  LIMIT 1;",
            clientType,
        )
        stateId = await conn.fetchval(
            "SELECT id FROM state WHERE name=$1  LIMIT 1;",
            stateName,
        )
        statusId = await conn.fetchval(
            "SELECT id FROM status WHERE category='client' AND value='Active'  LIMIT 1;"
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

@app.post("/fetch-client")
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
        JOIN state st ON st.id = c.state_id 
        JOIN status s ON s.id = c.status_id AND s.category = 'client' 
        WHERE c.id = $1 ;
    """

    try:
        row = await conn.fetchrow(sql, client_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not row:
        raise HTTPException(status_code=404, detail=f"Client {client_id} not found")

    return {"client": dict(row)}


@app.post("/fetch-client-invoices")
async def fetchClientInvoices(
        client_id: str = Query(..., description="Client UUID"),
        size: int = Query(..., gt=0, description="Number of invoices to return"),
        q: Optional[str] = Query(None, description="Search query"),
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
        JOIN status s ON s.id = i.status_id AND s.category = 'invoice' 
          i.client_id = $1
          
          AND (i.created_at, i.id) < ($2::timestamptz, $3::uuid)
          AND ($4::text IS NULL OR i.number::text ILIKE '%' || $4 || '%')
        ORDER BY i.created_at DESC, i.id DESC
        LIMIT $5;
    """

    try:
        rows = await conn.fetch(sql, client_id, cursor_ts, cursor_id, q, size)
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


@app.post("/fetch-client-onboarding-documents")
async def fetchClientOnboardingDocuments(
        request: Request,
        data: dict = Depends(decryptPayload()),
        client_id: Optional[str] = Query(None, description="Client UUID"),
        size: Optional[int] = Query(None, gt=0, description="Number of documents to return"),
        q: Optional[str] = Query(None, description="Search query"),
        last_seen_created_at: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        last_seen_id: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
):
    client_id = data.get("clientId") or client_id
    size = data.get("size", size)
    q = data.get("q", q)
    last_seen_created_at = data.get("last_seen_created_at", last_seen_created_at)
    last_seen_id = data.get("last_seen_id", last_seen_id)
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
          d.client_id = $1
          AND d.purpose = 'onboarding_paperwork'
          
          AND (d.created_at, d.id) < ($2::timestamptz, $3::uuid)
          AND ($4::text IS NULL OR d.file_name ILIKE '%' || $4 || '%')
        ORDER BY d.created_at DESC, d.id DESC
        LIMIT $5;
    """

    try:
        rows = await conn.fetch(sql, client_id, cursor_ts, cursor_id, q, size)
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

    payload = {
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
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/get-insurance-documents")
async def getInsuranceDocuments(
        request: Request,
        data: dict = Depends(decryptPayload()),
        client_id: Optional[str] = Query(None, description="Client UUID"),
        size: Optional[int] = Query(None, gt=0, description="Number of documents to return"),
        q: Optional[str] = Query(None, description="Search query"),
        last_seen_created_at: Optional[str] = Query(
            None,
            description="ISO-8601 UTC timestamp cursor (e.g. 2025-05-24T12:00:00Z)"
        ),
        last_seen_id: Optional[UUID] = Query(
            None,
            description="UUID cursor to break ties if multiple rows share the same timestamp"
        ),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
):
    client_id = data.get("clientId") or client_id
    size = data.get("size", size)
    q = data.get("q", q)
    last_seen_created_at = data.get("last_seen_created_at", last_seen_created_at)
    last_seen_id = data.get("last_seen_id", last_seen_id)
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
          d.client_id = $1
          AND d.purpose = 'insurance'
          
          AND (d.created_at, d.id) < ($2::timestamptz, $3::uuid)
          AND ($4::text IS NULL OR d.file_name ILIKE '%' || $4 || '%')
        ORDER BY d.created_at DESC, d.id DESC
        LIMIT $5;
    """

    rows = await conn.fetch(sql, client_id, cursor_ts, cursor_id, q, size)

    total = rows[0]["total_count"] if rows else 0

    if rows:
        last = rows[-1]
        next_ts = last["created_at"].isoformat()
        next_id = str(last["id"])
    else:
        next_ts = None
        next_id = None

    payload = {
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
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


@app.post("/fetch-client-projects")
async def fetchClientProjects(
        client_id: str = Query(..., description="Client UUID"),
        size: int = Query(..., gt=0, description="Number of projects to return"),
        q: Optional[str] = Query(None, description="Search query"),
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
        JOIN status s ON s.id = p.status_id AND s.category = 'project' 
          p.client_id = $1
          AND s.value = 'Open'
          
          AND (p.created_at, p.id) < ($2::timestamptz, $3::uuid)
          AND ($4::text IS NULL OR p.business_name ILIKE '%' || $4 || '%')
        ORDER BY p.created_at DESC, p.id DESC
        LIMIT $5;
    """

    try:
        rows = await conn.fetch(sql, client_id, cursor_ts, cursor_id, q, size)
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


@app.post("/get-billings")
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
         
        LEFT JOIN document d
          ON d.id = i.file_id
         
        WHERE (i.created_at, i.id) < ($1::timestamptz, $2::uuid)
          
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


@app.post("/get-billing-statuses")
async def getBillingStatuses(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, value, color
            FROM status
            WHERE category = 'billing'
              
            ORDER BY value;
        """

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"billing_statuses": [dict(r) for r in rows]}


@app.post("/get-invoice-statuses")
async def getInvoiceStatuses(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, value, color
              FROM status
             WHERE category = 'invoice'
               
             ORDER BY value;
        """
    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"invoice_statuses": [dict(r) for r in rows]}


@app.post("/get-quote-statuses")
async def getQuoteStatuses(conn: Connection = Depends(get_conn)):
    sql = """
            SELECT id, value, color
              FROM status
             WHERE category = 'quote'
               
             ORDER BY value;
        """
    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"quote_statuses": [dict(r) for r in rows]}


@app.post("/get-passwords")
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
            "SELECT id FROM status WHERE category='billing' AND value='Pending'  LIMIT 1;"
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
            "SELECT id FROM status WHERE category='quote' AND value='Pending'  LIMIT 1;"
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


@app.post("/get-invoice")
async def getInvoice(
        id: str = Query(..., description="Invoice UUID"),
        conn: Connection = Depends(get_conn)
):
    sql = """
        SELECT i.*, d.file_url, d.document_type
          FROM invoice i
          LEFT JOIN document d ON d.id = i.file_id 
         WHERE i.id = $1 
         LIMIT 1;
    """
    row = await conn.fetchrow(sql, id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Invoice {id} not found")
    return {"invoice": dict(row)}


@app.post("/get-quote")
async def getQuote(
        id: str = Query(..., description="Quote UUID"),
        conn: Connection = Depends(get_conn)
):
    sql = """
        SELECT q.*, d.file_url, d.document_type
          FROM quote q
          LEFT JOIN document d ON d.id = q.file_id 
         WHERE q.id = $1 
         LIMIT 1;
    """
    row = await conn.fetchrow(sql, id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Quote {id} not found")
    return {"quote": dict(row)}


################################################################################
# TODO:                         PROFILE ENDPOINTS                              #
################################################################################

@app.post("/get-onboarding-data")
async def getOnboardingData(
        request: Request,
        data: dict = Depends(decryptPayload()),
        clientId: Optional[str] = Query(None, description="Client UUID"),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
):
    clientId = data.get("clientId") or clientId
    if not clientId or not isUUIDv4(clientId):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid UUIDv4 (must be lowercase-hyphenated): {clientId}"
        )

    generalSql = """
        SELECT satellite_office_address, organization_type, establishment_year,
               annual_revenue, accepted_payment_methods, naics_code, duns_number
          FROM client_onboarding_general
         WHERE client_id = $1 
         LIMIT 1;
    """
    serviceSql = """
        SELECT coverage_area, admin_staff_count, field_staff_count, licenses,
               working_hours, covers_after_hours, covers_weekend_calls
          FROM client_onboarding_service
         WHERE client_id = $1 
         LIMIT 1;
    """
    contactSql = """
        SELECT dispatch_supervisor, field_supervisor, management_supervisor,
               regular_hours_contact, emergency_hours_contact
          FROM client_onboarding_contact
         WHERE client_id = $1 
         LIMIT 1;
    """
    loadSql = """
        SELECT avg_monthly_tickets_last4, po_source_split, monthly_po_capacity
          FROM client_onboarding_load
         WHERE client_id = $1 
         LIMIT 1;
    """
    tradeSql = """
        SELECT pt.value AS trade, tc.coverage_level
          FROM client_trade_coverage tc
          JOIN project_trade pt ON pt.id = tc.project_trade_id
         WHERE tc.client_id = $1
           
           ;
    """
    pricingSql = """
        SELECT item_label, regular_hours_rate, after_hours_rate, is_custom
          FROM client_pricing_structure
         WHERE client_id = $1 ;
    """
    refsSql = """
        SELECT company_name, contact_name, contact_email, contact_phone
          FROM client_references
         WHERE client_id = $1 ;
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

    payload = {
        "general": dict(generalRow) if generalRow else {},
        "service": dict(serviceRow) if serviceRow else {},
        "contact": dict(contactRow) if contactRow else {},
        "load": dict(loadRow) if loadRow else {},
        "tradeCoverage": [dict(r) for r in tradeRows],
        "pricing": [dict(r) for r in pricingRows],
        "references": [dict(r) for r in refsRows],
    }
    if user:
        payload = await encryptForUser(payload, user.email, conn, request.app)
    return payload


################################################################################
# TODO:                         ONBOARDING ENDPOINTS                           #
################################################################################


@app.post("/save-onboarding-data")
async def saveOnboardingData(
        request: Request,
        payload: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
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
                "SELECT id FROM project_trade WHERE value=$1  LIMIT 1;",
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

    resp_payload = {"status": "success"}
    if user:
        resp_payload = await encryptForUser(resp_payload, user.email, conn, request.app)
    return resp_payload


@app.post("/update-insurance-data")
async def updateInsuranceData(
        request: Request,
        payload: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn),
        user: SimpleUser = Depends(getCurrentUser)
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

    resp_payload = {"insuranceId": insuranceId}
    if user:
        resp_payload = await encryptForUser(resp_payload, user.email, conn, request.app)
    return resp_payload


@app.post("/auth/invite")
async def inviteUser(
        request: Request,
        data: dict = Depends(decryptPayload()),
        conn: Connection = Depends(get_conn)
):
    emailToInvite = data.get("emailToInvite")
    accountType = data.get("accountType")
    firstName = data.get("firstName", "")
    lastName = data.get("lastName", "")

    if not emailToInvite:
        raise HTTPException(status_code=400, detail="Invalid email")
    if not accountType:
        raise HTTPException(status_code=400, detail="Invalid account type")

    await createMagicLink(
        conn,
        request.app.state.privateKey,
        purpose="signup",
        recipientEmail=emailToInvite,
        firstName=firstName,
        lastName=lastName,
        accountType=accountType,
        ttlHours=24,
    )

    return {"status": "Link will be sent shortly"}


@app.put("/update-user")
async def updateUser(request: Request, payload: dict = Body(), conn: Connection = Depends(get_conn), enforcer: SyncedEnforcer = Depends(getEnforcer)):
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
        updates.append(f"email=${len(params) + 1}")
        params.append(email)
    if first_name:
        updates.append(f"first_name=${len(params) + 1}")
        params.append(first_name)
    if last_name:
        updates.append(f"last_name=${len(params) + 1}")
        params.append(last_name)

    if updates:
        await conn.execute(
            f"UPDATE \"user\" SET {', '.join(updates)} WHERE id=${len(params) + 1}",
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

        await enforcer.load_policy()
        enforcer.build_role_links()
        request.app.state.casbin_watcher.update()

    return {"status": "updated"}


TABLE_FIELDS = {
    "project_priority": ["value", "color"],
    "project_type": ["value"],
    "project_trade": ["value"],
    "state": ["name"],
    "status": ["category", "value", "color"],
}


@app.post("/admin/create-record")
async def adminCreateRecord(payload: dict = Body(), conn: Connection = Depends(get_conn)):
    table = payload.get("table")
    fields = TABLE_FIELDS.get(table)
    if not table or not fields:
        raise HTTPException(status_code=400, detail="Invalid table")
    values = [payload.get(f) for f in fields]
    placeholders = ",".join(f"${i+1}" for i in range(len(fields)))
    sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders}) RETURNING id;"
    try:
        newId = await conn.fetchval(sql, *values)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"id": str(newId)}


@app.put("/admin/update-record")
async def adminUpdateRecord(payload: dict = Body(), conn: Connection = Depends(get_conn)):
    table = payload.get("table")
    recordId = payload.get("id")
    fields = TABLE_FIELDS.get(table)
    if not table or not fields or not recordId:
        raise HTTPException(status_code=400, detail="Invalid data")
    updates = []
    params = []
    for f in fields:
        if f in payload:
            params.append(payload[f])
            updates.append(f"{f}=${len(params)}")
    params.append(UUID(recordId))
    if not updates:
        return {"status": "no-op"}
    sql = f"UPDATE {table} SET {', '.join(updates)}, updated_at=now() WHERE id=${len(params)};"
    try:
        await conn.execute(sql, *params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "updated"}


@app.post("/admin/delete-record")
async def adminDeleteRecord(payload: dict = Body(), conn: Connection = Depends(get_conn)):
    table = payload.get("table")
    recordId = payload.get("id")
    if not table or not recordId or table not in TABLE_FIELDS:
        raise HTTPException(status_code=400, detail="Invalid data")
    sql = f"UPDATE {table} SET is_deleted=TRUE, deleted_at=now() WHERE id=$1;"
    try:
        await conn.execute(sql, UUID(recordId))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "deleted"}


@app.post("/auth/login")
async def login(request: Request, data: dict = Depends(decryptPayload()), conn: Connection = Depends(get_conn)):
    email = data.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="Email required")
    row = await conn.fetchrow(
        "SELECT id, is_blacklisted FROM \"user\" WHERE email=$1 ",
        email,
    )
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    if row["is_blacklisted"]:
        raise HTTPException(status_code=403, detail="blacklisted")

    await createMagicLink(
        conn,
        request.app.state.privateKey,
        purpose="login",
        recipientEmail=email,
        firstName="",
        lastName="",
        accountType="",
        ttlHours=24,
    )

    return {"status": "Link will be sent shortly"}


@app.post("/auth/set-recovery-phrase")
async def setRecoveryPhrase(request: Request, data: dict = Depends(decryptPayload(True)),
                            conn: Connection = Depends(get_conn), enforcer: SyncedEnforcer = Depends(getEnforcer)):

    print("RECOVERY-PHRASE-PAYLOAD: ", data)
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
    print("USER EMAIL: ", userEmail)
    if userEmail:
        first_name = data.get("firstName", "")
        last_name = data.get("lastName", "")
        accountType = data.get("accountType", "")
        existing = await conn.fetchrow(
            "SELECT id FROM \"user\" WHERE email=$1 ",
            userEmail,
        )
        if not existing:
            hex_color = f"#{random.randint(0, 0xFFFFFF):06x}"
            await conn.fetchrow(
                "INSERT INTO \"user\" (email, first_name, last_name, hex_color, is_active, is_client) "
                "VALUES ($1,$2,$3,$4,FALSE,$5) RETURNING id",
                userEmail,
                first_name,
                last_name,
                hex_color,
                accountType.startswith("Client"),
            )

            role = accountType.replace(" ", "_").lower()
            domain = "*"

            added: bool = await enforcer.add_role_for_user_in_domain(
                userEmail,
                role,
                domain,
            )

            if not added:
                raise HTTPException(status_code=500, detail="Role assignment failed")
            gPolicies = enforcer.get_grouping_policy()
            print("ALL GROUPING POLICIES:", gPolicies)
            await enforcer.load_policy()
            enforcer.build_role_links()
            request.app.state.casbin_watcher.update()

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

        jti = str(uuid4())
        exp_dt = datetime.now(timezone.utc) + timedelta(minutes=60)
        await conn.execute(
            "INSERT INTO jwt_token (jti, user_email, expires_at, revoked) VALUES ($1,$2,$3,FALSE)",
            jti,
            userEmail,
            exp_dt
        )
        redis = request.app.state.redis
        await redis.sadd("active_jtis", jti)
        await redis.publish("jwt_updates", f"add:{jti}")
        await conn.execute("UPDATE magic_link SET consumed=TRUE WHERE uuid=$1", UUID(link_uuid))

        roles = await enforcer.get_roles_for_user(userEmail)
        await conn.execute(
            "UPDATE \"user\" SET is_active=TRUE, onboarding_done=$2, has_set_recovery_phrase=TRUE WHERE email=$1",
            userEmail,
            ("client_admin" in roles)
        )
        print("ROLES: ", roles)
        print("IS CLIENT: ", "client_admin" in roles)
        if "client_admin" in roles:
            next_payload = {
                "next_step": "/onboarding",
                "exp": int((datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()),
            }
        else:
            next_payload = {
                "next_step": "/dashboard",
                "exp": int((datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()),
            }

        nav_token = generateJwtRs256(next_payload, request.app.state.privateKey)
        session_token = jwt.encode({"sub": userEmail, "jti": jti, "exp": exp_dt}, SECRET_KEY, algorithm="HS256")
        payload = {"token": nav_token}

        if clientPub is not None:
            payload = encryptForClient(payload, clientPub, request.app)

        response = JSONResponse(payload)
        response.set_cookie(
            "session",
            session_token,
            httponly=True,
            secure=True,
            samesite="none",
            domain="localhost",
            path="/",
            max_age=60 * 60 * 24,
        )

        return response


@app.post("/auth/get-recovery-params")
async def getRecoveryParams(email: str, conn: Connection = Depends(get_conn)):
    row = await conn.fetchrow(
        """
        SELECT u.id, p.salt, p.kdf_params
          FROM "user" u
          JOIN user_recovery_params p ON p.user_email = u.email
         WHERE u.email=$1 
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
async def completeOnboarding(request: Request, payload: dict = Depends(decryptPayload()),
                             conn: Connection = Depends(get_conn),
                             user: SimpleUser = Depends(getCurrentUser)):
    userId = payload.get("userId")
    if not userId or not isUUIDv4(userId):
        raise HTTPException(status_code=400, detail="Invalid userId")
    await conn.execute(
        "UPDATE \"user\" SET onboarding_done=TRUE WHERE id=$1",
        userId,
    )
    resp_payload = {"status": "ok"}
    if user:
        resp_payload = await encryptForUser(resp_payload, user.email, conn, request.app)
    return resp_payload


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


@app.post("/auth/refresh-session")
async def refreshSession(request: Request, conn: Connection = Depends(get_conn)):
    token = request.cookies.get("session")
    if not token:
        raise HTTPException(status_code=401, detail="no token")
    try:
        data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"], options={"verify_exp": False})
    except Exception:
        raise HTTPException(status_code=401, detail="invalid")
    jti = data.get("jti")
    email = data.get("sub")
    if not jti or not email:
        raise HTTPException(status_code=400, detail="bad token")
    redis = request.app.state.redis
    if not await redis.sismember("active_jtis", jti):
        raise HTTPException(status_code=401, detail="revoked")
    exp_dt = datetime.now(timezone.utc) + timedelta(minutes=60)
    await conn.execute("UPDATE jwt_token SET expires_at=$1 WHERE jti=$2", exp_dt, jti)
    session_token = jwt.encode({"sub": email, "jti": jti, "exp": exp_dt}, SECRET_KEY, algorithm="HS256")
    resp = JSONResponse({"status": "ok"})
    resp.set_cookie(
        "session",
        session_token,
        httponly=True,
        secure=True,
        samesite="none",
        domain="localhost",
        path="/",
        max_age=60 * 60 * 24,
    )

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
