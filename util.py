import uuid
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from hmac import new
from time import perf_counter
from typing import Optional
from uuid import UUID, uuid4

from asyncpg import Record, Pool, Connection
import base64
import json
import hmac


async def isUUIDv4(u: str) -> bool:
    try:
        val = uuid.UUID(u, version=4)
    except (ValueError, AttributeError):
        return False

    return str(val) == u


async def hasUnexpiredMagicLink(
        db_pool,
        user_id: UUID,
        purpose: str
) -> bool:
    sql = """
        SELECT 1
        FROM magic_link
        WHERE
          user_id    = $1
          AND purpose = $2
          AND consumed = FALSE
          AND expires_at > now()
        LIMIT 1;
    """
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(sql, user_id, purpose)
    return bool(row)


async def createMagicLink(
        conn: Connection,
        server_secret: str,
        purpose: str,
        recipientEmail: str,
        ttlHours: int = 24,
) -> tuple[str, str]:
    # 1) Generate a new UUID v4
    new_uuid = uuid4()

    # 2) Compute HMAC-SHA256 signature over the UUID (as a string)
    #    Note: hmac.new expects bytes, so encode both key and message as UTF-8.
    signature = new(
        key=server_secret.encode("utf-8"),
        msg=str(new_uuid).encode("utf-8"),
        digestmod=sha256
    ).hexdigest()

    # 3) Set an expiration time based on ttlHours
    expires_at = datetime.now(timezone.utc) + timedelta(hours=ttlHours)

    # 4) Insert into magic_links table
    sql = """
        INSERT INTO magic_link (
          uuid, sig, expires_at, consumed, purpose, send_to
        ) VALUES ($1, $2, $3, FALSE, $4, $5);
    """

    await conn.execute(
        sql,
        new_uuid,
        signature,
        expires_at,
        purpose,
        recipientEmail
    )

    return str(new_uuid), signature


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def generateJwt(payload: dict, secret: str, expiresIn: int) -> str:
    exp = int(datetime.now(timezone.utc).timestamp()) + expiresIn
    payload = {**payload, "exp": exp}
    header = {"alg": "HS256", "typ": "JWT"}
    segments = [
        _b64url_encode(json.dumps(header).encode("utf-8")),
        _b64url_encode(json.dumps(payload).encode("utf-8")),
    ]
    signingInput = ".".join(segments).encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), signingInput, sha256).digest()
    segments.append(_b64url_encode(signature))
    return ".".join(segments)


# TODO: Make it an endpoint
async def fetch_project_assessments(
        db_pool: Pool,
        project_id: str
) -> Optional[Record]:
    """
    Fetch the “Visit Notes”, “Planned Resolution”, and “Material/Parts Needed”
    fields for a given project.id. Returns an Record containing:
      - project_id
      - visit_notes
      - planned_resolution
      - material_parts_needed
    or None if the project doesn’t exist or is_deleted = TRUE.
    """

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

    start = perf_counter()
    async with db_pool.acquire() as conn:
        rec = await conn.fetchrow(sql, project_id)
    dt = perf_counter() - start
    return rec
