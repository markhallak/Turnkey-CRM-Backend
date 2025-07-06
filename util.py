import uuid
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from asyncpg import Connection
import json
import jwt


async def isUUIDv4(u: str) -> bool:
    try:
        val = uuid.UUID(u, version=4)
    except (ValueError, AttributeError):
        return False

    return str(val) == u


async def createMagicLink(
        conn: Connection,
        private_key_pem: str,
        purpose: str,
        recipientEmail: str,
        firstName: str,
        lastName: str,
        accountType: str,
        ttlHours: int = 24,
) -> str:
    new_uuid = uuid4()
    expires_at = datetime.now(timezone.utc) + timedelta(hours=ttlHours)

    if purpose == "signup":
        if not firstName or len(firstName) == 0:
            raise Exception("First Name is empty")
        if not lastName or len(lastName) == 0:
            raise Exception("Last Name is empty")

        payload = {
            "uuid": str(new_uuid),
            "userEmail": recipientEmail,
            "firstName": firstName,
            "lastName": lastName,
            "accountType": accountType,
            "next_step": "set-recovery-phrase",
            "exp": int(expires_at.timestamp()),
        }
    else:
        payload = {
            "uuid": str(new_uuid),
            "userEmail": recipientEmail,
            "next_step": "dashboard",
            "exp": int(expires_at.timestamp()),
        }
    token = generateJwtRs256(payload, private_key_pem)

    sql = """
        INSERT INTO magic_link (
          uuid, user_email, token, expires_at, consumed, purpose, send_to
        ) VALUES ($1, $2, $3, $4, FALSE, $5, $6);
    """

    await conn.execute(
        sql,
        new_uuid,
        recipientEmail,
        token,
        expires_at,
        purpose,
        recipientEmail,
    )

    return token



def generateJwtRs256(payload: dict, privateKeyPem: str) -> str:
    return jwt.encode(payload, privateKeyPem, algorithm="RS256")


def decodeJwtRs256(token: str, publicKeyPem: str) -> dict:
    return jwt.decode(token, publicKeyPem, algorithms=["RS256"])


