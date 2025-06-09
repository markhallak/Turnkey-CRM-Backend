from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from asyncpg import create_pool, Pool, Connection
from fastapi import FastAPI, HTTPException, Query, Body, Request, Depends
from constants import DATABASE_URL
from util import isUUIDv4


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup logic
    app.state.db_pool: Pool = await create_pool(
        dsn=DATABASE_URL, min_size=5, max_size=20
    )
    print("DB pool created")
    yield

    # shutdown logic
    await app.state.db_pool.close()
    print("DB pool closed")


app = FastAPI(lifespan=lifespan)


def get_db_pool(request: Request) -> Pool:
    return request.app.state.db_pool


async def get_conn(db_pool: Pool = Depends(get_db_pool)):
    async with db_pool.acquire() as conn:
        yield conn


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


# TODO: Still needs work, check the user_type, and the client_id if we need to return those
@app.get("/get-profile-details")
async def getProfileDetails(
        user_id: UUID = Query(..., description="UUID of the user whose profile to fetch"),
        conn: Connection = Depends(get_conn)
):
    sql = """
            SELECT first_name, hex_color
            FROM "user"
            WHERE id = $1
              AND is_deleted = FALSE
            LIMIT 1;
        """

    row = await conn.fetchrow(sql, user_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")

    return {
        "first_name": row["first_name"],
        "hex_color": row["hex_color"],
    }


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

          JOIN user_type cut
            ON cut.id = cu.type_id
           AND cut.is_deleted = FALSE

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
        FROM password p
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

@app.post("/setup-recovery")
async def setupRecovery(payload: dict = Body()):
    userId = payload['userId']
    purpose = payload['purpose']

    if not isUUIDv4(userId):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid UUIDv4 (must be lowercase-hyphenated): {userId}"
        )

    return {
        "status": "success",
        "userId": userId,
        "purpose": purpose
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
