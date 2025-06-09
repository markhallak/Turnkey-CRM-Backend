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
                "date_uploaded": r["date_uploaded"]
            }
            for r in rows
        ],
        "total_count": total,
        "page_size": size,
        "last_seen_created_at": next_ts,
        "last_seen_id": next_id,
    }


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
