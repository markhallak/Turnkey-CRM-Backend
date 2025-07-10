import asyncpg
import asyncio
import sys
import re
from constants import ASYNCPG_URL

RESET_SCHEMA = """
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;
"""

EXTENSIONS = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS citext;
"""

# List of US states used to seed the state table on first run
US_STATES = [
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
    "District of Columbia",
]

# ──────────────────────────────────────────────────────────────────────────────
# CASBIN RULE TABLE
# ──────────────────────────────────────────────────────────────────────────────

CASBIN_RULE = """
CREATE TABLE casbin_rule (
  id SERIAL PRIMARY KEY,
  ptype VARCHAR(100) NOT NULL,
  v0   VARCHAR(255),
  v1   VARCHAR(255),
  v2   VARCHAR(255),
  v3   VARCHAR(255),
  v4   VARCHAR(255),
  v5   VARCHAR(255)
);
"""

# ──────────────────────────────────────────────────────────────────────────────
# 1: STATUS
# ──────────────────────────────────────────────────────────────────────────────

STATUS = """
CREATE TABLE IF NOT EXISTS status (
  id         UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  category   VARCHAR(50)  NOT NULL,
  value      VARCHAR(255) NOT NULL,
  color      VARCHAR(255),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(category, value)
);
"""

# ──────────────────────────────────────────────────────────────────────────────
# 2: CLIENT
# ──────────────────────────────────────────────────────────────────────────────

CLIENT = """
CREATE TABLE IF NOT EXISTS client (
  id                          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  company_name                VARCHAR(255) NOT NULL,
  poc_first_name              VARCHAR(255) NOT NULL,
  poc_last_name               VARCHAR(255) NOT NULL,
  type_id                     UUID         NOT NULL REFERENCES client_type(id)   ON UPDATE CASCADE ON DELETE RESTRICT,
  status_id                   UUID         NOT NULL REFERENCES status(id)      ON UPDATE CASCADE ON DELETE RESTRICT,
  address_line_1              VARCHAR(255) NOT NULL,
  address_line_2              VARCHAR(255),
  city                        VARCHAR(255) NOT NULL,
  state_id                    UUID         NOT NULL REFERENCES state(id)         ON UPDATE CASCADE ON DELETE RESTRICT,
  zip_code                    VARCHAR(255) NOT NULL,
  general_onboarding_email    CITEXT       NOT NULL,
  phone_number_main_line      VARCHAR(255) NOT NULL,
  accounting_email            CITEXT       NOT NULL,
  accounting_phone_number     VARCHAR(255) NOT NULL,
  pay_terms                   VARCHAR(255) NOT NULL,
  trip_rate                   INT          NOT NULL,
  updates                     TEXT         NOT NULL,
  special_notes               TEXT         NOT NULL,
  created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  is_deleted                  BOOLEAN      NOT NULL DEFAULT FALSE,
  deleted_at                  TIMESTAMPTZ,
  search_text TEXT
);
"""

CLIENT_TYPE = """
CREATE TABLE IF NOT EXISTS client_type (
  id         UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  value      VARCHAR(255) NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

PAY_TERM = """
CREATE TABLE IF NOT EXISTS pay_term (
  id         UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  value      VARCHAR(255) NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

CLIENT_RATE = """
CREATE TABLE IF NOT EXISTS client_rate (
  client_id   UUID        NOT NULL REFERENCES client(id) ON UPDATE CASCADE ON DELETE CASCADE,
  rate_type   VARCHAR(50) NOT NULL,
  rate_amount INT         NOT NULL,
  PRIMARY KEY (client_id, rate_type),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# ──────────────────────────────────────────────────────────────────────────────
# 3: USER
# ──────────────────────────────────────────────────────────────────────────────

USER = """
CREATE TABLE IF NOT EXISTS "user" (
  id         UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  email      CITEXT       NOT NULL UNIQUE,
  first_name VARCHAR(255) NOT NULL,
  last_name  VARCHAR(255) NOT NULL,
  hex_color  VARCHAR(7)   CHECK (hex_color ~ '^#[0-9A-Fa-f]{6}$'),
  client_id  UUID         REFERENCES client(id)    ON UPDATE CASCADE ON DELETE RESTRICT,
  created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
  has_set_recovery_phrase BOOLEAN NOT NULL DEFAULT FALSE,
  onboarding_done BOOLEAN NOT NULL DEFAULT FALSE,
  is_active  BOOLEAN      NOT NULL DEFAULT TRUE,
  is_client  BOOLEAN      NOT NULL DEFAULT FALSE,
  is_blacklisted BOOLEAN NOT NULL DEFAULT FALSE,
  is_deleted BOOLEAN      NOT NULL DEFAULT FALSE,
  deleted_at TIMESTAMPTZ
);
"""

# ──────────────────────────────────────────────────────────────────────────────
# 4: PROJECT
# ──────────────────────────────────────────────────────────────────────────────

PROJECT = """
CREATE TABLE IF NOT EXISTS project (
  id                    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id             UUID         NOT NULL REFERENCES client(id)            ON UPDATE CASCADE ON DELETE RESTRICT,
  priority_id           UUID         NOT NULL REFERENCES project_priority(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  type_id               UUID         NOT NULL REFERENCES project_type(id)     ON UPDATE CASCADE ON DELETE RESTRICT,
  address               VARCHAR(255) NOT NULL,
  address_line1         VARCHAR(255) NOT NULL,
  address_line2         VARCHAR(255),
  city                  VARCHAR(255) NOT NULL,
  state_id              UUID         NOT NULL REFERENCES state(id)           ON UPDATE CASCADE ON DELETE RESTRICT,
  zip_code              VARCHAR(20)  NOT NULL,
  trade_id              UUID         NOT NULL REFERENCES project_trade(id)    ON UPDATE CASCADE ON DELETE RESTRICT,
  status_id             UUID         NOT NULL REFERENCES status(id)         ON UPDATE CASCADE ON DELETE RESTRICT,
  nte                   NUMERIC      NOT NULL,
  business_name         VARCHAR(255) NOT NULL,
  due_date              DATE         NOT NULL,
  scheduled_date         DATE,
  date_received         DATE         NOT NULL,
  scope_of_work         TEXT         NOT NULL,
  special_notes         TEXT         NOT NULL,
  visit_notes           TEXT         NOT NULL,
  planned_resolution    TEXT         NOT NULL,
  material_parts_needed TEXT         NOT NULL,
  assignee_id           UUID         NOT NULL REFERENCES "user"(id)           ON UPDATE CASCADE ON DELETE RESTRICT,
  created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
  is_deleted            BOOLEAN      NOT NULL DEFAULT FALSE,
  deleted_at            TIMESTAMPTZ,
  search_text TEXT
);
"""

PROJECT_PRIORITY = """
CREATE TABLE IF NOT EXISTS project_priority (
  id         UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  value      VARCHAR(255) NOT NULL UNIQUE,
  color      VARCHAR(255) NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

PROJECTS_TYPE = """
CREATE TABLE IF NOT EXISTS project_type (
  id         UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  value      VARCHAR(255) NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

PROJECTS_TRADE = """
CREATE TABLE IF NOT EXISTS project_trade (
  id         UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  value      VARCHAR(255) NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# ──────────────────────────────────────────────────────────────────────────────
# 5: DOCUMENT
# ──────────────────────────────────────────────────────────────────────────────

DOCUMENT = """
CREATE TABLE IF NOT EXISTS document (
  id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  file_name      VARCHAR(255) NOT NULL,
  file_url       TEXT         NOT NULL,
  file_extension VARCHAR(50)  NOT NULL,
  file_size      BIGINT       NOT NULL,
  document_type  VARCHAR(255) NOT NULL,
  project_id     UUID         REFERENCES project(id)   ON UPDATE CASCADE ON DELETE RESTRICT,
  client_id      UUID         REFERENCES client(id)    ON UPDATE CASCADE ON DELETE RESTRICT,
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  purpose        VARCHAR(100) NOT NULL,
  is_deleted     BOOLEAN      NOT NULL DEFAULT FALSE,
  deleted_at     TIMESTAMPTZ,
  search_text TEXT
);
"""

# ──────────────────────────────────────────────────────────────────────────────
# 6: QUOTE
# ──────────────────────────────────────────────────────────────────────────────

QUOTE = """
CREATE TABLE IF NOT EXISTS quote (
  id         UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  number     INT          GENERATED ALWAYS AS IDENTITY UNIQUE,
  created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
  amount     NUMERIC      NOT NULL,
  project_id UUID         NOT NULL REFERENCES project(id)   ON UPDATE CASCADE ON DELETE RESTRICT,
  client_id  UUID         NOT NULL REFERENCES client(id)    ON UPDATE CASCADE ON DELETE RESTRICT,
  status_id  UUID         NOT NULL REFERENCES status(id)  ON UPDATE CASCADE ON DELETE RESTRICT,
  file_id    UUID         NOT NULL REFERENCES document(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  is_deleted BOOLEAN      NOT NULL DEFAULT FALSE,
  deleted_at TIMESTAMPTZ,
  search_text TEXT
);
"""

# ──────────────────────────────────────────────────────────────────────────────
# 7: INVOICE
# ──────────────────────────────────────────────────────────────────────────────

INVOICE = """
CREATE TABLE IF NOT EXISTS invoice (
  id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  number        INT          GENERATED ALWAYS AS IDENTITY UNIQUE,
  due_date DATE         NOT NULL,
  issuance_date DATE         NOT NULL,
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  amount        NUMERIC      NOT NULL,
  client_id     UUID         NOT NULL REFERENCES client(id)   ON UPDATE CASCADE ON DELETE RESTRICT,
  status_id     UUID         NOT NULL REFERENCES status(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  file_id       UUID         NOT NULL REFERENCES document(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  is_deleted    BOOLEAN      NOT NULL DEFAULT FALSE,
  deleted_at    TIMESTAMPTZ,
  search_text TEXT
);
"""

# ──────────────────────────────────────────────────────────────────────────────
# 8: MESSAGE
# ──────────────────────────────────────────────────────────────────────────────

MESSAGE = """
CREATE TABLE IF NOT EXISTS message (
  id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
  content            TEXT         NOT NULL,
  sender_id          UUID         NOT NULL REFERENCES "user"(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  project_id         UUID         NOT NULL REFERENCES project(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  file_attachment_id UUID         REFERENCES document(id)      ON UPDATE CASCADE ON DELETE RESTRICT,
  is_deleted         BOOLEAN      NOT NULL DEFAULT FALSE,
  deleted_at         TIMESTAMPTZ,
  has_mentions       BOOLEAN      NOT NULL DEFAULT FALSE,
  search_text TEXT
);
"""

MESSAGE_MENTION = """
CREATE TABLE IF NOT EXISTS message_mention (
  id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  message_id  UUID         NOT NULL REFERENCES message(id) ON DELETE CASCADE,
  user_email  CITEXT       NOT NULL REFERENCES "user"(email)    ON DELETE CASCADE,
  created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  UNIQUE(message_id, user_email)
);
"""

# ──────────────────────────────────────────────────────────────────────────────
# 9: MAGIC_LINK
# ──────────────────────────────────────────────────────────────────────────────

MAGIC_LINK = """
CREATE TABLE IF NOT EXISTS magic_link (
  uuid           UUID        PRIMARY KEY,
  user_email     CITEXT       NOT NULL,
  token          TEXT        NOT NULL,
  expires_at     TIMESTAMPTZ NOT NULL,
  consumed       BOOLEAN     NOT NULL DEFAULT FALSE,
  purpose        VARCHAR(32) NOT NULL,
  is_sent        BOOLEAN     NOT NULL DEFAULT FALSE,
  send_to        CITEXT       NOT NULL
);
"""

MAGIC_LINK_LISTEN_NOTIFY = """
CREATE OR REPLACE FUNCTION notify_magic_link_insert() RETURNS trigger AS $$
DECLARE
  payload TEXT;
BEGIN
  payload := row_to_json(NEW)::text;
  PERFORM pg_notify('new_magic_link_row', payload);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_new_magic_link_row
  AFTER INSERT ON magic_link
  FOR EACH ROW
  EXECUTE FUNCTION notify_magic_link_insert();
"""

# ──────────────────────────────────────────────────────────────────────────────
# 10: CLIENT_PASSWORD
# ──────────────────────────────────────────────────────────────────────────────

CLIENT_PASSWORD = """
CREATE TABLE IF NOT EXISTS client_password (
  user_email               CITEXT        PRIMARY KEY
    REFERENCES "user"(email) ON DELETE CASCADE,
  client_id             UUID        NOT NULL REFERENCES client(id) ON UPDATE CASCADE ON DELETE CASCADE,
  encrypted_password    BYTEA       NOT NULL,
  iv                    BYTEA       NOT NULL,
  salt                  BYTEA       NOT NULL,
  kdf_params            JSONB       NOT NULL,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# Parameters used for user recovery phrase KDF
USER_RECOVERY_PARAMS = """
CREATE TABLE IF NOT EXISTS user_recovery_params (
  user_email    CITEXT        PRIMARY KEY
    REFERENCES "user"(email) ON DELETE CASCADE,
  iv         BYTEA       NOT NULL,
  salt       BYTEA       NOT NULL,
  kdf_params JSONB       NOT NULL,
  digest     BYTEA       NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# 11: USER_KEY
USER_KEY = """
CREATE TABLE IF NOT EXISTS user_key (
  user_email  CITEXT    NOT NULL REFERENCES "user"(email) ON DELETE CASCADE,
  purpose  VARCHAR(16) NOT NULL,
  public_key BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_email, purpose)
);
"""

JWT_TOKEN = """
CREATE TABLE IF NOT EXISTS jwt_token (
  jti UUID PRIMARY KEY,
  user_email CITEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  revoked BOOLEAN NOT NULL DEFAULT FALSE
);
"""

INSURANCE = """
CREATE TABLE IF NOT EXISTS insurance (
  id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id       UUID         NOT NULL REFERENCES client(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  provider        VARCHAR(255),
  policy_number   VARCHAR(255),
  coverage_amount NUMERIC,
  start_date      DATE,
  end_date        DATE,
  created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
  is_deleted      BOOLEAN      NOT NULL DEFAULT FALSE,
  deleted_at      TIMESTAMPTZ
);
"""

# ──────────────────────────────────────────────────────────────────────────────
# 11: STATE
# ──────────────────────────────────────────────────────────────────────────────

STATE = """
CREATE TABLE IF NOT EXISTS state (
  id         UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  name       VARCHAR(255) NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# ──────────────────────────────────────────────────────────────────────────────
# 12: Notification
# ──────────────────────────────────────────────────────────────────────────────

NOTIFICATION = """
CREATE TABLE IF NOT EXISTS notification (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  triggered_by_category VARCHAR(100) NOT NULL,
  triggered_by_user CITEXT,
  content TEXT NOT NULL,
  isProcessed BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

NOTIFICATION_LISTEN_NOTIFY = """
CREATE OR REPLACE FUNCTION notify_notification_insert() RETURNS trigger AS $$
DECLARE
  payload TEXT;
BEGIN
  payload := row_to_json(NEW)::text;
  PERFORM pg_notify('new_notification_row', payload);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_new_notification_row
  AFTER INSERT ON notification
  FOR EACH ROW
  EXECUTE FUNCTION notify_notification_insert();
"""

# ──────────────────────────────────────────────────────────────────────────────
# 13: Notification
# ──────────────────────────────────────────────────────────────────────────────

ONBOARDING = """
CREATE TABLE IF NOT EXISTS client_onboarding_general (
  id                          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id                   UUID         NOT NULL REFERENCES client(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  satellite_office_address    VARCHAR(255),
  organization_type           VARCHAR(100),
  establishment_year          INT,
  annual_revenue              NUMERIC,
  accepted_payment_methods    TEXT,
  naics_code                  VARCHAR(20),
  duns_number                 VARCHAR(20),
  created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS client_onboarding_service (
  id                          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id                   UUID         NOT NULL REFERENCES client(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  coverage_area               TEXT,
  admin_staff_count           INT,
  field_staff_count           INT,
  licenses                    TEXT,
  working_hours               VARCHAR(100),
  covers_after_hours          BOOLEAN      NOT NULL DEFAULT FALSE,
  covers_weekend_calls        BOOLEAN      NOT NULL DEFAULT FALSE,
  created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS client_onboarding_contact (
  id                          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id                   UUID         NOT NULL REFERENCES client(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  dispatch_supervisor         VARCHAR(255),
  field_supervisor            VARCHAR(255),
  management_supervisor       VARCHAR(255),
  regular_hours_contact       VARCHAR(255),
  emergency_hours_contact     VARCHAR(255),
  created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS client_onboarding_load (
  id                          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id                   UUID         NOT NULL REFERENCES client(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  avg_monthly_tickets_last4   INT,
  po_source_split             TEXT,
  monthly_po_capacity         INT,
  created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS client_trade_coverage (
  id                          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id                   UUID         NOT NULL REFERENCES client(id) ON UPDATE CASCADE ON DELETE CASCADE,
  project_trade_id            UUID         NOT NULL REFERENCES project_trade(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  coverage_level              VARCHAR(20)  NOT NULL 
    CHECK (coverage_level IN ('NOT','LIGHT','FULL')),
  created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  UNIQUE (client_id, project_trade_id)
);

CREATE TABLE IF NOT EXISTS client_pricing_structure (
  id                          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id                   UUID         NOT NULL REFERENCES client(id) ON UPDATE CASCADE ON DELETE CASCADE,
  item_label                  VARCHAR(100) NOT NULL,
  regular_hours_rate          VARCHAR(50),
  after_hours_rate            VARCHAR(50),
  is_custom                   BOOLEAN      NOT NULL DEFAULT FALSE,
  created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS client_references (
  id                          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id                   UUID         NOT NULL REFERENCES client(id) ON UPDATE CASCADE ON DELETE CASCADE,
  company_name                VARCHAR(255),
  contact_name                VARCHAR(255),
  contact_email               CITEXT,
  contact_phone               VARCHAR(50),
  created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
"""

# Mapping of account managers to clients
ACCOUNT_MANAGER_CLIENT = """
CREATE TABLE IF NOT EXISTS account_manager_client (
  account_manager_email CITEXT NOT NULL REFERENCES "user"(email) ON DELETE CASCADE,
  client_id UUID NOT NULL REFERENCES client(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (account_manager_email, client_id)
);
"""

# Mapping of client admins to client technicians
CLIENT_ADMIN_TECHNICIAN = """
CREATE TABLE IF NOT EXISTS client_admin_technician (
  client_admin_email CITEXT NOT NULL REFERENCES "user"(email) ON DELETE CASCADE,
  technician_email   CITEXT NOT NULL REFERENCES "user"(email) ON DELETE CASCADE,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (client_admin_email, technician_email)
);
"""


# ──────────────────────────────────────────────────────────────────────────────
# 14: ALTERS (search_text columns)
# ──────────────────────────────────────────────────────────────────────────────

ALTERS = """
ALTER TABLE project  ADD COLUMN IF NOT EXISTS search_text TEXT;
ALTER TABLE client   ADD COLUMN IF NOT EXISTS search_text TEXT;
ALTER TABLE document ADD COLUMN IF NOT EXISTS search_text TEXT;
ALTER TABLE quote    ADD COLUMN IF NOT EXISTS search_text TEXT;
ALTER TABLE invoice  ADD COLUMN IF NOT EXISTS search_text TEXT;
ALTER TABLE message  ADD COLUMN IF NOT EXISTS search_text TEXT;
"""

# ──────────────────────────────────────────────────────────────────────────────
# 15: VIEWS
# ──────────────────────────────────────────────────────────────────────────────

VIEWS = """
CREATE OR REPLACE VIEW client_aggregates AS
SELECT
  c.id                                AS client_id,
  COALESCE(SUM(i.amount), 0)          AS total_invoiced,
  COALESCE(
    SUM(i.amount) FILTER (
      WHERE s.category = 'invoice'
        AND s.value IN ('Paid','Collected')
    ), 0
  )                                    AS total_collected,
  COALESCE(COUNT(p.id), 0)            AS total_projects,
  COALESCE(
    COUNT(p.id) FILTER (
      WHERE ps.value IN ('Open','In Progress')
    ), 0
  )                                    AS open_projects
FROM client c
LEFT JOIN invoice i ON i.client_id = c.id AND i.is_deleted = FALSE
LEFT JOIN status s ON s.id = i.status_id
LEFT JOIN project p ON p.client_id = c.id AND p.is_deleted = FALSE
LEFT JOIN status ps ON ps.id = p.status_id
WHERE c.is_deleted = FALSE
GROUP BY c.id;

CREATE OR REPLACE VIEW overall_aggregates AS
SELECT
  COALESCE(SUM(i.amount), 0) AS total_invoiced,
  COALESCE(
    SUM(i.amount) FILTER (
      WHERE s.category = 'invoice'
        AND s.value IN ('Paid', 'Collected')
    ), 0
  ) AS total_collected,
  COALESCE(COUNT(p.id), 0)    AS total_projects,
  COALESCE(
    COUNT(p.id) FILTER (
      WHERE ps.value IN ('Open', 'In Progress')
    ), 0
  ) AS open_projects
FROM client c
LEFT JOIN invoice i ON i.client_id = c.id AND i.is_deleted = FALSE
LEFT JOIN status s ON s.id = i.status_id
LEFT JOIN project p ON p.client_id = c.id AND p.is_deleted = FALSE
LEFT JOIN status ps ON ps.id = p.status_id
WHERE c.is_deleted = FALSE;

CREATE OR REPLACE VIEW global_search AS
  SELECT 'project'  AS source_table, id::TEXT AS record_id, search_text, is_deleted FROM project
  UNION ALL
  SELECT 'client',  id::TEXT AS record_id, search_text, is_deleted FROM client
  UNION ALL
  SELECT 'document',id::TEXT AS record_id, search_text, is_deleted FROM document
  UNION ALL
  SELECT 'quote',   id::TEXT AS record_id, search_text, is_deleted FROM quote
  UNION ALL
  SELECT 'invoice', id::TEXT AS record_id, search_text, is_deleted FROM invoice
  UNION ALL
  SELECT 'message', id::TEXT AS record_id, search_text, is_deleted FROM message;
"""

# ──────────────────────────────────────────────────────────────────────────────
# 16: PREPARES
# ──────────────────────────────────────────────────────────────────────────────

PREPARES = """
PREPARE project_search(text, int) AS
SELECT *
FROM project
WHERE is_deleted = FALSE
  AND length($1) >= 3
  AND search_text ILIKE '%' || $1 || '%'
ORDER BY created_at DESC
LIMIT $2;

PREPARE global_search(text, int) AS
SELECT source_table, record_id
FROM global_search
WHERE is_deleted = FALSE
  AND length($1) >= 3
  AND search_text ILIKE '%' || $1 || '%'
ORDER BY source_table, record_id
LIMIT $2;
"""

# ──────────────────────────────────────────────────────────────────────────────
# 17: INDICES
# ──────────────────────────────────────────────────────────────────────────────

INDICES = """
CREATE INDEX IF NOT EXISTS idx_proj_client      ON project(client_id);
CREATE INDEX IF NOT EXISTS idx_proj_priority    ON project(priority_id);
CREATE INDEX IF NOT EXISTS idx_proj_type        ON project(type_id);
CREATE INDEX IF NOT EXISTS idx_proj_state       ON project(state_id);
CREATE INDEX IF NOT EXISTS idx_proj_trade       ON project(trade_id);
CREATE INDEX IF NOT EXISTS idx_proj_status      ON project(status_id);
CREATE INDEX IF NOT EXISTS idx_proj_assignee    ON project(assignee_id);
CREATE INDEX IF NOT EXISTS idx_proj_due         ON project(due_date);
CREATE INDEX IF NOT EXISTS idx_proj_received    ON project(date_received);
CREATE INDEX IF NOT EXISTS idx_proj_cursor      ON project (created_at DESC, id DESC) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_project_scheduled_month ON project (EXTRACT(MONTH FROM scheduled_date)) WHERE scheduled_date IS NOT NULL AND is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_project_due_month ON project (EXTRACT(MONTH FROM due_date)) WHERE scheduled_date IS NULL AND is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_project_event_cursor ON project ((COALESCE(scheduled_date, due_date)) DESC, id DESC) WHERE is_deleted = FALSE;

CREATE INDEX IF NOT EXISTS idx_project_type_value ON project_type (value);
CREATE INDEX IF NOT EXISTS idx_client_type      ON client(type_id);
CREATE INDEX IF NOT EXISTS idx_client_state     ON client(state_id);
CREATE INDEX IF NOT EXISTS idx_client_rate      ON client_rate(client_id);
CREATE INDEX IF NOT EXISTS idx_quote_client     ON quote(client_id);
CREATE INDEX IF NOT EXISTS idx_quote_project    ON quote(project_id);
CREATE INDEX IF NOT EXISTS idx_quote_status     ON quote(status_id);
CREATE INDEX IF NOT EXISTS idx_quote_file       ON quote(file_id);
CREATE INDEX IF NOT EXISTS idx_inv_client       ON invoice(client_id);
CREATE INDEX IF NOT EXISTS idx_inv_status       ON invoice(status_id);
CREATE INDEX IF NOT EXISTS idx_inv_file         ON invoice(file_id);
CREATE INDEX IF NOT EXISTS idx_inv_issuance     ON invoice(issuance_date);
CREATE INDEX IF NOT EXISTS idx_msg_sender       ON message(sender_id);
CREATE INDEX IF NOT EXISTS idx_msg_project      ON message(project_id);
CREATE INDEX IF NOT EXISTS idx_msg_attachment   ON message(file_attachment_id);
CREATE INDEX IF NOT EXISTS idx_doc_project      ON document(project_id);
CREATE INDEX IF NOT EXISTS idx_msg_mention_user   ON message_mention(user_email);
CREATE INDEX IF NOT EXISTS idx_magiclink_email    ON magic_link(user_email);
CREATE INDEX IF NOT EXISTS idx_jwt_user           ON jwt_token(user_email);
CREATE INDEX IF NOT EXISTS idx_casbin_subject_dom ON casbin_rule(v0, v1);
CREATE INDEX IF NOT EXISTS idx_notification_cursor ON notification (created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_status_proj_category_value ON status (category, value);
CREATE INDEX IF NOT EXISTS idx_project_trade_value ON project_trade (value);
CREATE INDEX IF NOT EXISTS idx_pay_term_value ON pay_term (value);
CREATE INDEX IF NOT EXISTS idx_message_project_created_at ON message(project_id, created_at DESC) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_quote_project_created_at ON quote(project_id, created_at DESC) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_document_project_created_at ON document(project_id, created_at DESC) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_client_cursor ON client (created_at DESC, id DESC) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_invoice_client_created_at ON invoice(client_id, created_at DESC) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_document_client_created_at ON document(client_id, created_at DESC) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_project_client_created_at ON project(client_id, created_at DESC) WHERE is_deleted = FALSE;


-- GIN indexes on search_text
CREATE INDEX IF NOT EXISTS idx_project_search_text   ON project   USING GIN (search_text gin_trgm_ops) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_client_search_text    ON client    USING GIN (search_text gin_trgm_ops) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_document_search_text  ON document  USING GIN (search_text gin_trgm_ops) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_quote_search_text     ON quote     USING GIN (search_text gin_trgm_ops) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_invoice_search_text   ON invoice   USING GIN (search_text gin_trgm_ops) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_message_search_text   ON message   USING GIN (search_text gin_trgm_ops) WHERE is_deleted = FALSE;
"""


# ──────────────────────────────────────────────────────────────────────────────
# 18: FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────

FUNCTIONS = """
CREATE OR REPLACE FUNCTION projects_refresh_search_text() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.search_text := array_to_string(ARRAY[
    COALESCE(NEW.business_name,''), COALESCE(NEW.address,''), COALESCE(NEW.address_line1,''), 
    COALESCE(NEW.address_line2,''), COALESCE(NEW.city,''), COALESCE(NEW.zip_code,''), 
    COALESCE(NEW.nte::text,''), COALESCE(NEW.due_date::text,''), COALESCE(NEW.date_received::text,''), 
    COALESCE(NEW.scope_of_work,''), COALESCE(NEW.special_notes,''), COALESCE(NEW.visit_notes,''), 
    COALESCE(NEW.planned_resolution,''), COALESCE(NEW.material_parts_needed,''),

    COALESCE((SELECT u.first_name||' '||u.last_name FROM "user" u WHERE u.id=NEW.client_id AND NOT u.is_deleted), ''),
    COALESCE((SELECT u2.first_name||' '||u2.last_name FROM "user" u2 WHERE u2.id=NEW.assignee_id AND NOT u2.is_deleted), ''),
    COALESCE((SELECT pp.value FROM project_priority pp WHERE pp.id=NEW.priority_id), ''),
    COALESCE((SELECT pt.value FROM project_type pt WHERE pt.id=NEW.type_id), ''),
    COALESCE((SELECT st.value FROM status st WHERE st.id=NEW.status_id AND st.category='project'), ''),
    COALESCE((SELECT tr.value FROM project_trade tr WHERE tr.id=NEW.trade_id), ''),
    COALESCE((SELECT s.name FROM state WHERE s.id=NEW.state_id), '')
  ], ' ');
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION client_refresh_search_text() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.search_text := array_to_string(ARRAY[
    COALESCE(NEW.company_name,''), COALESCE(NEW.poc_first_name,''), COALESCE(NEW.poc_last_name,''), 
    COALESCE(NEW.address_line_1,''), COALESCE(NEW.address_line_2,''), COALESCE(NEW.city,''), 
    COALESCE(NEW.zip_code,'')
  ], ' ');
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION document_refresh_search_text() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.search_text := array_to_string(ARRAY[
    COALESCE(NEW.file_name,''), COALESCE(NEW.file_extension,'')
  ], ' ');
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION quote_refresh_search_text() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.search_text := NEW.amount::text;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION invoice_refresh_search_text() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.search_text := array_to_string(ARRAY[
    NEW.amount::text, NEW.issuance_date::text
  ], ' ');
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION message_refresh_search_text() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.search_text := COALESCE(NEW.content,'');
  RETURN NEW;
END;
$$;
"""

# ──────────────────────────────────────────────────────────────────────────────
# 19: TRIGGERS
# ──────────────────────────────────────────────────────────────────────────────

TRIGGERS = """-- 1) Project: split INSERT vs. UPDATE so we don’t reference OLD in INSERT
DROP TRIGGER IF EXISTS trg_project_refresh_search_text_insert ON project;
DROP TRIGGER IF EXISTS trg_project_refresh_search_text_update ON project;

-- Always refresh on INSERT
CREATE TRIGGER trg_project_refresh_search_text_insert
  BEFORE INSERT ON project
  FOR EACH ROW
  EXECUTE FUNCTION projects_refresh_search_text();

-- Only refresh on UPDATE when relevant columns actually change
CREATE TRIGGER trg_project_refresh_search_text_update
  BEFORE UPDATE ON project
  FOR EACH ROW
  WHEN (
       NEW.business_name         IS DISTINCT FROM OLD.business_name
    OR NEW.address               IS DISTINCT FROM OLD.address
    OR NEW.address_line1         IS DISTINCT FROM OLD.address_line1
    OR NEW.address_line2         IS DISTINCT FROM OLD.address_line2
    OR NEW.city                  IS DISTINCT FROM OLD.city
    OR NEW.zip_code              IS DISTINCT FROM OLD.zip_code
    OR NEW.nte                   IS DISTINCT FROM OLD.nte
    OR NEW.due_date              IS DISTINCT FROM OLD.due_date
    OR NEW.date_received         IS DISTINCT FROM OLD.date_received
    OR NEW.scope_of_work         IS DISTINCT FROM OLD.scope_of_work
    OR NEW.special_notes         IS DISTINCT FROM OLD.special_notes
    OR NEW.visit_notes           IS DISTINCT FROM OLD.visit_notes
    OR NEW.planned_resolution    IS DISTINCT FROM OLD.planned_resolution
    OR NEW.material_parts_needed IS DISTINCT FROM OLD.material_parts_needed
    OR NEW.client_id             IS DISTINCT FROM OLD.client_id
    OR NEW.assignee_id           IS DISTINCT FROM OLD.assignee_id
    OR NEW.priority_id           IS DISTINCT FROM OLD.priority_id
    OR NEW.type_id               IS DISTINCT FROM OLD.type_id
    OR NEW.status_id             IS DISTINCT FROM OLD.status_id
    OR NEW.trade_id              IS DISTINCT FROM OLD.trade_id
    OR NEW.state_id              IS DISTINCT FROM OLD.state_id
  )
  EXECUTE FUNCTION projects_refresh_search_text();


-- 2) Client
DROP TRIGGER IF EXISTS trg_client_refresh_search_text ON client;
CREATE TRIGGER trg_client_refresh_search_text
  BEFORE INSERT OR UPDATE ON client
  FOR EACH ROW
  EXECUTE FUNCTION client_refresh_search_text();

-- 3) Document
DROP TRIGGER IF EXISTS trg_document_refresh_search_text ON document;
CREATE TRIGGER trg_document_refresh_search_text
  BEFORE INSERT OR UPDATE ON document
  FOR EACH ROW
  EXECUTE FUNCTION document_refresh_search_text();

-- 4) Quote
DROP TRIGGER IF EXISTS trg_quote_refresh_search_text ON quote;
CREATE TRIGGER trg_quote_refresh_search_text
  BEFORE INSERT OR UPDATE ON quote
  FOR EACH ROW
  EXECUTE FUNCTION quote_refresh_search_text();

-- 5) Invoice
DROP TRIGGER IF EXISTS trg_invoice_refresh_search_text ON invoice;
CREATE TRIGGER trg_invoice_refresh_search_text
  BEFORE INSERT OR UPDATE ON invoice
  FOR EACH ROW
  EXECUTE FUNCTION invoice_refresh_search_text();

-- 6) Message
DROP TRIGGER IF EXISTS trg_message_refresh_search_text ON message;
CREATE TRIGGER trg_message_refresh_search_text
  BEFORE INSERT OR UPDATE ON message
  FOR EACH ROW
  EXECUTE FUNCTION message_refresh_search_text();

"""


def preprocess_sql(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not re.match(r'^\s*-{3,}', line))


def split_sql(sql: str) -> list[str]:
    stmts, buf, in_dollar = [], [], False
    i = 0
    while i < len(sql):
        if sql[i:i + 2] == "$$":
            in_dollar = not in_dollar
            buf.append("$$")
            i += 2
        elif sql[i] == ";" and not in_dollar:
            stmt = "".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf = []
            i += 1
        else:
            buf.append(sql[i])
            i += 1
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


async def execute_block(conn, name: str, sql: str):
    print(f"\n--- Executing {name} ---")
    raw = preprocess_sql(sql)
    for stmt in split_sql(raw):
        s = stmt.lstrip()
        if not s or s.upper() in ("BEGIN", "COMMIT") or s.startswith("--"):
            continue
        await conn.execute(stmt + ";")
        print(f"✅ {s.splitlines()[0]}")


async def create_tables():
    conn = await asyncpg.connect(ASYNCPG_URL)
    try:
        for name, sql in [
            ("extensions", EXTENSIONS),
            ("casbin_rule", CASBIN_RULE),
            ("status", STATUS),
            ("state", STATE),
            ("client_type", CLIENT_TYPE),
            ("pay_term", PAY_TERM),
            ("client", CLIENT),
            ("client_rate", CLIENT_RATE),
            ("user", USER),
            ("account_manager_client", ACCOUNT_MANAGER_CLIENT),
            ("client_admin_technician", CLIENT_ADMIN_TECHNICIAN),
            ("project_priority", PROJECT_PRIORITY),
            ("project_type", PROJECTS_TYPE),
            ("project_trade", PROJECTS_TRADE),
            ("project", PROJECT),
            ("document", DOCUMENT),
            ("quote", QUOTE),
            ("invoice", INVOICE),
            ("message", MESSAGE),
            ("message_mention", MESSAGE_MENTION),
            ("magic_link", MAGIC_LINK),
            ("magic_link_listen_notify", MAGIC_LINK_LISTEN_NOTIFY),
            ("client_password", CLIENT_PASSWORD),
            ("user_recovery_params", USER_RECOVERY_PARAMS),
            ("user_key", USER_KEY),
            ("jwt_token", JWT_TOKEN),
            ("insurance", INSURANCE),
            ("notification", NOTIFICATION),
            ("notification_listen_notify", NOTIFICATION_LISTEN_NOTIFY),
            ("alter", ALTERS),
            ("views", VIEWS),
            ("prepares", PREPARES),
            ("indices", INDICES),
            ("functions", FUNCTIONS),
            ("triggers", TRIGGERS),
        ]:
            await execute_block(conn, name, sql)

        if await conn.fetchval("SELECT count(*) FROM casbin_rule") == 0:
            await conn.executemany(
                """
                INSERT INTO casbin_rule (ptype, v0, v1, v2, v3)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT DO NOTHING;
                """,
                [
                    ("p", "employee_admin", "*", "*", "*"),

                    ("p", "employee_account_manager", "*", "/get-messages", "*"),
                    ("p", "employee_account_manager", "*", "/send-message", "*"),
                    ("p", "employee_account_manager", "*", "/global-search", "*"),
                    ("p", "employee_account_manager", "*", "/get-notifications", "*"),
                    ("p", "employee_account_manager", "*", "/get-profile-details", "*"),
                    ("p", "employee_account_manager", "*", "/get-dashboard-metrics", "*"),
                    ("p", "employee_account_manager", "*", "/get-calendar-events", "*"),
                    ("p", "employee_account_manager", "*", "/get-projects", "*"),
                    ("p", "employee_account_manager", "*", "/get-project-statuses", "*"),
                    ("p", "employee_account_manager", "*", "/get-project-types", "*"),
                    ("p", "employee_account_manager", "*", "/get-project-trades", "*"),
                    ("p", "employee_account_manager", "*", "/get-project-priorities", "*"),
                    ("p", "employee_account_manager", "*", "/get-project-assessments", "*"),
                    ("p", "employee_account_manager", "*", "/get-states", "*"),
                    ("p", "employee_account_manager", "*", "/create-new-project", "*"),
                    ("p", "employee_account_manager", "*", "/fetch-project", "*"),
                    ("p", "employee_account_manager", "*", "/fetch-project-quotes", "*"),
                    ("p", "employee_account_manager", "*", "/fetch-project-documents", "*"),
                    ("p", "employee_account_manager", "*", "/get-clients", "*"),
                    ("p", "employee_account_manager", "*", "/get-pay-terms", "*"),
                    ("p", "employee_account_manager", "*", "/create-new-client", "*"),
                    ("p", "employee_account_manager", "*", "/fetch-client", "*"),
                    ("p", "employee_account_manager", "*", "/fetch-client-invoices", "*"),
                    ("p", "employee_account_manager", "*", "/fetch-client-onboarding-documents", "*"),
                    ("p", "employee_account_manager", "*", "/get-client-statuses", "*"),
                    ("p", "employee_account_manager", "*", "/get-insurance-documents", "*"),
                    ("p", "employee_account_manager", "*", "/fetch-client-projects", "*"),
                    ("p", "employee_account_manager", "*", "/get-billings", "*"),
                    ("p", "employee_account_manager", "*", "/get-billing-statuses", "*"),
                    ("p", "employee_account_manager", "*", "/get-invoice-statuses", "*"),
                    ("p", "employee_account_manager", "*", "/get-quote-statuses", "*"),
                    ("p", "employee_account_manager", "*", "/get-passwords", "*"),
                    ("p", "employee_account_manager", "*", "/create-new-invoice", "*"),
                    ("p", "employee_account_manager", "*", "/create-new-quote", "*"),
                    ("p", "employee_account_manager", "*", "/get-invoice", "*"),
                    ("p", "employee_account_manager", "*", "/get-quote", "*"),
                    ("p", "employee_account_manager", "*", "/get-onboarding-data", "*"),
                    ("p", "employee_account_manager", "*", "/update-insurance-data", "*"),

                    ("p", "client_admin", "*", "/get-messages", "*"),
                    ("p", "client_admin", "*", "/send-message", "*"),
                    ("p", "client_admin", "*", "/get-notifications", "*"),
                    ("p", "client_admin", "*", "/save-onboarding-data", "*"),
                    ("p", "client_admin", "*", "/get-onboarding-data", "*"),
                    ("p", "client_admin", "*", "/update-insurance-data", "*"),
                    ("p", "client_admin", "*", "/save-onboarding-data", "*"),
                    ("p", "client_admin", "*", "/get-profile-details", "*"),
                    ("p", "client_admin", "*", "/get-states", "*"),
                    ("p", "client_admin", "*", "/fetch-project", "*"),
                    ("p", "client_admin", "*", "/fetch-project-quotes", "*"),
                    ("p", "client_admin", "*", "/fetch-project-documents", "*"),
                    ("p", "client_admin", "*", "/fetch-client", "*"),
                    ("p", "client_admin", "*", "/fetch-client-invoices", "*"),
                    ("p", "client_admin", "*", "/fetch-client-onboarding-documents", "*"),
                    ("p", "client_admin", "*", "/get-client-statuses", "*"),
                    ("p", "client_admin", "*", "/get-insurance-documents", "*"),
                    ("p", "client_admin", "*", "/fetch-client-projects", "*"),
                    ("p", "client_admin", "*", "/get-billings", "*"),
                    ("p", "client_admin", "*", "/get-billing-statuses", "*"),
                    ("p", "client_admin", "*", "/get-invoice-statuses", "*"),
                    ("p", "client_admin", "*", "/get-quote-statuses", "*"),
                    ("p", "client_admin", "*", "/get-passwords", "*"),
                    ("p", "client_admin", "*", "/get-invoice", "*"),
                    ("p", "client_admin", "*", "/get-quote", "*"),
                    ("p", "client_admin", "*", "/get-onboarding-data", "*"),
                    ("p", "client_admin", "*", "/update-insurance-data", "*"),

                    ("p", "client_technician", "*", "/get-messages", "*"),
                    ("p", "client_technician", "*", "/get-notifications", "*"),
                    ("p", "client_technician", "*", "/get-profile-details", "*"),
                    ("p", "client_technician", "*", "/get-states", "*"),
                    ("p", "client_technician", "*", "/fetch-project", "*"),
                    ("p", "client_technician", "*", "/fetch-project-quotes", "*"),
                    ("p", "client_technician", "*", "/fetch-project-documents", "*"),
                    ("p", "client_technician", "*", "/fetch-client", "*"),
                    ("p", "client_technician", "*", "/fetch-client-invoices", "*"),
                    ("p", "client_technician", "*", "/fetch-client-onboarding-documents", "*"),
                    ("p", "client_technician", "*", "/get-client-statuses", "*"),
                    ("p", "client_technician", "*", "/get-insurance-documents", "*"),
                    ("p", "client_technician", "*", "/fetch-client-projects", "*"),
                    ("p", "client_technician", "*", "/get-billings", "*"),
                    ("p", "client_technician", "*", "/get-billing-statuses", "*"),
                    ("p", "client_technician", "*", "/get-invoice-statuses", "*"),
                    ("p", "client_technician", "*", "/get-quote-statuses", "*"),
                    ("p", "client_technician", "*", "/get-passwords", "*"),
                    ("p", "client_technician", "*", "/get-invoice", "*"),
                    ("p", "client_technician", "*", "/get-quote", "*"),
                    ("p", "client_technician", "*", "/get-onboarding-data", "*"),
                    ("p", "client_technician", "*", "/update-insurance-data", "*"),

                    ("g", "client_technician", "client_admin", "*", "*"),
                ]
            )

        if await conn.fetchval("SELECT count(*) FROM state") == 0:
            await conn.executemany(
                "INSERT INTO state (name) VALUES ($1) ON CONFLICT DO NOTHING;",
                [(s,) for s in US_STATES],
            )

        if await conn.fetchval("SELECT count(*) FROM project_priority") == 0:
            await conn.executemany(
                "INSERT INTO project_priority (value, color) VALUES ($1, $2);",
                [
                    ("P1 - Emergency", "red-500"),
                    ("P2 - Same Day", "orange-500"),
                    ("P3 - Standard", "yellow-500"),
                ],
            )

        if await conn.fetchval("SELECT count(*) FROM project_type") == 0:
            await conn.executemany(
                "INSERT INTO project_type (value) VALUES ($1);",
                [("Repair",), ("Installation",)],
            )

        if await conn.fetchval("SELECT count(*) FROM project_trade") == 0:
            await conn.executemany(
                "INSERT INTO project_trade (value) VALUES ($1);",
                [("Electrical",), ("Plumbing",)],
            )

        if await conn.fetchval("SELECT count(*) FROM client_type") == 0:
            await conn.executemany(
                "INSERT INTO client_type (value) VALUES ($1);",
                [("Retail",), ("Corporate",)],
            )

        if await conn.fetchval("SELECT count(*) FROM pay_term") == 0:
            await conn.executemany(
                "INSERT INTO pay_term (value) VALUES ($1);",
                [("Net 30",), ("Net 60",)],
            )

        if await conn.fetchval("SELECT count(*) FROM status WHERE category='project'") == 0:
            await conn.executemany(
                "INSERT INTO status (category, value, color) VALUES ($1,$2,$3);",
                [
                    ("project", "Open", "green-500"),
                    ("project", "Delayed", "yellow-500"),
                ],
            )

        if await conn.fetchval("SELECT count(*) FROM status WHERE category='quote'") == 0:
            await conn.executemany(
                "INSERT INTO status (category, value, color) VALUES ($1,$2,$3);",
                [
                    ("quote", "Pending", "yellow-500"),
                    ("quote", "Approved", "green-500"),
                ],
            )

        if await conn.fetchval("SELECT count(*) FROM status WHERE category='invoice'") == 0:
            await conn.executemany(
                "INSERT INTO status (category, value, color) VALUES ($1,$2,$3);",
                [
                    ("invoice", "Sent", "blue-500"),
                    ("invoice", "Paid", "green-500"),
                ],
            )

        if await conn.fetchval("SELECT count(*) FROM status WHERE category='client'") == 0:
            await conn.executemany(
                "INSERT INTO status (category, value, color) VALUES ($1,$2,$3);",
                [
                    ("client", "Active", "green-500"),
                    ("client", "Inactive", "red-500"),
                ],
            )

        # Moved outside the loop
        print("\n🎉 Schema creation complete.")
    finally:
        await conn.close()


async def reset_schema():
    conn = await asyncpg.connect(ASYNCPG_URL)
    try:
        await conn.execute(RESET_SCHEMA)
        print("🗑️ Public schema reset successfully.")
    finally:
        await conn.close()


def print_usage():
    print("Usage:\n  python db_manager.py reset\n  python db_manager.py create")


if __name__ == "__main__":
    asyncio.run(reset_schema())
    asyncio.run(create_tables())
