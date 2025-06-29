# tests/test_casbin_from_db.py
import asyncio

import asyncpg
import casbin
import pytest
from casbin.persist.adapters import StringAdapter

from constants import ASYNCPG_URL

MODEL_CONF = "model.conf"             # uses the model you showed

# ──────────────────────────── FIXTURE ────────────────────────────────────────
@pytest.fixture(scope="module")
def enforcer():
    """
    1. Wipe & recreate the schema (via DbManager helpers).
    2. Read all rows from casbin_rule.
    3. Translate them into Casbin policy / grouping lines.
    4. Build a SyncedEnforcer backed by an in-memory StringAdapter.
    5. Return the enforcer *and* the UUID of our seeded admin user.
    """
    loop = asyncio.get_event_loop()
    # Step 2 – pull rows
    conn = loop.run_until_complete(asyncpg.connect(ASYNCPG_URL))
    rows = loop.run_until_complete(
        conn.fetch(
            "SELECT ptype, subject, domain, object, action "
            "FROM casbin_rule"
        )
    )

    # Step 3 – translate to plain-text policy lines
    policy_lines = []
    admin_uuid = None

    for r in rows:
        if r["ptype"] == "p":
            # p, sub, dom, obj, act   (matches your model)
            line = f"p, {r['subject']}, {r['domain']}, {r['object']}, {r['action']}"
        elif r["ptype"] == "g":
            # g = _, _, _   → keep first three fields (user, role, domain)
            sub = r["subject"]
            role = r["domain"]
            dom  = r["object"] or "*"        # default “*” if NULL / empty
            line = f"g, {sub}, {role}, {dom}"
            if role == "employee_admin":     # remember our admin user
                admin_uuid = sub
                print(admin_uuid)
        else:
            continue
        policy_lines.append(line)

    loop.run_until_complete(conn.close())

    policy_text = "\n".join(policy_lines)
    adapter = StringAdapter(policy_text)
    e = casbin.Enforcer(MODEL_CONF, adapter)

    # expose the seeded admin UUID so tests can reference it
    e.admin_uuid = admin_uuid
    return e


# ──────────────────────────── TESTS ──────────────────────────────────────────
def test_seeded_admin_has_global_wildcard(enforcer):
    """The seeded employee_admin UUID should be able to do anything, anywhere."""
    assert enforcer.enforce(enforcer.admin_uuid, "*", "*", "*")


@pytest.mark.parametrize(
    "obj, act",
    [
        ("/clients",        "read"),
        ("/clients/123",    "delete"),
        ("/projects",       "post"),
        ("/auth/invite", "post")
    ],
)
def test_admin_misc_cases(enforcer, obj, act):
    # Extra sanity-checks using a few random routes/actions
    assert enforcer.enforce(enforcer.admin_uuid, "*", obj, act)
