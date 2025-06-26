import casbin
import sqlalchemy
from databases import Database
from casbin_adapter import CasbinAdapter
from constants import DATABASE_URL
import pytest

def auto_enforce(enforcer, sub: str, obj: str, act: str) -> bool:
    grouping = enforcer.get_model().model["g"]["g"].policy
    domains = {rule[2] for rule in grouping if rule[0] == sub}
    for dom in domains:
        if enforcer.enforce(sub, dom, obj, act):
            return True
    return False

@pytest.fixture(scope="module")
def enforcer():
    database = Database(DATABASE_URL)
    metadata = sqlalchemy.MetaData()
    casbin_table = sqlalchemy.Table(
        "casbin_rule",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("ptype", sqlalchemy.String(255)),
        sqlalchemy.Column("subject", sqlalchemy.String(255)),
        sqlalchemy.Column("domain", sqlalchemy.String(255)),
        sqlalchemy.Column("object", sqlalchemy.String(255)),
        sqlalchemy.Column("action", sqlalchemy.String(255)),
        sqlalchemy.Column("extra", sqlalchemy.String(255)),
    )
    adapter = CasbinAdapter(database, casbin_table)
    e = casbin.Enforcer("model.conf", adapter)
    e.enable_auto_save(True)
    return e

@pytest.mark.parametrize(
    "sub, obj, act, expected",
    [
        # — Admin (alice) wildcard everything —
        ("alice", "*", "*", True),
        ("alice", "/any/route", "delete", True),
        # Extra alice cases
        ("alice", "/get-messages", "foo", True),
        ("alice", "/send-message", "bar", True),

        # — Bob (emp_acct_manager) —
        ("bob",   "/clients",         "read",  True),
        ("bob",   "/clients",         "delete",True),
        ("bob",   "/clients/1",       "read",  True),
        ("bob",   "/clients/1",       "write", True),
        ("bob",   "/projects",        "read",  True),
        ("bob",   "/projects",        "delete",True),
        ("bob",   "/projects/123",    "read",  True),
        ("bob",   "/projects/123/edit","post",  True),
        ("bob",   "/projects2",       "read",  False),
        ("bob",   "/send-message",    "read",  True),
        ("bob",   "/send-message",    "write", True),
        ("bob",   "/get-messages",    "read",  True),

        # — John (emp_acct_manager on client2) —
        ("john",  "/clients",         "read",  True),
        ("john",  "/clients",         "delete",True),
        ("john",  "/projects",        "write", True),
        ("john",  "/projects",        "delete",True),
        ("john",  "/projects/xyz",    "read",  True),
        ("john",  "/send-message",    "write", True),
        ("john",  "/get-messages",    "read",  True),

        # — Carol (client_admin) —
        ("carol", "/projects",        "read",   True),
        ("carol", "/projects",        "delete", True),
        ("carol", "/projects/42",     "read",   False),
        ("carol", "/projects/42",     "write",  False),
        ("carol", "/send-message",    "read",   True),
        ("carol", "/send-message",    "write",  True),
        ("carol", "/send-message/1",  "read",   False),
        ("carol", "/get-messages",    "read",   False),

        # — Dave (client_technician) —
        ("dave",  "/projects",         "read_without_financial", True),
        ("dave",  "/projects",         "read",   False),
        ("dave",  "/projects/view/1",  "read_without_financial", True),
        ("dave",  "/projects/view/1",  "read",   False),
        ("dave",  "/projects/1",       "read_without_financial", False),
        ("dave",  "/send-message",     "read",   False),
        ("dave",  "/send-message",     "write",  False),
        ("dave",  "/get-messages",     "read",   True),
        ("dave",  "/get-messages",     "delete", True),

        # — Unknown user —
        ("eve",   "/projects",         "read",   False),
        ("eve",   "/anything",         "foo",    False),
    ],
)
def test_policy(enforcer, sub, obj, act, expected):
    result = auto_enforce(enforcer, sub, obj, act)
    assert result is expected, f"{sub!r} on {obj!r}#{act!r} expected={expected}, got={result}"
