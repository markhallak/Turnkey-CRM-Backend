from typing import List, Dict
from casbin_databases_adapter import DatabasesAdapter

class CasbinFilter:
    ptype: List[str] = []
    subject: List[str] = []
    domain: List[str] = []
    object: List[str] = []
    action: List[str] = []
    extra: List[str] = []


class CasbinAdapter(DatabasesAdapter):
    cols = ["ptype", "subject", "domain", "object", "action", "extra"]

    @staticmethod
    def _policy_to_dict(p_type: str, rule: List[str]) -> Dict[str, str]:
        keys = ["subject", "domain", "object", "action", "extra"]
        row: Dict[str, str] = {"ptype": p_type}
        for i, value in enumerate(rule):
            if i < len(keys):
                row[keys[i]] = value
        return row

    async def remove_policy(self, sec, p_type, rule):
        query = self.table.delete().where(self.table.columns.ptype == p_type)
        keys = ["subject", "domain", "object", "action", "extra"]
        for i, value in enumerate(rule):
            query = query.where(self.table.columns[keys[i]] == value)
        result = await self.db.execute(query)
        return True if result > 0 else False

    async def remove_filtered_policy(self, sec, p_type, field_index, *field_values):
        query = self.table.delete().where(self.table.columns.ptype == p_type)
        keys = ["subject", "domain", "object", "action", "extra"]
        for i, value in enumerate(field_values):
            if value:
                query = query.where(self.table.columns[keys[field_index + i]] == value)
        result = await self.db.execute(query)
        return True if result else False

