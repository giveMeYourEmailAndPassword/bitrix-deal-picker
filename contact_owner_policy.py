"""Read-only policy for the owner of a contact's current active deal.

An incomplete candidate list or uncertain binding is an error, never permission
to choose an older deal. Callers own durable jobs and all CRM mutations.
"""

import re
from datetime import datetime, timezone


EXCLUDED_STAGES = frozenset({"UC_ZJ55BR", "UC_PUCAAQ"})
PAGE_SIZE = 50
MAX_DEALS = 1000
ITEM_FIELDS = {"id": "ID", "assignedById": "ASSIGNED_BY_ID", "contactId": "CONTACT_ID",
               "categoryId": "CATEGORY_ID", "stageId": "STAGE_ID",
               "stageSemanticId": "STAGE_SEMANTIC_ID", "closed": "CLOSED",
               "createdTime": "DATE_CREATE"}


def _entity_id(value):
    if isinstance(value, bool):
        return ""
    text = str(value if value is not None else "")
    return text if re.fullmatch(r"[1-9]\d{0,14}", text) else ""


def _stage(value):
    return value if isinstance(value, str) and value and value.strip() == value else ""


def eligible_deal(deal):
    """Only open, in-progress deals with a valid assigned employee qualify."""
    if not isinstance(deal, dict):
        return False
    stage = _stage(deal.get("STAGE_ID"))
    return bool(_entity_id(deal.get("ID"))
                and _entity_id(deal.get("ASSIGNED_BY_ID"))
                and stage and stage.rsplit(":", 1)[-1] not in EXCLUDED_STAGES
                and deal.get("CLOSED") == "N"
                and deal.get("STAGE_SEMANTIC_ID") == "P")


def contact_ids(call, deal):
    """Return all actual bindings; never infer them from legacy CONTACT_ID."""
    deal_id = _entity_id(deal.get("ID")) if isinstance(deal, dict) else ""
    if not deal_id:
        raise ValueError("contact_owner_invalid_deal")
    rows = call("crm.deal.contact.items.get", {"id": deal_id}, timeout=8)
    if not isinstance(rows, list):
        raise ValueError("contact_owner_bindings_unavailable")
    ids = []
    for row in rows:
        value = _entity_id(row.get("CONTACT_ID")) if isinstance(row, dict) else ""
        if not value or value in ids:
            raise ValueError("contact_owner_bindings_invalid")
        ids.append(value)
    return ids


def _created_at(value):
    if not isinstance(value, str):
        raise ValueError("contact_owner_creation_date_invalid")
    try:
        created = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise ValueError("contact_owner_creation_date_invalid") from None
    if created.tzinfo is None or created.utcoffset() is None:
        raise ValueError("contact_owner_creation_date_invalid")
    return created.astimezone(timezone.utc)


def _candidate(row):
    if (not isinstance(row, dict) or not _entity_id(row.get("ID"))
            or not _entity_id(row.get("ASSIGNED_BY_ID"))
            or not _stage(row.get("STAGE_ID"))
            or row.get("CLOSED") not in ("Y", "N")
            or row.get("STAGE_SEMANTIC_ID") not in ("P", "S", "F")):
        raise ValueError("contact_owner_deal_schema_invalid")
    created = _created_at(row.get("DATE_CREATE"))
    return {**row, "ID": _entity_id(row["ID"]),
            "ASSIGNED_BY_ID": _entity_id(row["ASSIGNED_BY_ID"])}, created


def _item_candidate(row, cid):
    if not isinstance(row, dict) or not isinstance(row.get("contactIds"), list):
        raise ValueError("contact_owner_deal_bindings_invalid")
    ids = [_entity_id(value) for value in row["contactIds"]]
    if not all(ids) or len(ids) != len(set(ids)) or cid not in ids:
        raise ValueError("contact_owner_deal_bindings_invalid")
    normalized = {target: row[source] for source, target in ITEM_FIELDS.items() if source in row}
    normalized["CONTACT_IDS"] = ids
    return _candidate(normalized)


def latest_eligible_deal(call, contact_id):
    """Read the complete bounded active set and return its newest eligible deal.

    contactIds filtering covers every bound contact, including secondary ones.
    The API wrapper returns only the result object, without `next`. A short page
    proves completion. A full page at the hard limit fails closed, including the
    exact-limit case: no potentially truncated result can authorize a write.
    """
    cid = _entity_id(contact_id)
    if not cid:
        raise ValueError("contact_owner_invalid_contact")
    candidates, seen = [], set()
    complete = False
    for start in range(0, MAX_DEALS, PAGE_SIZE):
        result = call("crm.item.list", {
            "entityTypeId": 2,
            "filter[contactIds]": cid,
            "filter[stageSemanticId]": "P",
            "filter[closed]": "N",
            "select[]": [*ITEM_FIELDS, "contactIds"],
            "order[createdTime]": "DESC",
            "order[id]": "DESC",
            "start": start,
        }, timeout=8)
        rows = result.get("items") if isinstance(result, dict) else None
        if not isinstance(rows, list) or len(rows) > PAGE_SIZE:
            raise ValueError("contact_owner_deal_page_invalid")
        for row in rows:
            deal, created = _item_candidate(row, cid)
            if deal["ID"] in seen:
                raise ValueError("contact_owner_deal_page_repeated")
            seen.add(deal["ID"])
            if eligible_deal(deal):
                candidates.append((created, int(deal["ID"]), deal))
        if len(rows) < PAGE_SIZE:
            complete = True
            break
    if not complete:
        raise ValueError("contact_owner_deal_limit")
    if not candidates:
        return None
    selected_created, _, winner = max(candidates, key=lambda item: (item[0], item[1]))
    # The full record also carries the caller's dynamic claim marker and
    # DATE_MODIFY; those must not disappear through the list's narrow select.
    fresh, fresh_created = _candidate(call("crm.deal.get", {"id": winner["ID"]}, timeout=8))
    if (not eligible_deal(fresh) or fresh_created != selected_created
            or any(fresh.get(field) != winner.get(field) for field in
                   ("ID", "ASSIGNED_BY_ID", "STAGE_ID", "STAGE_SEMANTIC_ID", "CLOSED"))):
        raise ValueError("contact_owner_winner_changed")
    if cid not in contact_ids(call, fresh):
        raise ValueError("contact_owner_winner_unlinked")
    return fresh
