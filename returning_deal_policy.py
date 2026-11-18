"""Complete, read-only open-deal inventory for returning-client decisions.

Intake stages and unassigned deals are deliberately included. Unavailable,
truncated or changing data raises instead of authorizing a duplicate deal.
"""
from datetime import datetime, timezone

from contact_owner_policy import contact_ids as actual_contact_ids
from contact_owner_sync import entity_id


PAGE_SIZE = 50
MAX_DEALS_PER_CONTACT = 1000
READ_METHODS = frozenset({"crm.item.list", "crm.deal.get", "crm.deal.contact.items.get"})
ITEM_FIELDS = {"id": "ID", "categoryId": "CATEGORY_ID", "stageId": "STAGE_ID",
               "stageSemanticId": "STAGE_SEMANTIC_ID", "closed": "CLOSED",
               "createdTime": "DATE_CREATE"}


def _date(value):
    if not isinstance(value, str):
        raise ValueError("returning_deal_date_unavailable")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError()
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        raise ValueError("returning_deal_date_unavailable") from None


def _ids(values):
    if not isinstance(values, (list, tuple, set, frozenset)) or not values:
        raise ValueError("returning_contacts_required")
    ids = [entity_id(value) for value in values]
    if not all(ids) or len(ids) != len(set(ids)):
        raise ValueError("returning_contacts_invalid")
    return sorted(ids, key=int)


def _open_deal(row):
    if not isinstance(row, dict):
        raise ValueError("returning_deal_schema_unavailable")
    did = entity_id(row.get("ID"))
    category = entity_id(row.get("CATEGORY_ID"), allow_zero=True)
    stage = row.get("STAGE_ID")
    if (not did or not category or not isinstance(stage, str) or not stage or stage.strip() != stage
            or row.get("STAGE_SEMANTIC_ID") != "P" or row.get("CLOSED") != "N"):
        raise ValueError("returning_deal_schema_unavailable")
    _date(row.get("DATE_CREATE"))
    # Ownership does not decide whether a deal blocks creation. Bitrix can
    # return an unassigned deal; preserve its missing/null/zero value.
    owner = row.get("ASSIGNED_BY_ID")
    if owner is not None and not entity_id(owner, allow_zero=True):
        raise ValueError("returning_deal_owner_schema_unavailable")
    normalized = {**row, "ID": did, "CATEGORY_ID": category}
    if owner is not None:
        normalized["ASSIGNED_BY_ID"] = entity_id(owner, allow_zero=True)
    return normalized


def open_deals_for_contacts(call, contact_ids):
    """Return unique full legacy records plus actual ``CONTACT_IDS``.

    ``call`` accepts (method, flat_params, timeout=8) and unwraps ``result``.
    Universal contactIds filters cover secondary bindings. Short pages prove a
    complete list; a full page at the cap fails closed, including exactly1000.
    Fresh full records preserve origin fields and other saga proof metadata.
    """
    requested = _ids(contact_ids)

    def read(method, params, timeout=8):
        if method not in READ_METHODS:
            raise ValueError("returning_policy_write_forbidden")
        return call(method, params, timeout=8)

    candidates = {}
    for cid in requested:
        seen, complete = set(), False
        for start in range(0, MAX_DEALS_PER_CONTACT, PAGE_SIZE):
            response = read("crm.item.list", {
                "entityTypeId": 2, "filter[contactIds]": cid,
                "filter[stageSemanticId]": "P", "filter[closed]": "N",
                "select[]": [*ITEM_FIELDS, "contactIds"],
                "order[id]": "ASC", "start": start,
            })
            rows = response.get("items") if isinstance(response, dict) else None
            if not isinstance(rows, list) or len(rows) > PAGE_SIZE:
                raise ValueError("returning_deal_page_unavailable")
            for row in rows:
                if not isinstance(row, dict) or not isinstance(row.get("contactIds"), list):
                    raise ValueError("returning_deal_bindings_unavailable")
                bindings = _ids(row["contactIds"])
                if cid not in bindings:
                    raise ValueError("returning_deal_filter_unavailable")
                deal = _open_deal({target: row[source] for source, target in ITEM_FIELDS.items() if source in row})
                did = deal["ID"]
                if did in seen:
                    raise ValueError("returning_deal_page_repeated")
                seen.add(did)
                observation = {**deal, "CONTACT_IDS": bindings}
                old = candidates.get(did)
                if old is not None and old != observation:
                    raise ValueError("returning_deal_changed_between_contacts")
                candidates[did] = observation
            if len(rows) < PAGE_SIZE:
                complete = True
                break
        if not complete:
            raise ValueError("returning_deal_inventory_limit")

    result = []
    for did in sorted(candidates, key=int):
        selected = candidates[did]
        fresh = _open_deal(read("crm.deal.get", {"id": did}))
        _date(fresh.get("DATE_MODIFY"))
        if (fresh["ID"] != did or fresh["CATEGORY_ID"] != selected["CATEGORY_ID"]
                or fresh["STAGE_ID"] != selected["STAGE_ID"]
                or _date(fresh["DATE_CREATE"]) != _date(selected["DATE_CREATE"])):
            raise ValueError("returning_deal_changed_after_list")
        actual = sorted(actual_contact_ids(read, fresh), key=int)
        if actual != selected["CONTACT_IDS"]:
            raise ValueError("returning_deal_actual_bindings_changed")
        result.append({**fresh, "CONTACT_IDS": actual})
    return result
