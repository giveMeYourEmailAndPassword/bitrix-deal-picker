"""Process-local claim fences; durable SQLite leases remain authoritative."""

from contextlib import contextmanager
import threading


class ClaimLocks:
    """Serialize one employee's quota/policy and one deal's remote mutation.

    Every compound holder takes sorted manager locks before the deal lock. Admin
    policy updates take only the manager lock. Never nest ``hold`` contexts;
    callers provide their already validated, normalized server-side identities.
    Waiting holders keep references too, so an idle-looking lock cannot be
    replaced while another thread is about to acquire it.
    """

    def __init__(self):
        self._registry_lock = threading.Lock()
        self._entries = {}

    @contextmanager
    def hold(self, manager_id, deal_id=None, *, additional_manager_ids=()):
        manager_id = str(manager_id or "").strip()
        deal_id = str(deal_id or "").strip() if deal_id is not None else None
        managers = {manager_id, *(str(value or "").strip() for value in additional_manager_ids)}
        if "" in managers or deal_id == "":
            raise ValueError("Claim lock identity is required")
        keys = [("manager", value) for value in sorted(managers)]
        if deal_id is not None:
            keys.append(("deal", deal_id))
        with self._registry_lock:
            entries = []
            for key in keys:
                entry = self._entries.setdefault(key, [threading.Lock(), 0])
                entry[1] += 1
                entries.append((key, entry))
        acquired = []
        try:
            for _key, entry in entries:
                entry[0].acquire()
                acquired.append(entry[0])
            yield
        finally:
            for lock in reversed(acquired):
                lock.release()
            with self._registry_lock:
                for key, entry in entries:
                    entry[1] -= 1
                    if not entry[1]:
                        del self._entries[key]


@contextmanager
def hold_claim_operation(registry, manager_id, deal_id, read_operation_manager):
    """A claimant may reconcile an older attempt owned by another employee.

    Fence both quotas before taking the deal lock. Re-read its owner after
    acquisition: a retry may have changed the operation while we waited. If
    that owner was not fenced, release everything before acquiring a new set;
    never take a manager lock from inside a deal critical section.
    """
    operation_manager = read_operation_manager()
    while True:
        additional = {operation_manager} - {manager_id, "", None}
        with registry.hold(manager_id, deal_id, additional_manager_ids=additional):
            current_manager = read_operation_manager()
            if current_manager and current_manager not in additional | {manager_id}:
                operation_manager = current_manager
                continue
            yield
            return
