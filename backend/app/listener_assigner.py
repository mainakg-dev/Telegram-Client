"""
Listener Assigner — Smart group-to-listener account assignment engine.

Responsibilities:
- Auto-picks the healthiest/oldest accounts as LISTENER accounts
- Distributes target groups evenly across listener accounts (max 35 per listener)
- Handles failover: if a listener account dies, reassigns its groups to healthy ones
- Stores assignments in Redis for all workers to read
"""

import os
import math
import logging
from typing import List, Dict, Optional

from .queue_manager import queue_manager, MAX_GROUPS_PER_LISTENER

logger = logging.getLogger("ListenerAssigner")

SESSIONS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sessions")


def _get_all_accounts_from_db() -> List[Dict]:
    """
    Reads accounts from local SQLite database.
    Returns list of account dicts with id, phone, session_name, role, status, server_group, created_at.
    """
    import sqlite3
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "app.db")
    if not os.path.exists(db_path):
        return []

    accounts = []
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, phone, session_name, role, status, server_group, created_at "
            "FROM accounts ORDER BY id ASC"
        )
        for row in cursor.fetchall():
            accounts.append(dict(row))
        conn.close()
    except Exception as e:
        logger.error(f"Error reading accounts from DB: {e}")
    return accounts


def _get_healthy_accounts(accounts: List[Dict]) -> List[Dict]:
    """Filter accounts that have a valid session file and are not banned/unauthorized."""
    healthy = []
    for acc in accounts:
        sname = acc.get("session_name", "")
        status = acc.get("status", "")
        if status in ("UNAUTHORIZED", "DISABLED"):
            continue
        session_file = os.path.join(SESSIONS_DIR, f"{sname}.session")
        if os.path.exists(session_file):
            healthy.append(acc)
    return healthy


def _calculate_needed_listeners(num_groups: int) -> int:
    """Calculate how many listener accounts are needed for the given number of groups."""
    if num_groups == 0:
        return 0
    return max(1, math.ceil(num_groups / MAX_GROUPS_PER_LISTENER))


async def auto_assign_listeners(targets: Optional[List[str]] = None, force_rebalance: bool = False) -> Dict[str, List[str]]:
    """
    Main assignment function. Auto-picks listener accounts and distributes groups.

    Algorithm:
    1. Get all active target groups
    2. Calculate how many listeners we need (ceil(groups / 35))
    3. Pick the healthiest/oldest accounts as listeners
    4. Distribute groups evenly (round-robin assignment)
    5. Store in Redis

    Args:
        targets: Optional list of target group strings. If None, reads from Redis.
        force_rebalance: If True, recalculates even if existing assignments look valid.

    Returns:
        Dict[session_name, List[group_targets]] — the assignment map
    """
    # 1. Get target groups
    if targets is None:
        targets = await queue_manager.get_active_targets()

    if not targets:
        logger.info("No active targets — clearing listener assignments")
        await queue_manager.set_listener_assignments({})
        return {}

    num_groups = len(targets)
    needed_listeners = _calculate_needed_listeners(num_groups)

    # 2. Get all healthy accounts
    all_accounts = _get_all_accounts_from_db()
    healthy_accounts = _get_healthy_accounts(all_accounts)

    if not healthy_accounts:
        logger.error("❌ No healthy accounts available for listener assignment!")
        return {}

    # 3. Check existing assignments — skip rebalance if still valid
    if not force_rebalance:
        existing = await queue_manager.get_listener_assignments()
        if existing and _is_assignment_valid(existing, targets, healthy_accounts):
            logger.debug("Existing listener assignments are valid — skipping rebalance")
            return existing

    # 4. Pick listener accounts (oldest/first N healthy accounts)
    # Accounts that already have role='LISTENER' in DB get priority
    listener_candidates = sorted(healthy_accounts, key=lambda a: (
        0 if a.get("role") == "LISTENER" else 1,  # LISTENER role first
        a.get("id", 9999)  # Then by oldest (lowest ID)
    ))

    num_available = len(listener_candidates)
    actual_listeners = min(needed_listeners, num_available)

    if actual_listeners < needed_listeners:
        logger.warning(
            f"⚠️ Need {needed_listeners} listeners for {num_groups} groups, "
            f"but only {actual_listeners} healthy accounts available. Some accounts will handle more groups."
        )

    selected_listeners = listener_candidates[:actual_listeners]
    listener_session_names = [acc["session_name"] for acc in selected_listeners]

    # 5. Distribute groups evenly (round-robin)
    assignments: Dict[str, List[str]] = {sname: [] for sname in listener_session_names}
    for idx, group in enumerate(targets):
        listener_sname = listener_session_names[idx % actual_listeners]
        assignments[listener_sname].append(group)

    # 6. Update database roles
    _update_account_roles_in_db(listener_session_names, all_accounts)

    # 7. Store in Redis
    await queue_manager.set_listener_assignments(assignments)

    # Log summary
    for sname, groups in assignments.items():
        logger.info(f"📋 Listener '{sname}' assigned {len(groups)} groups: {groups[:3]}{'...' if len(groups) > 3 else ''}")

    logger.info(
        f"✅ Listener assignment complete: {actual_listeners} listeners, "
        f"{num_groups} groups, ~{math.ceil(num_groups/actual_listeners)} groups/listener"
    )

    return assignments


def _is_assignment_valid(
    existing: Dict[str, List[str]],
    current_targets: List[str],
    healthy_accounts: List[Dict]
) -> bool:
    """
    Check if existing assignments are still valid:
    - All assigned listener accounts are still healthy
    - All current targets are covered
    - No extra targets that no longer exist
    """
    # Check all listener accounts are healthy
    healthy_sessions = {acc["session_name"] for acc in healthy_accounts}
    for sname in existing:
        if sname not in healthy_sessions:
            logger.info(f"Listener '{sname}' is no longer healthy — triggering rebalance")
            return False

    # Check all targets are covered
    assigned_targets = set()
    for groups in existing.values():
        assigned_targets.update(groups)

    current_set = set(current_targets)
    if assigned_targets != current_set:
        logger.info("Target list changed — triggering rebalance")
        return False

    return True


def _update_account_roles_in_db(listener_session_names: List[str], all_accounts: List[Dict]):
    """
    Update the 'role' column in SQLite: selected accounts get LISTENER, rest get REPLIER.
    """
    import sqlite3
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "app.db")
    if not os.path.exists(db_path):
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Reset all to REPLIER
        cursor.execute("UPDATE accounts SET role = 'REPLIER'")

        # Set selected ones to LISTENER
        for sname in listener_session_names:
            cursor.execute("UPDATE accounts SET role = 'LISTENER' WHERE session_name = ?", (sname,))

        conn.commit()
        conn.close()
        logger.info(f"📝 Updated DB roles: {len(listener_session_names)} LISTENERs, rest are REPLIERs")
    except Exception as e:
        logger.error(f"Error updating account roles in DB: {e}")


async def handle_listener_failure(failed_session_name: str):
    """
    Called when a listener account fails (banned, unauthorized, etc.).
    Triggers reassignment of its groups to remaining healthy listeners.
    """
    logger.warning(f"🔄 Listener '{failed_session_name}' failed — triggering failover rebalance")

    # Force a complete rebalance which will exclude the failed account
    assignments = await auto_assign_listeners(force_rebalance=True)

    if assignments:
        logger.info(f"✅ Failover complete — {len(assignments)} listeners now active")
    else:
        logger.error("❌ Failover failed — no healthy listener accounts available!")

    return assignments


# ─── Replier Assignment ──────────────────────────────────────────


async def auto_assign_repliers(
    worker_id: str,
    targets: Optional[List[str]] = None,
    force_rebalance: bool = False
) -> Dict[str, List[str]]:
    """
    Assigns target groups to REPLIER accounts for a specific worker.
    STRICT POLICY: No account is reused across different groups or sessions.
    Each group gets a unique Primary Replier and a unique Backup Replier.

    Returns:
        Dict[session_name, List[group_targets]] — the replier assignment map for warming entities
    """
    from .database import get_all_group_assignments, save_group_assignment

    # 1. Get target groups
    if targets is None:
        targets = await queue_manager.get_active_targets()

    if not targets:
        logger.info("No active targets — clearing replier assignments")
        await queue_manager.set_replier_assignments(worker_id, {})
        await queue_manager.set_group_pair_assignments(worker_id, {})
        return {}

    # 2. Get replier accounts for this worker from local DB
    all_accounts = _get_all_accounts_from_db()
    healthy_accounts = _get_healthy_accounts(all_accounts)

    replier_accounts = [
        acc for acc in healthy_accounts
        if acc.get("role", "REPLIER") == "REPLIER"
    ]

    if not replier_accounts:
        logger.error(f"❌ No healthy replier accounts available for worker '{worker_id}'!")
        return {}

    replier_session_names = [acc["session_name"] for acc in replier_accounts]
    replier_set = set(replier_session_names)

    # 3. Read sticky assignments from SQLite
    db_assignments = await get_all_group_assignments()

    pair_map: Dict[str, Dict[str, str]] = {}
    used_sessions: set = set()

    # Pass 1: Retain valid sticky DB assignments (ensuring no session is reused across groups)
    for group in targets:
        if group in db_assignments:
            p = db_assignments[group].get("primary")
            b = db_assignments[group].get("backup")

            # Ensure primary is healthy & not already claimed by another group
            valid_p = p if (p in replier_set and p not in used_sessions) else None
            if valid_p:
                used_sessions.add(valid_p)

            # Ensure backup is healthy, distinct, & not claimed
            valid_b = b if (b in replier_set and b not in used_sessions and b != valid_p) else None
            if valid_b:
                used_sessions.add(valid_b)

            if valid_p or valid_b:
                pair_map[group] = {
                    "primary": valid_p or valid_b,
                    "backup": valid_b or valid_p
                }

    # Pass 2: Assign unassigned targets from remaining available (unused) accounts
    available_sessions = [s for s in replier_session_names if s not in used_sessions]

    for group in targets:
        # Check if already has a complete distinct pair
        if (
            group in pair_map 
            and pair_map[group].get("primary") 
            and pair_map[group].get("backup") 
            and pair_map[group]["primary"] != pair_map[group]["backup"]
        ):
            continue

        existing_p = pair_map.get(group, {}).get("primary")
        existing_b = pair_map.get(group, {}).get("backup")

        p = existing_p
        b = existing_b

        # Assign primary if missing
        if not p:
            if available_sessions:
                p = available_sessions.pop(0)
                used_sessions.add(p)
            else:
                logger.warning(f"⚠️ No unused account available for Primary in group '{group}'!")

        # Assign backup if missing or duplicate
        if not b or b == p:
            if available_sessions:
                b = available_sessions.pop(0)
                used_sessions.add(b)
            else:
                b = p  # Fall back to primary if no spare unused account available

        if p:
            pair_map[group] = {"primary": p, "backup": b or p}
            await save_group_assignment(group, p, b or p)
        else:
            logger.error(f"❌ Cannot assign group '{group}': All replier accounts are already used by other groups (Strict 1-account-per-group policy).")

    # 4. Build reverse assignment map: session_name -> list of groups
    assignments: Dict[str, List[str]] = {sname: [] for sname in replier_session_names}
    for group, pair in pair_map.items():
        p_name = pair["primary"]
        b_name = pair["backup"]
        if p_name and group not in assignments[p_name]:
            assignments[p_name].append(group)
        if b_name and b_name != p_name and group not in assignments[b_name]:
            assignments[b_name].append(group)

    assignments = {k: v for k, v in assignments.items() if v}

    # 5. Store in Redis
    await queue_manager.set_replier_assignments(worker_id, assignments)
    await queue_manager.set_group_pair_assignments(worker_id, pair_map)

    logger.info(
        f"✅ Strict non-reusing replier assignment for '{worker_id}': {len(pair_map)} groups assigned to "
        f"{len(used_sessions)} unique replier sessions."
    )
    for grp, pair in pair_map.items():
        logger.info(f"  Group '{grp}' ➔ Primary: {pair['primary']} | Backup: {pair['backup']}")

    return assignments



def build_target_to_replier_map(assignments: Dict[str, List[str]]) -> Dict[str, str]:
    """
    Build a reverse map: target_string → primary_replier_session_name.
    Used by worker to quickly look up which replier account handles a group.
    """
    reverse_map = {}
    for session_name, groups in assignments.items():
        for group in groups:
            if group not in reverse_map:
                reverse_map[group] = session_name
    return reverse_map



# ─── Summary for Dashboard ──────────────────────────────────────


async def get_listener_summary() -> Dict:
    """Get a summary of current listener AND replier assignments for the dashboard."""
    listener_assignments = await queue_manager.get_listener_assignments()
    replier_assignments = await queue_manager.get_all_replier_assignments()
    targets = await queue_manager.get_active_targets()

    summary = {
        "total_targets": len(targets),
        "total_listeners": len(listener_assignments),
        "max_groups_per_listener": MAX_GROUPS_PER_LISTENER,
        "listener_assignments": {},
        "replier_assignments": {},
    }

    for sname, groups in listener_assignments.items():
        summary["listener_assignments"][sname] = {
            "group_count": len(groups),
            "groups": groups
        }

    for worker_id, worker_assigns in replier_assignments.items():
        summary["replier_assignments"][worker_id] = {}
        for sname, groups in worker_assigns.items():
            summary["replier_assignments"][worker_id][sname] = {
                "group_count": len(groups),
                "groups": groups
            }

    return summary

