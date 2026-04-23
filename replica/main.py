"""
replica/main.py — Python translation of replica1/main.go

A single file serves all three replica instances; the replica number,
port, peers and gateway URL are controlled by environment variables.

Bug fix carried forward from original Go code analysis:
  - In the /heartbeat handler, when a follower is behind, it now
    properly passes the CURRENT log length (not a stale closure value)
    to catch_up_from_leader, preventing missed entries.
"""

import asyncio
import json
import logging
import os

import aiohttp
from aiohttp import web

from raft.node import LogEntry, NodeState, RaftNode, StrokeData

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("replica")

# ── Config ────────────────────────────────────────────────────────────────────

REPLICA_ID: str = os.environ.get("REPLICA_ID", "1")
PORT: int = int(os.environ.get("PORT", "8001"))
PEERS: list[str] = [
    p.strip() for p in os.environ.get("PEERS", "").split(",") if p.strip()
]
GATEWAY_URL: str = os.environ.get("GATEWAY_URL", "")


# ── Submit helper (leader commits an entry, notifies gateway) ─────────────────

async def handle_submission(
    raft_node: RaftNode,
    entry: LogEntry,
    session: aiohttp.ClientSession,
) -> dict:
    """Attempt to commit entry via Raft majority. Returns JSON-serialisable dict."""
    async with raft_node._lock:
        if raft_node.state != NodeState.LEADER:
            return {"success": False, "error": "not leader"}

        entry.index = len(raft_node.log)
        entry.term = raft_node.current_term
        raft_node.log.append(entry)

        req_index = entry.index
        req_term = entry.term
        peers = list(raft_node.peers)
        leader_id = raft_node.id
        commit_index = raft_node.commit_index
        prev_log_index = req_index - 1
        prev_log_term = raft_node.log[prev_log_index].term if prev_log_index >= 0 else 0

    append_req = {
        "term": req_term,
        "leaderId": leader_id,
        "prevLogIndex": prev_log_index,
        "prevLogTerm": prev_log_term,
        "entries": [entry.to_dict()],
        "leaderCommit": commit_index,
    }

    success_acks = 1  # self

    async def replicate(peer: str) -> bool:
        try:
            async with session.post(
                f"{peer}/append-entries",
                json=append_req,
                timeout=aiohttp.ClientTimeout(total=0.5),
            ) as resp:
                data = await resp.json()
                return data.get("success", False)
        except Exception:
            return False

    tasks = [asyncio.create_task(replicate(p)) for p in peers]
    done, _ = await asyncio.wait(tasks, timeout=0.5)
    for t in done:
        if t.result():
            success_acks += 1

    quorum = (len(peers) + 1) // 2 + 1
    if success_acks >= quorum:
        async with raft_node._lock:
            if req_index > raft_node.commit_index:
                raft_node.commit_index = req_index
                log.info("[%s] committed index=%d term=%d (majority ack=%d)",
                         raft_node.id, req_index, req_term, success_acks)

        # Notify gateway (fire-and-forget with retries)
        if GATEWAY_URL:
            asyncio.create_task(_notify_gateway(raft_node.id, entry, session))

        return {"success": True, "index": req_index}
    else:
        log.info("[%s] failed to commit index=%d: acks=%d quorum=%d",
                 raft_node.id, req_index, success_acks, quorum)
        return {"success": False, "error": "failed to get majority ack"}


async def _notify_gateway(replica_id: str, entry: LogEntry, session: aiohttp.ClientSession) -> None:
    for attempt in range(3):
        try:
            async with session.post(
                f"{GATEWAY_URL}/notify-gateway",
                json=entry.to_dict(),
                timeout=aiohttp.ClientTimeout(total=2),
            ) as resp:
                resp.raise_for_status()
                log.info("[%s] notified gateway index=%d type=%s", replica_id, entry.index, entry.type)
                return
        except Exception as exc:
            log.warning("[%s] notify gateway attempt %d failed: %s", replica_id, attempt + 1, exc)
            await asyncio.sleep(0.2)


# ── HTTP handlers ─────────────────────────────────────────────────────────────

async def status_handler(request: web.Request) -> web.Response:
    node: RaftNode = request.app["raft_node"]
    async with node._lock:
        data = {
            "id": node.id,
            "state": node.state.value,
            "term": node.current_term,
            "commitIndex": node.commit_index,
            "logLength": len(node.log),
            "leader": node.leader_id,
        }
    return web.json_response(data)


async def debug_handler(request: web.Request) -> web.Response:
    node: RaftNode = request.app["raft_node"]
    async with node._lock:
        data = {
            "id": node.id,
            "state": node.state.value,
            "term": node.current_term,
            "commitIndex": node.commit_index,
            "logLength": len(node.log),
            "leader": node.leader_id,
            "peers": node.peers,
            "lastHeartbeat": node.last_heartbeat,
        }
    return web.json_response(data)


async def request_vote_handler(request: web.Request) -> web.Response:
    node: RaftNode = request.app["raft_node"]
    req = await request.json()

    candidate_id = req.get("candidateId", "")
    term = req.get("term", 0)
    last_log_index = req.get("lastLogIndex", -1)
    last_log_term = req.get("lastLogTerm", 0)

    log.info("[%s] received RequestVote from %s term %d", node.id, candidate_id, term)

    if term > node.current_term:
        await node.become_follower(term)

    async with node._lock:
        resp_term = node.current_term
        granted = False

        if term == node.current_term and (not node.voted_for or node.voted_for == candidate_id):
            my_last_index = node.log[-1].index if node.log else -1
            my_last_term = node.log[-1].term if node.log else 0
            if last_log_term > my_last_term or (last_log_term == my_last_term and last_log_index >= my_last_index):
                node.voted_for = candidate_id
                granted = True

    log.info("[%s] responding to RequestVote from %s: granted=%s, term=%d",
             node.id, candidate_id, granted, resp_term)

    if granted:
        asyncio.create_task(node._reset_election_timer())

    return web.json_response({"term": resp_term, "granted": granted})


async def heartbeat_handler(request: web.Request) -> web.Response:
    node: RaftNode = request.app["raft_node"]
    req = await request.json()

    term = req.get("term", 0)
    leader_id = req.get("leaderId", "")
    leader_commit = req.get("leaderCommit", -1)

    if term > node.current_term:
        await node.become_follower(term)
    elif term == node.current_term:
        async with node._lock:
            if node.state != NodeState.FOLLOWER:
                pass  # will be handled below

    async with node._lock:
        if term < node.current_term:
            return web.json_response({"term": node.current_term, "success": False})

        node.leader_id = leader_id
        import time
        node.last_heartbeat = time.monotonic()

        if node.state != NodeState.FOLLOWER:
            node.state = NodeState.FOLLOWER
            node.voted_for = ""

        needs_catchup = False
        catch_log_len = len(node.log)

        if leader_commit > node.commit_index:
            last_log_idx = len(node.log) - 1
            if leader_commit <= last_log_idx:
                node.commit_index = leader_commit
                log.info("[%s] commitIndex advanced to %d via heartbeat", node.id, node.commit_index)
            else:
                # Bug fix: capture log length inside the lock for accurate catch-up
                needs_catchup = True

        resp_term = node.current_term

    asyncio.create_task(node._reset_election_timer())

    if needs_catchup:
        asyncio.create_task(node.catch_up_from_leader(leader_id, catch_log_len))

    return web.json_response({"term": resp_term, "success": True})


async def append_entries_handler(request: web.Request) -> web.Response:
    node: RaftNode = request.app["raft_node"]
    req = await request.json()

    term = req.get("term", 0)
    leader_id = req.get("leaderId", "")
    prev_log_index = req.get("prevLogIndex", -1)
    prev_log_term = req.get("prevLogTerm", 0)
    entries = [LogEntry.from_dict(e) for e in req.get("entries", [])]
    leader_commit = req.get("leaderCommit", -1)

    async with node._lock:
        if term < node.current_term:
            return web.json_response({"term": node.current_term, "success": False, "logLength": len(node.log)})

    if term > node.current_term:
        await node.become_follower(term)

    async with node._lock:
        node.leader_id = leader_id
        import time
        node.last_heartbeat = time.monotonic()

        # Consistency check
        needs_catchup = False
        catch_log_len = len(node.log)
        if prev_log_index >= 0:
            if prev_log_index >= len(node.log) or node.log[prev_log_index].term != prev_log_term:
                needs_catchup = True
                resp = {"term": node.current_term, "success": False, "logLength": len(node.log)}

        if not needs_catchup:
            insert_index = prev_log_index + 1
            for i, entry in enumerate(entries):
                log_index = insert_index + i
                if log_index < len(node.log):
                    if node.log[log_index].term != entry.term:
                        node.log = node.log[:log_index]
                        node.log.append(entry)
                        log.info("[%s] resolved conflict, appended entry index=%d term=%d",
                                 node.id, entry.index, entry.term)
                elif log_index == len(node.log):
                    node.log.append(entry)
                    log.info("[%s] appended entry index=%d term=%d", node.id, entry.index, entry.term)

            if leader_commit > node.commit_index:
                last_new_index = entries[-1].index if entries else (node.log[-1].index if node.log else -1)
                if last_new_index >= 0:
                    node.commit_index = min(leader_commit, last_new_index)
                    log.info("[%s] commitIndex updated to %d", node.id, node.commit_index)

            resp = {"term": node.current_term, "success": True, "logLength": len(node.log)}

    asyncio.create_task(node._reset_election_timer())

    if needs_catchup:
        asyncio.create_task(node.catch_up_from_leader(leader_id, catch_log_len))

    return web.json_response(resp)


async def sync_log_handler(request: web.Request) -> web.Response:
    node: RaftNode = request.app["raft_node"]
    req = await request.json()
    from_index = req.get("fromIndex", 0)

    async with node._lock:
        entries = []
        for i in range(from_index, min(node.commit_index + 1, len(node.log))):
            if i >= 0:
                entries.append(node.log[i].to_dict())
        commit_idx = node.commit_index

    log.info("[%s] sync-log: sending %d entries from index %d, commitIndex=%d",
             node.id, len(entries), from_index, commit_idx)
    return web.json_response({"entries": entries, "commitIndex": commit_idx})


async def submit_stroke_handler(request: web.Request) -> web.Response:
    node: RaftNode = request.app["raft_node"]
    session: aiohttp.ClientSession = request.app["http_session"]

    if request.method != "POST":
        raise web.HTTPMethodNotAllowed(request.method, ["POST"])

    data = await request.json()
    stroke = StrokeData.from_dict(data)
    entry = LogEntry(type="stroke", stroke=stroke)
    result = await handle_submission(node, entry, session)
    if not result.get("success") and result.get("error") == "not leader":
        return web.json_response(result, status=200)
    return web.json_response(result)


async def submit_clear_handler(request: web.Request) -> web.Response:
    node: RaftNode = request.app["raft_node"]
    session: aiohttp.ClientSession = request.app["http_session"]

    if request.method != "POST":
        raise web.HTTPMethodNotAllowed(request.method, ["POST"])

    entry = LogEntry(type="clear")
    result = await handle_submission(node, entry, session)
    if not result.get("success") and result.get("error") == "not leader":
        return web.json_response(result, status=200)
    return web.json_response(result)


# ── App lifecycle ─────────────────────────────────────────────────────────────

async def on_startup(app: web.Application) -> None:
    session = aiohttp.ClientSession()
    app["http_session"] = session
    node: RaftNode = app["raft_node"]
    node.set_http_session(session)
    node.start()
    log.info("replica%s started on port %d, peers: %s", REPLICA_ID, PORT, PEERS)


async def on_cleanup(app: web.Application) -> None:
    await app["http_session"].close()


def build_app() -> web.Application:
    node = RaftNode(f"replica{REPLICA_ID}", PEERS)
    app = web.Application()
    app["raft_node"] = node

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    app.router.add_get("/status", status_handler)
    app.router.add_get("/debug", debug_handler)
    app.router.add_post("/request-vote", request_vote_handler)
    app.router.add_post("/heartbeat", heartbeat_handler)
    app.router.add_post("/append-entries", append_entries_handler)
    app.router.add_post("/sync-log", sync_log_handler)
    app.router.add_post("/submit-stroke", submit_stroke_handler)
    app.router.add_post("/submit-clear", submit_clear_handler)

    return app


if __name__ == "__main__":
    import asyncio
    web.run_app(build_app(), host="0.0.0.0", port=PORT)
