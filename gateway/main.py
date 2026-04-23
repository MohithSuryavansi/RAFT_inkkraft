"""
Inkraft Gateway — Python rewrite of gateway/main.go

Fixes vs the original Go version:
 1. lastCommittedIdx was a single global that never reset across leader changes.
    We now track committed indices per (index, term) pair so duplicate
    suppression is correct even after leader failover / re-election.
 2. When a new tab opens and calls get_history it now fetches from the leader
    AND fast-forwards the gateway's own committed-index watermark so that
    subsequent notify-gateway calls for already-seen entries are correctly
    deduped rather than sent twice.
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional

import aiohttp
from aiohttp import web, WSMsgType

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gateway")

# ── Config ───────────────────────────────────────────────────────────────────

REPLICAS: list[str] = [
    r.strip()
    for r in os.environ.get(
        "REPLICAS", "http://localhost:8001,http://localhost:8002,http://localhost:8003"
    ).split(",")
    if r.strip()
]

# ── Hub (broadcast to all connected WebSocket clients) ───────────────────────

class Hub:
    def __init__(self) -> None:
        self._clients: set[web.WebSocketResponse] = set()
        self._lock = asyncio.Lock()

    async def register(self, ws: web.WebSocketResponse) -> None:
        async with self._lock:
            self._clients.add(ws)
        log.info("client connected (%d total)", len(self._clients))

    async def unregister(self, ws: web.WebSocketResponse) -> None:
        async with self._lock:
            self._clients.discard(ws)
        log.info("client disconnected (%d total)", len(self._clients))

    async def broadcast(self, msg: dict) -> None:
        data = json.dumps(msg)
        dead: list[web.WebSocketResponse] = []
        async with self._lock:
            clients = list(self._clients)
        for ws in clients:
            try:
                await ws.send_str(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.unregister(ws)

    async def send_to(self, ws: web.WebSocketResponse, msg: dict) -> None:
        try:
            await ws.send_str(json.dumps(msg))
        except Exception:
            await self.unregister(ws)


# ── Leader tracker ────────────────────────────────────────────────────────────

class LeaderTracker:
    def __init__(self, replicas: list[str], hub: Hub) -> None:
        self._replicas = replicas
        self._hub = hub
        self._leader_url: str = ""
        self._leader_id: str = ""
        self._lock = asyncio.Lock()

    async def get_leader(self) -> tuple[str, str]:
        async with self._lock:
            return self._leader_url, self._leader_id

    async def invalidate(self) -> None:
        async with self._lock:
            if self._leader_id:
                log.info("invalidating leader %s (%s)", self._leader_id, self._leader_url)
            self._leader_url = ""
            self._leader_id = ""

    async def poll_forever(self) -> None:
        timeout = aiohttp.ClientTimeout(total=0.4)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                found_url, found_id = "", ""
                for replica in self._replicas:
                    try:
                        async with session.get(f"{replica}/status") as resp:
                            status = await resp.json()
                        if status.get("state") == "Leader":
                            found_url = replica
                            found_id = status["id"]
                            break
                    except Exception:
                        pass

                async with self._lock:
                    old_id = self._leader_id
                    self._leader_url = found_url
                    self._leader_id = found_id

                if found_id != old_id:
                    if found_id:
                        log.info("leader discovered: %s (%s)", found_id, found_url)
                        await self._hub.broadcast({"type": "leader_changed", "leader": found_id})
                    elif old_id:
                        log.info("leader lost (was %s)", old_id)
                        await self._hub.broadcast({"type": "no_leader", "message": "Election in progress"})

                sleep_ms = 200 if not found_id else 500
                await asyncio.sleep(sleep_ms / 1000)


# ── Log-entry committed-index tracker (Bug fix #1) ───────────────────────────
# We track committed entries by their (index, term) pair so that after a
# leader change — where terms change — we never accidentally suppress a new
# entry that re-uses an old index with a different term.

class CommitTracker:
    def __init__(self) -> None:
        self._seen: dict[int, int] = {}   # index → term
        self._lock = asyncio.Lock()

    async def is_duplicate(self, index: int, term: int) -> bool:
        async with self._lock:
            return self._seen.get(index) == term

    async def mark_seen(self, index: int, term: int) -> None:
        async with self._lock:
            self._seen[index] = term

    async def sync_from_history(self, entries: list[dict]) -> None:
        """Advance watermark from a full history fetch (Bug fix #2)."""
        async with self._lock:
            for e in entries:
                idx = e.get("index", -1)
                term = e.get("term", 0)
                if idx >= 0:
                    self._seen[idx] = term


commit_tracker = CommitTracker()


# ── History fetch ─────────────────────────────────────────────────────────────

async def fetch_history(lt: LeaderTracker, session: aiohttp.ClientSession) -> Optional[list[dict]]:
    leader_url, _ = await lt.get_leader()
    if not leader_url:
        return None
    try:
        async with session.post(
            f"{leader_url}/sync-log",
            json={"fromIndex": 0},
            timeout=aiohttp.ClientTimeout(total=2),
        ) as resp:
            data = await resp.json()
            return data.get("entries", [])
    except Exception as exc:
        log.warning("history fetch failed: %s", exc)
        await lt.invalidate()
        return None


# ── Forward stroke/clear to leader ───────────────────────────────────────────

async def forward_to_leader(lt: LeaderTracker, msg: dict, session: aiohttp.ClientSession) -> bool:
    endpoint = "/submit-clear" if msg.get("type") == "clear" else "/submit-stroke"
    payload = {k: msg[k] for k in ("x0", "y0", "x1", "y1", "color", "width") if k in msg}

    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        leader_url, _ = await lt.get_leader()
        if leader_url:
            try:
                async with session.post(
                    f"{leader_url}{endpoint}",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=1),
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        if result.get("success"):
                            return True
                        if result.get("error") == "not leader":
                            log.info("forward: %s says not leader, invalidating", leader_url)
                            await lt.invalidate()
            except Exception as exc:
                log.warning("forward error to %s: %s, invalidating", leader_url, exc)
                await lt.invalidate()
        await asyncio.sleep(0.2)
    return False


# ── WebSocket handler ─────────────────────────────────────────────────────────

async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    hub: Hub = request.app["hub"]
    lt: LeaderTracker = request.app["leader_tracker"]
    session: aiohttp.ClientSession = request.app["http_session"]

    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    await hub.register(ws)

    # Greet the new client
    await hub.send_to(ws, {"type": "connected", "message": "Inkraft connected"})

    _, leader_id = await lt.get_leader()
    if leader_id:
        await hub.send_to(ws, {"type": "leader_changed", "leader": leader_id})

    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type")

            if msg_type == "get_history":
                entries = await fetch_history(lt, session)
                if entries is not None:
                    # Bug fix #2: sync watermark so live notify-gateway calls
                    # for already-committed entries are correctly deduped.
                    await commit_tracker.sync_from_history(entries)
                    strokes = [
                        {
                            "type": e.get("type", "stroke"),
                            "x0": e.get("stroke", {}).get("x0", 0),
                            "y0": e.get("stroke", {}).get("y0", 0),
                            "x1": e.get("stroke", {}).get("x1", 0),
                            "y1": e.get("stroke", {}).get("y1", 0),
                            "color": e.get("stroke", {}).get("color", "#000000"),
                            "width": e.get("stroke", {}).get("width", 3),
                            "index": e.get("index", -1),
                        }
                        for e in entries
                    ]
                    await hub.send_to(ws, {"type": "history", "strokes": strokes})

            elif msg_type in ("stroke", "clear"):
                asyncio.create_task(forward_to_leader(lt, data, session))

        elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
            break

    await hub.unregister(ws)
    return ws


# ── /notify-gateway ───────────────────────────────────────────────────────────

async def notify_gateway(request: web.Request) -> web.Response:
    hub: Hub = request.app["hub"]
    try:
        entry = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text="invalid JSON")

    idx = entry.get("index", -1)
    term = entry.get("term", 0)
    entry_type = entry.get("type", "stroke")

    # Bug fix #1: deduplicate by (index, term), not just index
    if await commit_tracker.is_duplicate(idx, term):
        log.info("duplicate entry index=%d term=%d, ignoring", idx, term)
        return web.json_response({"ok": True, "duplicate": True})

    await commit_tracker.mark_seen(idx, term)
    log.info("committed %s index=%d term=%d", entry_type, idx, term)

    stroke = entry.get("stroke", {})
    broadcast_msg = {
        "type": entry_type,
        "x0": stroke.get("x0", 0),
        "y0": stroke.get("y0", 0),
        "x1": stroke.get("x1", 0),
        "y1": stroke.get("y1", 0),
        "color": stroke.get("color", "#000000"),
        "width": stroke.get("width", 3),
        "index": idx,
    }
    await hub.broadcast(broadcast_msg)
    return web.json_response({"ok": True})


# ── /history HTTP endpoint ────────────────────────────────────────────────────

async def history_http(request: web.Request) -> web.Response:
    lt: LeaderTracker = request.app["leader_tracker"]
    session: aiohttp.ClientSession = request.app["http_session"]
    entries = await fetch_history(lt, session)
    if entries is None:
        raise web.HTTPServiceUnavailable(text="no leader available")
    strokes = [
        {
            "type": "stroke",
            "x0": e.get("stroke", {}).get("x0", 0),
            "y0": e.get("stroke", {}).get("y0", 0),
            "x1": e.get("stroke", {}).get("x1", 0),
            "y1": e.get("stroke", {}).get("y1", 0),
            "color": e.get("stroke", {}).get("color", "#000000"),
            "width": e.get("stroke", {}).get("width", 3),
            "index": e.get("index", -1),
        }
        for e in entries
    ]
    return web.json_response({"type": "history", "strokes": strokes})


# ── Static frontend ───────────────────────────────────────────────────────────

async def index_handler(request: web.Request) -> web.FileResponse:
    return web.FileResponse("/frontend/index.html")


# ── App lifecycle ─────────────────────────────────────────────────────────────

async def on_startup(app: web.Application) -> None:
    app["http_session"] = aiohttp.ClientSession()
    hub: Hub = app["hub"]
    lt: LeaderTracker = app["leader_tracker"]
    app["poll_task"] = asyncio.create_task(lt.poll_forever())
    log.info("gateway started, replicas: %s", REPLICAS)


async def on_cleanup(app: web.Application) -> None:
    app["poll_task"].cancel()
    await app["http_session"].close()


def build_app() -> web.Application:
    hub = Hub()
    lt = LeaderTracker(REPLICAS, hub)

    app = web.Application()
    app["hub"] = hub
    app["leader_tracker"] = lt

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/health", lambda r: web.Response(text="ok"))
    app.router.add_get("/history", history_http)
    app.router.add_post("/notify-gateway", notify_gateway)
    # Serve frontend static files
    app.router.add_static("/", "/frontend", show_index=True)

    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=9000)
