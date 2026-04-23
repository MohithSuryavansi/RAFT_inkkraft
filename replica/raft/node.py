"""
raft/node.py — Python translation of replica1/raft/node.go + types.go + log.go

Implements the Raft consensus algorithm core:
  - Election timer with randomised timeout (500–800 ms)
  - Candidate / vote solicitation
  - Leader heartbeats (150 ms interval)
  - Log replication helpers
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import aiohttp

log = logging.getLogger("raft")


# ── Types ─────────────────────────────────────────────────────────────────────

class NodeState(str, Enum):
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"


@dataclass
class StrokeData:
    x0: float = 0.0
    y0: float = 0.0
    x1: float = 0.0
    y1: float = 0.0
    color: str = "#000000"
    width: float = 3.0

    def to_dict(self) -> dict:
        return {"x0": self.x0, "y0": self.y0, "x1": self.x1, "y1": self.y1,
                "color": self.color, "width": self.width}

    @classmethod
    def from_dict(cls, d: dict) -> "StrokeData":
        return cls(x0=d.get("x0", 0), y0=d.get("y0", 0),
                   x1=d.get("x1", 0), y1=d.get("y1", 0),
                   color=d.get("color", "#000000"), width=d.get("width", 3))


@dataclass
class LogEntry:
    index: int = 0
    term: int = 0
    type: str = "stroke"   # "stroke" | "clear"
    stroke: StrokeData = field(default_factory=StrokeData)

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "term": self.term,
            "type": self.type,
            "stroke": self.stroke.to_dict(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "LogEntry":
        return cls(
            index=d.get("index", 0),
            term=d.get("term", 0),
            type=d.get("type", "stroke"),
            stroke=StrokeData.from_dict(d.get("stroke", {})),
        )


# ── Node ──────────────────────────────────────────────────────────────────────

ELECTION_TIMEOUT_MIN_MS = 500
ELECTION_TIMEOUT_MAX_MS = 800
HEARTBEAT_INTERVAL_MS = 150


class RaftNode:
    def __init__(self, node_id: str, peers: list[str]) -> None:
        self.id = node_id
        self.state = NodeState.FOLLOWER

        self.current_term = 0
        self.voted_for: str = ""
        self.log: list[LogEntry] = []
        self.commit_index = -1

        self.peers = peers
        self.leader_id = ""
        self.last_heartbeat: float = 0.0

        self._lock = asyncio.Lock()

        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        # Injected by main.py after startup — used for heartbeat catch-up
        self._http_session: Optional[aiohttp.ClientSession] = None

    # ── Public entry points ───────────────────────────────────────────────────

    def start(self) -> None:
        """Call once the event loop is running."""
        asyncio.get_event_loop().call_soon(lambda: asyncio.create_task(self._reset_election_timer()))

    def set_http_session(self, session: aiohttp.ClientSession) -> None:
        self._http_session = session

    # ── Election timer ────────────────────────────────────────────────────────

    async def _reset_election_timer(self) -> None:
        """Cancel existing election timer and start a new one."""
        async with self._lock:
            await self._cancel_election_task_locked()
            self._election_task = asyncio.create_task(self._election_timeout_loop())

    async def _cancel_election_task_locked(self) -> None:
        if self._election_task and not self._election_task.done():
            self._election_task.cancel()
            try:
                await self._election_task
            except asyncio.CancelledError:
                pass
            self._election_task = None

    async def _election_timeout_loop(self) -> None:
        timeout = random.randint(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS) / 1000
        await asyncio.sleep(timeout)
        await self._become_candidate()

    # ── State transitions ─────────────────────────────────────────────────────

    async def become_follower(self, term: int) -> None:
        async with self._lock:
            old_state = self.state
            self.state = NodeState.FOLLOWER
            self.current_term = term
            self.voted_for = ""
            await self._stop_heartbeat_locked()
            log.info("[%s] %s->FOLLOWER term=%d commitIndex=%d", self.id, old_state, term, self.commit_index)
        await self._reset_election_timer()

    async def _become_candidate(self) -> None:
        async with self._lock:
            old_state = self.state
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.id
            log.info("[%s] %s->CANDIDATE term=%d commitIndex=%d", self.id, old_state, self.current_term, self.commit_index)
            # Reset election timer for split-vote retry
            await self._cancel_election_task_locked()
            self._election_task = asyncio.create_task(self._election_timeout_loop())
            term = self.current_term
            peers = list(self.peers)
            last_log_index = self.log[-1].index if self.log else -1
            last_log_term = self.log[-1].term if self.log else 0

        asyncio.create_task(self._run_election(term, peers, last_log_index, last_log_term))

    async def _run_election(self, term: int, peers: list[str], last_log_index: int, last_log_term: int) -> None:
        log.info("[%s] starting election for term %d", self.id, term)
        votes = 1  # self-vote

        async def request_vote(peer: str) -> bool:
            nonlocal votes
            try:
                async with self._http_session.post(
                    f"{peer}/request-vote",
                    json={"term": term, "candidateId": self.id,
                          "lastLogIndex": last_log_index, "lastLogTerm": last_log_term},
                    timeout=aiohttp.ClientTimeout(total=0.5),
                ) as resp:
                    data = await resp.json()
                    if data.get("term", 0) > term:
                        log.info("[%s] higher term %d from %s, reverting to follower", self.id, data["term"], peer)
                        await self.become_follower(data["term"])
                        return False
                    if data.get("granted"):
                        log.info("[%s] vote granted by %s for term %d", self.id, peer, term)
                        return True
                    log.info("[%s] vote denied by %s for term %d", self.id, peer, term)
                    return False
            except Exception as exc:
                log.debug("[%s] error requesting vote from %s: %s", self.id, peer, exc)
                return False

        tasks = [asyncio.create_task(request_vote(p)) for p in peers]
        done, _ = await asyncio.wait(tasks, timeout=0.3)
        for t in done:
            if t.result():
                votes += 1

        async with self._lock:
            if self.state == NodeState.CANDIDATE and self.current_term == term:
                quorum = (len(self.peers) + 1) // 2 + 1
                if votes >= quorum:
                    log.info("[%s] CANDIDATE->LEADER term=%d votes=%d", self.id, term, votes)
                    await self._become_leader_locked()

    async def _become_leader_locked(self) -> None:
        """Must be called with self._lock held."""
        if self.state != NodeState.CANDIDATE:
            return
        self.state = NodeState.LEADER
        self.leader_id = self.id
        await self._cancel_election_task_locked()
        log.info("[%s] BECAME LEADER term=%d commitIndex=%d logLength=%d",
                 self.id, self.current_term, self.commit_index, len(self.log))
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _stop_heartbeat_locked(self) -> None:
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

    # ── Heartbeats ────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_MS / 1000)
            async with self._lock:
                if self.state != NodeState.LEADER:
                    return
                term = self.current_term
                commit_index = self.commit_index
                peers = list(self.peers)

            for peer in peers:
                asyncio.create_task(self._send_heartbeat(peer, term, commit_index))

    async def _send_heartbeat(self, peer: str, term: int, commit_index: int) -> None:
        try:
            async with self._http_session.post(
                f"{peer}/heartbeat",
                json={"term": term, "leaderId": self.id, "leaderCommit": commit_index},
                timeout=aiohttp.ClientTimeout(total=0.2),
            ) as resp:
                data = await resp.json()
                if data.get("term", 0) > term:
                    log.info("[%s] higher term %d in heartbeat response from %s", self.id, data["term"], peer)
                    await self.become_follower(data["term"])
        except Exception:
            pass

    # ── Log helpers ───────────────────────────────────────────────────────────

    def get_entries_from(self, from_index: int) -> list[LogEntry]:
        if from_index < 0 or from_index >= len(self.log):
            return []
        return list(self.log[from_index:])

    # ── Catch-up sync (called from append-entries / heartbeat handlers) ───────

    async def catch_up_from_leader(self, leader_id: str, log_length: int) -> None:
        """Fetch missing log entries from the leader (async, no lock held)."""
        leader_url = ""
        async with self._lock:
            for p in self.peers:
                if leader_id in p:
                    leader_url = p
                    break

        if not leader_url or not self._http_session:
            return

        try:
            async with self._http_session.post(
                f"{leader_url}/sync-log",
                json={"fromIndex": log_length},
                timeout=aiohttp.ClientTimeout(total=2),
            ) as resp:
                data = await resp.json()
        except Exception as exc:
            log.warning("[%s] catch-up sync failed: %s", self.id, exc)
            return

        entries = [LogEntry.from_dict(e) for e in data.get("entries", [])]
        remote_commit = data.get("commitIndex", -1)

        if not entries:
            return

        async with self._lock:
            for entry in entries:
                if entry.index < len(self.log):
                    self.log[entry.index] = entry
                elif entry.index == len(self.log):
                    self.log.append(entry)
            if remote_commit > self.commit_index:
                last_idx = len(self.log) - 1
                self.commit_index = min(remote_commit, last_idx) if last_idx >= 0 else self.commit_index
            log.info("[%s] catch-up: logLength=%d commitIndex=%d", self.id, len(self.log), self.commit_index)
