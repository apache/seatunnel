#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Session and memory management for SeaTunnel CLI.

Two layers of persistence:
  - Session: conversation history, auto-saved after each interaction, resumable
  - Memory:  cross-session facts (connections, preferences, project context),
             injected into LLM system prompts for continuity
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .llm_provider import LLMProvider

logger = logging.getLogger(__name__)


def _atomic_write(path: Path, data: dict) -> None:
    """Write JSON atomically using temp file + rename."""
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=path.parent, suffix=".tmp", prefix=".seatunnel_"
    )
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, str(path))
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ─── Session Manager ───


class SessionManager:
    """Manages conversation sessions -- save, load, list, resume."""

    def __init__(self, base_dir: Path):
        self.sessions_dir = base_dir / "sessions"
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        self.current_session_id: str | None = None

    @staticmethod
    def _generate_session_id() -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{ts}_{secrets.token_hex(2)}"

    def new_session(self) -> str:
        sid = self._generate_session_id()
        self.current_session_id = sid
        return sid

    def save_session(
        self,
        conversation_history: list[dict],
        last_config: str | None = None,
    ) -> None:
        if not self.current_session_id:
            self.current_session_id = self._generate_session_id()

        path = self.sessions_dir / f"{self.current_session_id}.json"

        existing: dict = {}
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except Exception:
                pass

        data = {
            "session_id": self.current_session_id,
            "created_at": existing.get("created_at", _now_iso()),
            "last_active": _now_iso(),
            "summary": existing.get("summary", ""),
            "message_count": len(conversation_history),
            "last_config": last_config,
            "conversation_history": conversation_history,
        }
        _atomic_write(path, data)

    def load_session(self, session_id: str) -> tuple[list[dict], str | None]:
        """Load a session. Returns (conversation_history, last_config)."""
        path = self.sessions_dir / f"{session_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"Session not found: {session_id}")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.current_session_id = session_id
        return data.get("conversation_history", []), data.get("last_config")

    def list_sessions(self, limit: int = 10) -> list[dict]:
        result = []
        for f in sorted(self.sessions_dir.glob("*.json"), reverse=True):
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                result.append({
                    "session_id": data.get("session_id", f.stem),
                    "created_at": data.get("created_at", ""),
                    "last_active": data.get("last_active", ""),
                    "summary": data.get("summary", ""),
                    "message_count": data.get("message_count", 0),
                })
            except Exception:
                continue
            if len(result) >= limit:
                break
        return result

    def get_latest_session_id(self) -> str | None:
        files = sorted(self.sessions_dir.glob("*.json"), reverse=True)
        if files:
            try:
                with open(files[0], "r", encoding="utf-8") as f:
                    data = json.load(f)
                return data.get("session_id", files[0].stem)
            except Exception:
                pass
        return None

    def update_summary(self, summary: str) -> None:
        if not self.current_session_id:
            return
        path = self.sessions_dir / f"{self.current_session_id}.json"
        if not path.exists():
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            data["summary"] = summary
            _atomic_write(path, data)
        except Exception:
            pass

    def generate_summary(self, conversation_history: list[dict], client: LLMProvider) -> str:
        if len(conversation_history) < 2:
            return ""
        snippets = []
        for msg in conversation_history[:4] + conversation_history[-2:]:
            for block in msg.get("content", []):
                if "text" in block:
                    snippets.append(f"{msg['role']}: {block['text'][:200]}")
        conversation_text = "\n".join(snippets)[:1500]

        return client.quick_chat(
            f"Summarize this SeaTunnel conversation in one sentence (max 80 chars, "
            f"language should match the conversation):\n\n{conversation_text}",
            system="Output ONLY the summary sentence, nothing else.",
            use_fast_model=True,
        ).strip()


# ─── Memory Store ───


class MemoryStore:
    """Persistent cross-session memory store."""

    MEMORY_TYPES = ("connection", "preference", "project")

    def __init__(self, base_dir: Path):
        self.memory_file = base_dir / "memory.json"
        self._memories: list[dict] = []
        self._next_id: int = 1
        self._load()

    def _load(self) -> None:
        if self.memory_file.exists():
            try:
                with open(self.memory_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._memories = data.get("memories", [])
                if self._memories:
                    max_id = max(
                        int(m["id"].removeprefix("mem_"))
                        for m in self._memories
                        if m.get("id", "").startswith("mem_")
                    )
                    self._next_id = max_id + 1
            except Exception:
                self._memories = []

    def _save(self) -> None:
        self.memory_file.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(self.memory_file, {
            "version": 1,
            "memories": self._memories,
        })

    def add(
        self,
        content: str,
        memory_type: str = "project",
        source: str = "explicit",
    ) -> str:
        mem_id = f"mem_{self._next_id:03d}"
        self._next_id += 1
        self._memories.append({
            "id": mem_id,
            "type": memory_type if memory_type in self.MEMORY_TYPES else "project",
            "content": content,
            "created_at": _now_iso(),
            "source": source,
        })
        self._save()
        return mem_id

    def remove(self, memory_id: str) -> bool:
        before = len(self._memories)
        self._memories = [m for m in self._memories if m["id"] != memory_id]
        if len(self._memories) < before:
            self._save()
            return True
        return False

    def clear(self) -> int:
        """Remove all memories. Returns the number of entries removed."""
        count = len(self._memories)
        self._memories.clear()
        self._next_id = 1
        self._save()
        return count

    def get_all(self) -> list[dict]:
        return list(self._memories)

    def get_by_type(self, memory_type: str) -> list[dict]:
        return [m for m in self._memories if m["type"] == memory_type]

    def needs_compaction(self, threshold: int = 20) -> bool:
        """Check if memory count exceeds the compaction threshold."""
        return len(self._memories) >= threshold

    def compact(self, client: LLMProvider, threshold: int = 20) -> int:
        """Compact memories using LLM when count exceeds threshold.

        Merges similar memories per type into fewer, denser entries.
        Returns the number of entries removed (before - after).
        """
        if len(self._memories) < threshold:
            return 0

        compacted = compact_memories(self._memories, client)
        if not compacted or len(compacted) >= len(self._memories):
            return 0

        removed = len(self._memories) - len(compacted)
        self._memories = compacted
        # Reset IDs sequentially
        for i, m in enumerate(self._memories, 1):
            m["id"] = f"mem_{i:03d}"
        self._next_id = len(self._memories) + 1
        self._save()
        return removed

    def format_for_prompt(self, max_tokens: int = 800) -> str:
        if not self._memories:
            return ""

        sections: dict[str, list[str]] = {}
        titles = {
            "connection": "Connections (hosts, credentials, URLs)",
            "project": "Project Context",
            "preference": "User Preferences",
        }
        priority_order = ["connection", "project", "preference"]

        for mtype in priority_order:
            items = [m["content"] for m in self._memories if m["type"] == mtype]
            if items:
                sections[mtype] = items

        if not sections:
            return ""

        lines = ["## User Context (from memory)\n"]
        char_budget = max_tokens * 4
        used = len(lines[0])

        for mtype in priority_order:
            items = sections.get(mtype, [])
            if not items:
                continue
            header = f"### {titles.get(mtype, mtype.title())}"
            if used + len(header) + 5 > char_budget:
                break
            lines.append(header)
            used += len(header)
            for item in items:
                entry = f"- {item}"
                if used + len(entry) + 2 > char_budget:
                    break
                lines.append(entry)
                used += len(entry) + 1

        return "\n".join(lines)


# ─── Auto-extraction ───


def extract_memories(
    conversation_history: list[dict],
    existing_memories: list[dict],
    client: LLMProvider,
) -> list[dict]:
    """Extract new facts from conversation to remember across sessions."""
    if len(conversation_history) < 2:
        return []

    recent = conversation_history[-6:]
    snippets = []
    for msg in recent:
        for block in msg.get("content", []):
            if "text" in block:
                snippets.append(f"{msg['role']}: {block['text'][:300]}")
    conversation_text = "\n".join(snippets)[:2000]

    existing_text = "\n".join(f"- [{m['type']}] {m['content']}" for m in existing_memories) or "(none)"

    prompt = (
        f"Existing memories (DO NOT duplicate):\n{existing_text}\n\n"
        f"Conversation:\n{conversation_text}\n\n"
        f"Extract NEW concrete facts. Return JSON array: "
        f'[{{"content": "...", "type": "connection|preference|project"}}]\n'
        f"Return [] if nothing new."
    )
    system = (
        "You extract facts from SeaTunnel CLI conversations to remember across sessions.\n"
        "Categories:\n"
        "- connection: specific hosts, ports, JDBC URLs, broker addresses, credentials\n"
        "- preference: language preference, default settings, common patterns\n"
        "- project: what databases/systems are used, architecture facts\n"
        "Only extract SPECIFIC, CONCRETE facts. Return valid JSON array only."
    )

    try:
        raw = client.quick_chat(prompt, system=system, use_fast_model=True)
        start = raw.find("[")
        end = raw.rfind("]")
        if start >= 0 and end > start:
            items = json.loads(raw[start:end + 1])
            return [
                item for item in items
                if isinstance(item, dict) and "content" in item and "type" in item
            ]
    except Exception as e:
        logger.debug(f"Memory extraction failed: {e}")
    return []


def compact_memories(
    memories: list[dict],
    client: LLMProvider,
) -> list[dict]:
    """Merge memories into a smaller set using LLM summarization.

    Groups by type, asks the LLM to consolidate duplicates and merge related
    facts, then returns the compacted list with fresh timestamps.
    """
    if len(memories) < 5:
        return list(memories)

    formatted = "\n".join(
        f"- [{m['type']}] {m['content']}" for m in memories
    )

    prompt = (
        f"Current memories ({len(memories)} entries):\n{formatted}\n\n"
        f"Consolidate these into the FEWEST entries possible:\n"
        f"- Merge duplicates and near-duplicates into one entry\n"
        f"- Combine related facts of the same type (e.g. multiple connection details for the same system)\n"
        f"- Keep ALL concrete information (hosts, ports, URLs, specific settings) — do not lose detail\n"
        f"- Remove outdated entries if a newer one supersedes them\n"
        f"- Target: ≤10 entries total\n\n"
        f'Return JSON array: [{{"content": "...", "type": "connection|preference|project"}}]'
    )
    system = (
        "You consolidate SeaTunnel CLI memory entries. "
        "Preserve every concrete fact (hosts, ports, URLs, credentials, specific settings). "
        "Merge related items, remove true duplicates. Return valid JSON array only."
    )

    try:
        raw = client.quick_chat(prompt, system=system, use_fast_model=True)
        start = raw.find("[")
        end = raw.rfind("]")
        if start >= 0 and end > start:
            items = json.loads(raw[start : end + 1])
            now = _now_iso()
            return [
                {
                    "id": f"mem_{i:03d}",
                    "type": item["type"] if item.get("type") in MemoryStore.MEMORY_TYPES else "project",
                    "content": item["content"],
                    "created_at": now,
                    "source": "compacted",
                }
                for i, item in enumerate(items, 1)
                if isinstance(item, dict) and "content" in item
            ]
    except Exception as e:
        logger.debug(f"Memory compaction failed: {e}")
    return list(memories)
