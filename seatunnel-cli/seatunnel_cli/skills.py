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

"""Skill framework — three-layer config generation.

Layers:
  1. Skill (scenario SOP)     — loaded from ``skills/*.md``
  2. Golden Example            — loaded from ``golden_examples/*.md``
  3. Connector Metadata        — fetched via ``connectors.py`` at runtime

Flow:  Planner → SkillRouter.match() → expand_pipelines → SkillExecutor.build_enriched_prompt()
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from .memory import MemoryStore

logger = logging.getLogger(__name__)

# ─── Data structures ────────────────────────────────────────────────────────


@dataclass
class PipelineSlot:
    pipeline_id: str
    source_connector: str
    sink_connector: str
    tables: list[str] = field(default_factory=list)
    transform: str | None = None
    routing_label: str = ""
    mode: str = "BATCH"
    database: str = ""
    source_options: dict = field(default_factory=dict)
    sink_options: dict = field(default_factory=dict)
    missing_slots: list[str] = field(default_factory=list)


@dataclass
class StructuredPlan:
    pipelines: list[PipelineSlot]
    env_options: dict = field(default_factory=dict)
    shared_context: dict = field(default_factory=dict)
    raw_plan_text: str = ""


# ─── Markdown helpers (shared by skills/*.md and golden_examples/*.md) ────


def _parse_frontmatter(text: str) -> tuple[dict, str]:
    """Parse ``---`` YAML frontmatter. Supports scalars, booleans, and lists.

    Skips leading HTML comment blocks (ASF license headers) before looking
    for the ``---`` frontmatter delimiter.
    """
    # Strip leading HTML comments (license headers)
    content = re.sub(r"\A\s*<!--.*?-->\s*", "", text, count=1, flags=re.DOTALL)
    if not content.startswith("---"):
        return {}, text
    parts = content.split("---", 2)
    if len(parts) < 3:
        return {}, text

    meta: dict = {}
    current_key: str | None = None
    current_list: list[str] | None = None

    for line in parts[1].strip().splitlines():
        stripped = line.strip()
        if stripped.startswith("- ") and current_key is not None:
            if current_list is None:
                current_list = []
            current_list.append(stripped[2:].strip())
            continue
        if current_key and current_list is not None:
            meta[current_key] = current_list
            current_list = None
            current_key = None
        if ":" in line:
            k, v = line.split(":", 1)
            k, v = k.strip(), v.strip()
            if v:
                meta[k] = v.lower() == "true" if v.lower() in ("true", "false") else v
            else:
                current_key = k
    if current_key and current_list is not None:
        meta[current_key] = current_list

    return meta, parts[2]


def _extract_section(body: str, heading: str) -> str:
    """Extract markdown section content under *heading* until next ``##``."""
    m = re.search(rf"(?:^|\n){re.escape(heading)}\s*\n(.*?)(?=\n## |\Z)", body, re.DOTALL)
    return m.group(1).strip() if m else ""


def _extract_hocon_block(body: str, heading: str) -> str:
    """Extract first ```hocon block after *heading*."""
    idx = body.find(heading)
    if idx == -1:
        return ""
    m = re.search(r"```hocon\s*\n(.*?)```", body[idx:], re.DOTALL)
    return m.group(1).rstrip() if m else ""


# ─── Golden Examples ─────────────────────────────────────────────────────────

_EXAMPLES_DIR = Path(__file__).parent / "golden_examples"
_PAIR_CACHE: dict[str, dict] | None = None


def _load_pair_examples() -> dict[str, dict]:
    global _PAIR_CACHE
    if _PAIR_CACHE is not None:
        return _PAIR_CACHE
    _PAIR_CACHE = {}
    if not _EXAMPLES_DIR.is_dir():
        return _PAIR_CACHE
    for md in sorted(_EXAMPLES_DIR.glob("*.md")):
        if md.name.startswith("_"):
            continue
        try:
            meta, body = _parse_frontmatter(md.read_text(encoding="utf-8"))
        except OSError:
            continue
        src, sink = meta.get("source", ""), meta.get("sink", "")
        if src and sink:
            _PAIR_CACHE[f"{src}->{sink}"] = {
                "description": meta.get("description", ""),
                "notes": meta.get("notes", ""),
                "source_tpl": _extract_hocon_block(body, "## Source Template"),
                "sink_tpl": _extract_hocon_block(body, "## Sink Template"),
            }
    return _PAIR_CACHE



# ─── Plan parser ─────────────────────────────────────────────────────────────


def parse_structured_plan(plan_text: str) -> StructuredPlan:
    """Parse Planner output — JSON first, legacy regex fallback."""
    return _try_parse_json_plan(plan_text) or _parse_legacy_plan(plan_text)


def _try_parse_json_plan(plan_text: str) -> StructuredPlan | None:
    m = re.search(r"```json\s*\n(.*?)```", plan_text, re.DOTALL)
    json_str = m.group(1).strip() if m else None
    if not json_str:
        m = re.search(r"\{[\s\S]*\"pipelines\"[\s\S]*\}", plan_text)
        json_str = m.group(0) if m else None
    if not json_str:
        return None
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        return None
    if not isinstance(data.get("pipelines"), list):
        return None

    env_opts = data.get("env", {})
    shared = data.get("shared", {})
    mode = env_opts.get("mode", "BATCH")
    pipelines = []
    for p in data["pipelines"]:
        src = p.get("source", {})
        sink = p.get("sink", {})
        tables = p.get("tables", [])
        if tables is None:
            tables = []
        elif isinstance(tables, str):
            tables = [tables]
        pipelines.append(PipelineSlot(
            pipeline_id=p.get("id", f"pipeline_{len(pipelines) + 1}"),
            source_connector=src.get("connector", "") if isinstance(src, dict) else str(src),
            sink_connector=sink.get("connector", "") if isinstance(sink, dict) else str(sink),
            tables=tables,
            transform=p.get("transform"),
            mode=mode,
            database=p.get("database", shared.get("database", "")),
        ))
    if not pipelines:
        return None
    return StructuredPlan(pipelines=pipelines, env_options=env_opts,
                          shared_context=shared, raw_plan_text=plan_text)


def _parse_legacy_plan(plan_text: str) -> StructuredPlan:
    source_conn = sink_conn = ""
    mode = "BATCH"
    transform = None
    for line in plan_text.split("\n"):
        s = line.strip().lstrip("- ")
        if m := re.match(r"(?i)source:\s*(\S+)", s):
            source_conn = m.group(1).rstrip(",")
        elif m := re.match(r"(?i)sink:\s*(\S+)", s):
            sink_conn = m.group(1).rstrip(",")
        elif m := re.match(r"(?i)transform:\s*(\S+)", s):
            v = m.group(1).rstrip(",")
            if v.lower() not in ("none", "n/a", "-"):
                transform = v
        elif m := re.match(r"(?i)mode:\s*(\S+)", s):
            mode = m.group(1).strip().upper()
    if not source_conn or not sink_conn:
        return StructuredPlan(pipelines=[], raw_plan_text=plan_text)
    return StructuredPlan(
        pipelines=[PipelineSlot("pipeline_1", source_conn, sink_conn,
                                transform=transform, mode=mode)],
        env_options={"mode": mode}, raw_plan_text=plan_text)


# ─── Slot filling & pipeline expansion ───────────────────────────────────────


def _collect_required_options(
    connector_name: str, connector_type: str,
) -> list[dict]:
    """Collect absolutely_required option metadata for a connector.

    Pure data collection — no matching or filtering logic.
    Returns list of option dicts with keys: key, type, description.
    """
    from .connectors import fetch_connector_metadata

    meta = fetch_connector_metadata(connector_name, connector_type)
    if not meta:
        return []

    # Option types that are complex structures generated by the LLM, not
    # user-provided scalar values.  Don't ask the user for these.
    _LLM_GENERATED_TYPES = {"object", "map", "list<object>", "list<map>"}
    _LLM_GENERATED_PREFIXES = ("map<", "list<object", "list<map")

    result: list[dict] = []
    for opt in meta.get("required", []):
        if opt.get("rule_type") != "absolutely_required":
            continue
        key = opt.get("key", "")
        if not key:
            continue
        opt_type = opt.get("type", "").lower()
        if opt_type in _LLM_GENERATED_TYPES or opt_type.startswith(_LLM_GENERATED_PREFIXES):
            continue
        result.append({
            "key": key,
            "type": opt.get("type", ""),
            "description": opt.get("description", key),
            "connector": connector_name,
            "connector_type": connector_type,
        })
    return result


def _transform_connector_name(transform: object) -> str:
    """Return the transform plugin name from a structured or string plan field."""
    if not transform:
        return ""
    if isinstance(transform, dict):
        for key in ("connector", "name", "type"):
            value = transform.get(key)
            if value:
                return str(value)
        return ""
    return str(transform)


def _metadata_targets_for_pipeline(pipeline: PipelineSlot) -> list[tuple[str, str]]:
    """Return source, sink, and transform metadata targets for a pipeline."""
    targets = [
        (pipeline.source_connector, "source"),
        (pipeline.sink_connector, "sink"),
    ]
    transform_name = _transform_connector_name(pipeline.transform)
    if transform_name:
        targets.append((transform_name, "transform"))
    return [(name, ctype) for name, ctype in targets if name]


def llm_check_missing_info(
    client,
    user_request: str,
    required_options: list[dict],
    shared: dict,
    memory_ctx: str | None,
) -> list[str]:
    """Use LLM to determine which required options the user has NOT yet provided.

    Sends a lightweight prompt (via fast model) asking the LLM to judge which
    options are missing.  Returns human-readable prompts for each missing
    option, or an empty list if everything is covered.
    """
    if not required_options:
        return []

    context_parts = [f"User request: {user_request}"]
    if shared:
        context_parts.append(
            "Shared context: " + ", ".join(f"{k}={v}" for k, v in shared.items() if v)
        )
    if memory_ctx:
        context_parts.append(f"Memory: {memory_ctx}")
    context_block = "\n".join(context_parts)

    options_block = "\n".join(
        f"- `{o['key']}` ({o['connector_type']}.{o['connector']}): {o['description']}"
        for o in required_options
    )

    system = (
        "You are a SeaTunnel configuration assistant. Your job is to determine "
        "which required connector options the user has NOT yet provided — either "
        "explicitly or implicitly.\n\n"
        "IMPORTANT: Users may provide information in ANY format — natural "
        "language, CLI commands (e.g., `mysql -h host -P port -uuser -ppass db`), "
        "partial config snippets, or conversational text. Use your understanding "
        "to judge whether the information is present, even if the exact option "
        "key name is not mentioned.\n\n"
        "Examples of IMPLICIT provision:\n"
        "- 'mysql -h 127.0.0.1 -P 3306 -uroot -proot123 mydb' → provides "
        "url (host+port+db), driver (mysql implies com.mysql.cj.jdbc.Driver), "
        "username, password\n"
        "- 'sync MySQL tables' → provides driver (MySQL type is known)\n"
        "- 'Kafka topic orders on broker1:9092' → provides bootstrap.servers, topic\n\n"
        "Respond with ONLY a JSON array of option keys that are STILL MISSING. "
        "If ALL options are provided (explicitly or implicitly), respond with `[]`.\n"
        "Example: [\"url\", \"driver\"]\n"
        "Example: []"
    )

    prompt = (
        f"## User Context\n{context_block}\n\n"
        f"## Required Options\n{options_block}\n\n"
        "Which of these required options has the user NOT yet provided "
        "(explicitly or implicitly)? Return a JSON array of missing keys only."
    )

    try:
        reply = client.quick_chat(prompt, system=system, use_fast_model=True)
        # Parse JSON array from response
        import re as _re
        m = _re.search(r'\[.*?\]', reply, _re.DOTALL)
        if not m:
            return []
        missing_keys = json.loads(m.group())
        if not isinstance(missing_keys, list):
            return []
    except Exception:
        # If LLM call fails, don't block — return empty (let config gen proceed)
        return []

    # Map missing keys back to human-readable prompts
    key_to_opt = {o["key"]: o for o in required_options}
    missing: list[str] = []
    for key in missing_keys:
        if key in key_to_opt:
            o = key_to_opt[key]
            missing.append(
                f"{o['connector_type']}.{o['connector']}: `{key}` — {o['description']}"
            )
    return missing


def _make_routing_label(connector: str, tables: list[str], pid: str) -> str:
    if tables:
        return f"{connector.lower()}_{'_'.join(t.replace(' ', '_') for t in tables[:3])}"
    return f"{connector.lower()}_{pid}"


def fill_pipeline_slots(slot: PipelineSlot, user_request: str,
                        shared: dict, memory_ctx: str | None = None) -> None:
    """Fill routing labels and database defaults for a pipeline slot."""
    slot.database = slot.database or shared.get("database", "")

    if not slot.routing_label:
        slot.routing_label = _make_routing_label(
            slot.source_connector, slot.tables, slot.pipeline_id)

    slot.missing_slots = []


def expand_pipelines(plan: StructuredPlan) -> StructuredPlan:
    """Expand N-table pipelines into N sub-pipelines when sink is single-table."""
    from .connectors import sink_supports_multi_table
    expanded: list[PipelineSlot] = []
    for p in plan.pipelines:
        if len(p.tables) > 1 and not sink_supports_multi_table(p.sink_connector):
            for i, table in enumerate(p.tables, 1):
                expanded.append(PipelineSlot(
                    pipeline_id=f"{p.pipeline_id}_t{i}",
                    source_connector=p.source_connector,
                    sink_connector=p.sink_connector,
                    tables=[table], transform=p.transform, mode=p.mode,
                    database=p.database,
                    source_options=dict(p.source_options),
                    sink_options=dict(p.sink_options),
                ))
        else:
            expanded.append(p)
    return StructuredPlan(pipelines=expanded, env_options=plan.env_options,
                          shared_context=plan.shared_context,
                          raw_plan_text=plan.raw_plan_text)


# ─── Scenario Skills (loaded from skills/*.md) ──────────────────────────────

_SKILLS_DIR = Path(__file__).parent / "skills"


@dataclass
class ScenarioSkill:
    name: str
    description: str
    triggers: list[str]
    composable: bool
    domain_knowledge: str
    sop: str
    constraints: str
    pattern: str

    @classmethod
    def from_markdown(cls, path: Path) -> ScenarioSkill:
        meta, body = _parse_frontmatter(path.read_text(encoding="utf-8"))
        triggers = meta.get("triggers", [])
        if isinstance(triggers, str):
            triggers = [t.strip() for t in triggers.split(",")]
        composable = meta.get("composable", True)
        if isinstance(composable, str):
            composable = composable.lower() == "true"
        return cls(
            name=meta.get("name", path.stem),
            description=meta.get("description", ""),
            triggers=triggers, composable=composable,
            domain_knowledge=_extract_section(body, "## Domain Knowledge"),
            sop=_extract_section(body, "## SOP"),
            constraints=_extract_section(body, "## Constraints"),
            pattern=_extract_hocon_block(body, "## Pattern"),
        )


_SKILL_CACHE: list[ScenarioSkill] | None = None


def _load_all_skills() -> list[ScenarioSkill]:
    global _SKILL_CACHE
    if _SKILL_CACHE is not None:
        return _SKILL_CACHE
    _SKILL_CACHE = []
    if not _SKILLS_DIR.is_dir():
        return _SKILL_CACHE
    for md in sorted(_SKILLS_DIR.glob("*.md")):
        try:
            _SKILL_CACHE.append(ScenarioSkill.from_markdown(md))
        except Exception as e:
            logger.warning("Failed to load skill %s: %s", md.name, e)
    return _SKILL_CACHE


# ─── Skill Router ────────────────────────────────────────────────────────────


_MAX_SKILLS = 2  # Cap injected skills to control token budget


class SkillRouter:
    @staticmethod
    def match(plan: StructuredPlan, user_input: str) -> list[ScenarioSkill]:
        """Score skills by trigger hits, apply structural rules.

        Returns at most ``_MAX_SKILLS`` matched skills to keep the enriched
        prompt within a reasonable token budget (~1500 tokens for SOPs).
        """
        combined = (user_input + "\n" + plan.raw_plan_text).lower()
        scored: list[tuple[int, ScenarioSkill]] = []
        for skill in _load_all_skills():
            score = sum(1 for t in skill.triggers if t.lower() in combined)
            scored.append((score, skill))

        # Auto-activate multi_pipeline for 2+ pipelines
        if len(plan.pipelines) >= 2:
            scored = [(max(s, 3) if sk.name == "multi_pipeline" else s, sk)
                      for s, sk in scored]

        # Default to batch_sync if no mode-specific skill matched
        mode_skills = {"cdc_realtime", "file_etl", "cross_database", "data_quality"}
        if not any(s > 0 and sk.name in mode_skills for s, sk in scored):
            scored = [(max(s, 1) if sk.name == "batch_sync" else s, sk)
                      for s, sk in scored]

        matched = [sk for s, sk in sorted(scored, key=lambda x: -x[0]) if s > 0]
        if not matched:
            for sk in _load_all_skills():
                if sk.name == "batch_sync":
                    matched = [sk]
                    break
        return matched[:_MAX_SKILLS]


# ─── Skill Executor ──────────────────────────────────────────────────────────


class SkillExecutor:
    """Orchestrates: expand → fill → fetch metadata → build enriched prompt."""

    def __init__(self, plan: StructuredPlan, skills: list[ScenarioSkill]):
        self.plan = plan
        self.skills = skills

    def fill_and_check(self, user_request: str,
                       memory_store: MemoryStore | None = None,
                       client=None) -> list[str]:
        self.plan = expand_pipelines(self.plan)
        mem_ctx = memory_store.format_for_prompt(max_tokens=300) if memory_store else None

        # Step 1: Fill routing labels / defaults for each pipeline
        for p in self.plan.pipelines:
            fill_pipeline_slots(p, user_request, self.plan.shared_context, mem_ctx)

        # Step 2: Collect all required options across pipelines (deduplicated)
        all_required: list[dict] = []
        seen_keys: set[str] = set()
        for p in self.plan.pipelines:
            for connector, ctype in _metadata_targets_for_pipeline(p):
                for opt in _collect_required_options(connector, ctype):
                    if opt["key"] not in seen_keys:
                        seen_keys.add(opt["key"])
                        all_required.append(opt)

        if not all_required or not client:
            return []

        # Step 3: Single LLM call (fast model) to judge what's missing
        return llm_check_missing_info(
            client, user_request, all_required,
            self.plan.shared_context, mem_ctx,
        )

    def fetch_all_metadata(self, on_status: Callable) -> str:
        from .connectors import fetch_connector_metadata, format_metadata_for_prompt
        seen: set[tuple[str, str]] = set()
        sections: list[str] = []
        for p in self.plan.pipelines:
            for name, ctype in _metadata_targets_for_pipeline(p):
                if (name, ctype) in seen:
                    continue
                seen.add((name, ctype))
                on_status("fetching", f"Fetching {ctype} options for {name}...")
                meta = fetch_connector_metadata(name, ctype)
                sections.append(
                    format_metadata_for_prompt(meta, name, ctype) if meta
                    else f"### {name} ({ctype.upper()}) — no metadata available")
        if not sections:
            return ""
        return ("\n## Connector Option Rules (fetched for this request)\n"
                "Use EXACTLY these option keys.\n\n" + "\n\n".join(sections))

    def build_enriched_prompt(self, user_request: str, on_status: Callable,
                              memory_store: MemoryStore | None = None) -> str:
        parts = [f"## User Request:\n{user_request}"]

        # Layer 1: Skill SOPs
        for skill in self.skills:
            parts.append(f"\n## Scenario: {skill.name} — {skill.description}")
            if skill.domain_knowledge:
                parts.append(f"\n### Domain Knowledge\n{skill.domain_knowledge}")
            if skill.sop:
                parts.append(f"\n### SOP\n{skill.sop}")
            if skill.constraints:
                parts.append(f"\n### Constraints\n{skill.constraints}")
            if skill.pattern:
                parts.append(f"\n### Pattern\n```hocon\n{skill.pattern}\n```")

        # Layer 2: Pipeline structure
        pipelines = self.plan.pipelines
        parts.append(f"\n## Pipeline Structure ({len(pipelines)} pipeline(s)):")
        for p in pipelines:
            parts.append(f"\n### {p.pipeline_id}: {p.source_connector} → {p.sink_connector}")
            parts.append(f"- Source: {p.source_connector}")
            if p.tables:
                parts.append(f"  - Tables: {', '.join(p.tables)}")
            if p.database:
                parts.append(f"  - Database: {p.database}")
            parts.append(f'  - plugin_output: "{p.routing_label}"')
            parts.append(f"- Sink: {p.sink_connector}")
            parts.append(f'  - plugin_input: "{p.routing_label}"')
            if p.transform:
                parts.append(f"- Transform: {p.transform}")

        if len(pipelines) > 1:
            parts.append("\n## Routing Table:")
            parts.append("| Pipeline | plugin_output | plugin_input |")
            parts.append("|----------|---------------|--------------|")
            for p in pipelines:
                parts.append(f"| {p.pipeline_id} | {p.routing_label} | {p.routing_label} |")

        # Environment
        mode = self.plan.env_options.get("mode", "BATCH")
        parts.append(f"\n## Environment:\n- job.mode: {mode}")
        parts.append(f"- parallelism: {self.plan.env_options.get('parallelism', 2)}")
        if mode == "STREAMING":
            parts.append("- checkpoint.interval: 10000")

        # Layer 3: Golden examples (raw, not pre-filled — LLM uses as structural reference)
        self._append_golden_examples(parts)

        parts.append("\nGenerate the SeaTunnel HOCON config now. Use tools if needed.")
        return "\n".join(parts)

    def _append_golden_examples(self, parts: list[str]) -> None:
        seen: set[tuple[str, str]] = set()
        for p in self.plan.pipelines:
            key = (p.source_connector, p.sink_connector)
            if key in seen:
                continue
            seen.add(key)
            ex = _load_pair_examples().get(f"{key[0]}->{key[1]}")
            if not ex:
                continue
            parts.append(f"\n## Reference: {ex['description']}")
            if ex["source_tpl"]:
                parts.append(f"```hocon\n{ex['source_tpl']}\n```")
            if ex["sink_tpl"]:
                parts.append(f"```hocon\n{ex['sink_tpl']}\n```")
            if ex.get("notes"):
                parts.append(f"Note: {ex['notes']}")
