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

"""Multi-agent system for SeaTunnel config generation.

Architecture:
  PlannerAgent     → Analyzes intent, decides if clarification needed
  ConfigAgent      → Generates HOCON config based on plan
  ValidatorAgent   → Validates config syntax and semantics
  DryRunValidator  → Invokes seatunnel.sh --check or REST API for engine-level validation
  Orchestrator     → Coordinates agents in a loop (max 3 correction rounds)
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Callable, TYPE_CHECKING

from .llm_provider import LLMProvider
from .connectors import (
    get_connector_catalog, get_connector_detail, list_connector_names,
    route_by_keyword, validate_connector_options,
)

if TYPE_CHECKING:
    from .memory import MemoryStore

logger = logging.getLogger(__name__)


# ─── Tool definitions for Bedrock Converse API ───

TOOLS = [
    {
        "toolSpec": {
            "name": "list_connectors",
            "description": "List all available SeaTunnel connectors categorized by type (source/sink/transform).",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {},
                }
            },
        }
    },
    {
        "toolSpec": {
            "name": "get_connector_info",
            "description": "Get detailed info about a specific connector including parameters and examples.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "connector_name": {
                            "type": "string",
                            "description": "Name of the connector, e.g. 'Jdbc', 'Kafka', 'S3File'",
                        }
                    },
                    "required": ["connector_name"],
                }
            },
        }
    },
    {
        "toolSpec": {
            "name": "route_connectors",
            "description": "Given a user's natural language description, find the most relevant SeaTunnel connectors. Use this FIRST to narrow down which connectors to look up, before calling get_connector_info.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "user_text": {
                            "type": "string",
                            "description": "The user's request text or keywords (e.g., 'mysql to s3', 'kafka cdc')",
                        }
                    },
                    "required": ["user_text"],
                }
            },
        }
    },
    {
        "toolSpec": {
            "name": "validate_config",
            "description": "Validate a SeaTunnel HOCON config string for syntax and required fields.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "config": {
                            "type": "string",
                            "description": "The HOCON config string to validate",
                        }
                    },
                    "required": ["config"],
                }
            },
        }
    },
    {
        "toolSpec": {
            "name": "ask_user",
            "description": "Ask the user a clarifying question when the request is ambiguous or missing critical information (e.g., connection details, specific table names, data format). Use this when you cannot make a reasonable default assumption.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "The clarifying question to ask the user",
                        }
                    },
                    "required": ["question"],
                }
            },
        }
    },
]


def _handle_tool_call(tool_name: str, tool_input: dict) -> str:
    """Execute a tool call and return the result string."""
    if tool_name == "list_connectors":
        result = list_connector_names()
        return json.dumps(result, indent=2)

    elif tool_name == "get_connector_info":
        name = tool_input.get("connector_name", "")
        detail = get_connector_detail(name)
        return detail or f"Connector '{name}' not found. Use list_connectors to see available options."

    elif tool_name == "route_connectors":
        text = tool_input.get("user_text", "")
        matches = route_by_keyword(text)
        if matches:
            return f"Relevant connectors for '{text}': {', '.join(matches)}\nCall get_connector_info for each to get full option details."
        return f"No direct keyword match for '{text}'. Use list_connectors to browse all 81 connectors."

    elif tool_name == "validate_config":
        config_str = tool_input.get("config", "")
        return validate_hocon(config_str)

    elif tool_name == "ask_user":
        # This is handled specially by the orchestrator
        return "__ASK_USER__"

    return f"Unknown tool: {tool_name}"


def _flatten_hocon_keys(conf, prefix: str = "") -> set[str]:
    """Flatten pyhocon nested keys back to dotted form.

    pyhocon parses `bootstrap.servers = "x"` as nested {bootstrap: {servers: "x"}}.
    This function recovers the original dotted key: "bootstrap.servers".
    Also handles quoted keys like `"bootstrap.servers"` which pyhocon keeps as-is.
    """
    keys = set()
    try:
        if hasattr(conf, "keys"):
            items = conf
        elif isinstance(conf, dict):
            items = conf
        else:
            return keys

        for k in items:
            full_key = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
            # Clean quoted keys
            clean_key = full_key.strip('"')
            child = items[k]
            if hasattr(child, "keys") or isinstance(child, dict):
                # Could be a nested dotted key OR a genuine nested config block
                # Add both the parent key and recursively flatten
                keys.add(clean_key)
                keys.update(_flatten_hocon_keys(child, clean_key))
            else:
                keys.add(clean_key)
    except Exception:
        pass
    return keys


def validate_hocon(config_str: str) -> str:
    """Validate HOCON config — syntax, structure, and connector-level required params."""
    errors = []
    warnings = []

    # ── 1. Basic structure checks ──
    if "env" not in config_str and "env {" not in config_str:
        warnings.append("Missing 'env' block. Recommended to set job.mode and parallelism.")

    if "source" not in config_str and "source {" not in config_str:
        errors.append("Missing 'source' block. Every SeaTunnel job needs at least one source.")

    if "sink" not in config_str and "sink {" not in config_str:
        errors.append("Missing 'sink' block. Every SeaTunnel job needs at least one sink.")

    # Brace matching
    open_braces = config_str.count("{")
    close_braces = config_str.count("}")
    if open_braces != close_braces:
        errors.append(f"Unmatched braces: {open_braces} opening vs {close_braces} closing.")

    # ── 2. HOCON syntax parse ──
    parsed = None
    try:
        from pyhocon import ConfigFactory
        parsed = ConfigFactory.parse_string(config_str)
    except ImportError:
        pass
    except Exception as e:
        errors.append(f"HOCON parse error: {e}")

    # ── 3. Connector-level required params check (uses catalog) ──
    if parsed is not None:
        for section in ["source", "sink"]:
            try:
                section_conf = parsed.get(section, None)
                if section_conf is None:
                    continue
                for connector_name in section_conf:
                    connector_conf = section_conf[connector_name]
                    provided_keys = _flatten_hocon_keys(connector_conf)
                    result = validate_connector_options(connector_name, provided_keys)
                    if result.get("missing_required"):
                        missing = ", ".join(result["missing_required"])
                        errors.append(f"{section}.{connector_name}: missing required options: {missing}")
            except Exception:
                pass

        # Check STREAMING mode needs checkpoint.interval
        try:
            env = parsed.get("env", {})
            if hasattr(env, "get"):
                mode = env.get("job.mode", "")
                if isinstance(mode, str) and mode.upper() == "STREAMING":
                    interval = env.get("checkpoint.interval", None)
                    if interval is None:
                        warnings.append("STREAMING job should set env.checkpoint.interval (e.g., 10000)")
        except Exception:
            pass

    # ── 4. Security checks ──
    if '""' in config_str:
        warnings.append("Found empty string value. Make sure this is intentional.")

    password_pattern = re.compile(r'password\s*=\s*"(?!\$\{)[^"]{3,}"', re.IGNORECASE)
    if password_pattern.search(config_str):
        warnings.append("Hardcoded password detected. Consider using environment variable: ${PASSWORD}")

    # ── Result ──
    if errors:
        return "INVALID\n" + "\n".join(f"ERROR: {e}" for e in errors) + (
            "\n" + "\n".join(f"WARNING: {w}" for w in warnings) if warnings else ""
        )
    elif warnings:
        return "VALID (with warnings)\n" + "\n".join(f"WARNING: {w}" for w in warnings)
    else:
        return "VALID"


# ─── Dry-run validation ───

def _find_seatunnel_sh() -> str | None:
    """Locate seatunnel.sh script."""
    seatunnel_home = os.environ.get("SEATUNNEL_HOME", "")
    if seatunnel_home:
        path = os.path.join(seatunnel_home, "bin", "seatunnel.sh")
        if os.path.exists(path):
            return path

    # Try relative path from project root
    project_root = Path(__file__).parent.parent.parent
    path = str(project_root / "bin" / "seatunnel.sh")
    if os.path.exists(path):
        return path
    return None


def dry_run_config(config_str: str) -> dict:
    """Perform dry-run validation of a SeaTunnel HOCON config.

    Three-phase validation:
      Phase 1: Local validation (HOCON syntax + required params + security)
      Phase 2: Engine --check mode (if seatunnel.sh available)
      Phase 3: Engine REST API job validation (if engine is running)

    Returns:
        {
            "valid": bool,
            "phase1_local": str,       # local validation result
            "phase2_check": str|None,  # --check result (if available)
            "phase3_api": str|None,    # REST API result (if available)
            "summary": str,            # human-readable summary
        }
    """
    result = {
        "valid": False,
        "phase1_local": "",
        "phase2_check": None,
        "phase3_api": None,
        "summary": "",
    }

    # ── Phase 1: Local validation ──
    local_result = validate_hocon(config_str)
    result["phase1_local"] = local_result

    if local_result.startswith("INVALID"):
        result["summary"] = f"Dry-run FAILED (local validation)\n{local_result}"
        return result

    # ── Phase 2: seatunnel.sh --check ──
    sh_path = _find_seatunnel_sh()
    if sh_path:
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".conf", prefix="seatunnel_dryrun_", delete=False
            ) as tmp:
                tmp.write(config_str)
                tmp_path = tmp.name

            cmd = ["sh", sh_path, "--check", "--config", tmp_path]
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )

            if proc.returncode == 0:
                result["phase2_check"] = "PASS"
            else:
                stderr = proc.stderr.strip() or proc.stdout.strip()
                result["phase2_check"] = f"FAIL: {stderr[-500:]}"
                result["summary"] = (
                    f"Dry-run FAILED (engine --check)\n"
                    f"Local: {local_result}\n"
                    f"Engine: {result['phase2_check']}"
                )
                return result
        except subprocess.TimeoutExpired:
            result["phase2_check"] = "TIMEOUT (30s)"
        except Exception as e:
            result["phase2_check"] = f"ERROR: {e}"
        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    # ── Phase 3: REST API validation ──
    from .connectors import _check_engine, _ENGINE_API_BASE
    if _check_engine():
        try:
            import urllib.request
            # Use submit-job endpoint with a dry-run approach:
            # We validate by checking if config parses on server side
            # without actually starting the job
            url = f"{_ENGINE_API_BASE}/submit-job"
            headers = {"Content-Type": "application/json"}
            # Submit with an invalid job name pattern to trigger validation
            # without actual execution — this is a best-effort approach
            # since SeaTunnel doesn't have a dedicated validate endpoint
            data = json.dumps({
                "env": {"job.mode": "BATCH"},
                "params": {"config": config_str, "format": "hocon"},
            }).encode("utf-8")

            # For now, just verify the config format is accepted by the API
            # A full submit-and-cancel approach would be too risky
            result["phase3_api"] = "SKIPPED (no dedicated validate endpoint)"
        except Exception as e:
            result["phase3_api"] = f"ERROR: {e}"

    # ── Summary ──
    phases_passed = ["Local: " + local_result]
    if result["phase2_check"]:
        phases_passed.append("Engine --check: " + result["phase2_check"])

    result["valid"] = local_result.startswith("VALID") and (
        result["phase2_check"] is None or result["phase2_check"] == "PASS"
    )

    if result["valid"]:
        result["summary"] = "Dry-run PASSED\n" + "\n".join(phases_passed)
    else:
        result["summary"] = "Dry-run COMPLETED (partial)\n" + "\n".join(phases_passed)

    return result


# ─── System prompts ───

PLANNER_SYSTEM = """You are the **Planner Agent** of SeaTunnel CLI — an expert assistant \
specialized in Apache SeaTunnel data integration pipelines.

## Identity & Scope
You are a data ETL/ELT specialist. You help with:
- Designing data synchronization pipelines (source → transform → sink)
- Explaining SeaTunnel connectors, options, and configuration
- **Diagnosing errors**: analyzing logs, stack traces, and job failures
- **Troubleshooting**: fixing config issues, connector problems, runtime errors
- Recommending connector choices for specific data integration scenarios

You do NOT answer questions about:
- General programming, algorithms, or non-ETL topics
- Other data tools (Flink SQL, Spark, Airflow) unless comparing with SeaTunnel
- Anything unrelated to data integration / ETL / ELT

## Responsibilities
1. **Classify** the user's intent — is it a NEW pipeline request, or something else?
2. For pipeline requests: identify source/sink/transform and output a PLAN
3. For everything else (questions, diagnostics, troubleshooting, greetings): respond directly as CHAT
4. **Use tools** to look up connector details when needed
5. **Ask the user** (via ask_user tool) when critical information is missing

## How to Classify User Intent

Output **PLAN:** ONLY when the user explicitly asks to CREATE or MODIFY a data pipeline config.
Signals: "sync X to Y", "read from X write to Y", "create a job that...", "add a transform to...",
"modify the config to...", "change the sink to..."

Output **CHAT:** for EVERYTHING else, including:
- Greetings, help requests, "what is X" questions
- **Error logs, stack traces, exception messages** — analyze and diagnose them
- **Job failure analysis** — identify root cause and suggest fixes
- **Config review** — review a config without regenerating it
- **Connector questions** — explain options, compare connectors
- **Troubleshooting** — "why is my job slow", "my job keeps failing", etc.
- Pasted text that looks like logs/errors/exceptions rather than pipeline descriptions

IMPORTANT: When the user pastes logs or error messages, ALWAYS treat it as a diagnostic request (CHAT),
never as a pipeline creation request (PLAN). Analyze the error and provide actionable advice.

## Default Assumptions (for PLAN mode — do NOT ask for these):
- Parallelism → 2
- job.mode → infer from context (CDC/Kafka → STREAMING, otherwise BATCH)
- Ports → standard defaults (MySQL: 3306, PG: 5432, Kafka: 9092, etc.)
- Host → localhost
- If the user's remembered facts (memory) contain connection details (host, port, user, password,
  access keys), pass those ACTUAL VALUES to the config agent instead of using placeholders.
- Only use ${VAR} placeholders for credentials that are not in the user's memory or request.

## Output Format

For new pipeline requests:
```
PLAN:
- Source: <connector name> (reason)
- Transform: <transform name> or none
- Sink: <connector name> (reason)
- Mode: BATCH/STREAMING
- Key decisions: <any assumptions made>
- Missing info: <none, or what was asked>
```

For everything else:
```
CHAT:
<your response — diagnostics, explanations, advice, etc.>
```

## Few-shot Examples

User: "hi"
Response:
CHAT:
Hello! I'm the SeaTunnel assistant, here to help you build data pipelines.

Tell me what data you want to move — for example:
- "Sync MySQL orders table to Doris in real-time via CDC"
- "Export PostgreSQL logs to S3 as Parquet files"
- "Stream Kafka events to ClickHouse"

Or paste any SeaTunnel error logs and I'll help diagnose the issue.

---

User: "help me write a Python script"
Response:
CHAT:
I'm specialized in Apache SeaTunnel data pipeline configuration — I can't help with general Python scripting.

But if you need to move data between systems, I'm here for that! For example:
- Batch sync between databases (MySQL, PostgreSQL, Oracle → Doris, StarRocks, ClickHouse)
- Real-time CDC streaming (MySQL-CDC, PostgreSQL-CDC → any sink)
- File-based ETL (CSV, JSON, Parquet on S3/HDFS → data warehouses)

What data do you need to sync?

---

User: "What is SeaTunnel?"
Response:
CHAT:
Apache SeaTunnel is a high-performance distributed data integration engine that supports:

- **81+ connectors**: databases, messaging, file storage, OLAP, data lakes, vector DBs
- **Batch & Streaming**: unified API for both modes
- **Transforms**: SQL, field mapping, filtering, splitting, even LLM-based transforms
- **CDC support**: real-time change data capture from MySQL, PostgreSQL, Oracle, SQL Server, MongoDB

I can generate ready-to-use SeaTunnel configs from natural language. Just describe your pipeline — e.g., "Sync MySQL users to S3 Parquet" — and I'll handle the rest.

---

User: "How do I read from Kafka?"
Response:
CHAT:
SeaTunnel's **Kafka** connector supports reading from Kafka topics in streaming or batch mode.

Key options:
- `bootstrap.servers` — Kafka broker addresses
- `topic` — topic name to consume
- `start_mode` — earliest, latest, group_offsets, timestamp, or specific_offsets
- `format` — json, csv, avro, canal_json, debezium_json, etc.
- `consumer.group` — consumer group ID
- `schema.fields` — define output field types

Want me to generate a full config? Tell me where the Kafka data should go — e.g., "Stream Kafka orders topic to ClickHouse".

---

User: "java.lang.NullPointerException at org.apache.seatunnel.connectors.seatunnel.jdbc..."
Response:
CHAT:
This is a **NullPointerException in the JDBC connector**. Common causes:

1. **Missing required config option** — check that `url`, `driver`, `user`, `password`, and `query` are all set
2. **Wrong driver class** — make sure the JDBC driver JAR is in `$SEATUNNEL_HOME/lib/` and the driver class name matches (e.g., `com.mysql.cj.jdbc.Driver` for MySQL 8+)
3. **Database not reachable** — verify the host, port, and database name in your JDBC URL

Can you share the config you used? I'll pinpoint the exact issue.

---

User: "Job failed: org.apache.seatunnel.engine.server.exception.TaskGroupDeployException: Cannot find table 'orders' in database 'mydb'"
Response:
CHAT:
The error says **table 'orders' doesn't exist** in database 'mydb'. Check these:

1. **Table name casing** — some databases are case-sensitive. Try `"mydb"."orders"` or `"mydb"."ORDERS"`
2. **Schema prefix** — in PostgreSQL, you may need `public.orders` or the correct schema name
3. **Database name** — verify `mydb` is correct in your JDBC URL

If you share your config, I can check for mismatches.

---

User: "我的任务报错 Kafka consumer poll timeout, 请帮我看看"
Response:
CHAT:
**Kafka consumer poll timeout** 通常有以下原因:

1. **Broker 不可达** — 检查 `bootstrap.servers` 地址是否正确，网络是否通畅
2. **Topic 不存在** — 确认 topic 名称拼写正确，可以用 `kafka-topics.sh --list` 验证
3. **Consumer group 被阻塞** — 如果有其他 consumer 占用了所有 partition，新 consumer 会等待 rebalance
4. **超时设置太短** — 可以在 config 里增加 `properties { session.timeout.ms = 30000 }`

把你的配置文件贴出来，我帮你具体排查。

---

User: "从 MySQL 的 users 表同步到 S3 Parquet 文件"
Response:
PLAN:
- Source: Jdbc (MySQL via JDBC — batch full-table read)
- Transform: none
- Sink: S3File (S3 with Parquet format)
- Mode: BATCH
- Key decisions: Using Jdbc (not CDC, since user said "sync" implying batch); Parquet format as requested; standard MySQL port 3306; password as ${MYSQL_PASSWORD}
- Missing info: none

---

User: "I want real-time sync from MySQL orders to StarRocks, need to capture all changes"
Response:
PLAN:
- Source: MySQL-CDC (real-time change data capture — captures insert/update/delete)
- Transform: none
- Sink: StarRocks (StarRocks sink with stream load)
- Mode: STREAMING
- Key decisions: MySQL-CDC chosen for real-time requirement; checkpoint.interval=10000 for streaming; using ${PASSWORD} placeholder
- Missing info: none
"""

CONFIG_SYSTEM_TEMPLATE = """You are the **Config Generator Agent** of SeaTunnel CLI.

Your job is to generate a valid, immediately-runnable Apache SeaTunnel HOCON configuration file.

## SeaTunnel Config Structure:
```hocon
env {{
  parallelism = <number>
  job.mode = "BATCH" | "STREAMING"
  checkpoint.interval = <ms>  # only for STREAMING
}}

source {{
  <ConnectorName> {{
    <options...>
  }}
}}

transform {{
  <TransformName> {{
    <options...>
  }}
}}

sink {{
  <ConnectorName> {{
    <options...>
  }}
}}
```

## CRITICAL Rules — read carefully:

### Option placement
- **NEVER** put sink-only options on a source connector, or vice versa.
  - `schema_save_mode`, `data_save_mode`, `generate_sink_sql` → **SINK ONLY**
  - `partition_num`, `fetch_size`, `where_condition` → **SOURCE ONLY**
- When unsure, call `get_connector_info` for the specific connector and check the option list.

### Option names and values
- Use EXACTLY the option names from the connector catalog. Do NOT invent option names.
- **S3/HDFS file connectors**: credential keys use DASHES not DOTS:
  - Correct: `fs.s3a.access-key`, `fs.s3a.secret-key`, `fs.s3a.endpoint`
  - WRONG: `fs.s3a.access.key`, `fs.s3a.secret.key`
- **Jdbc connector**: the url option is `url` (with fallback key `base-url`), NOT `base_url` or `jdbc_url`.
- **Enum values**: use the exact values from the catalog (e.g., `format = "parquet"` not `format = "PARQUET"` unless catalog says so).

### Credential handling — PREFER real values from memory
- If the user's memory store contains connection details (host, port, username, password, access keys),
  use those ACTUAL VALUES directly in the config — do NOT replace them with ${{ENV_VAR}} placeholders.
- Only use `${{ENV_VAR}}` placeholders when:
  (a) No real value is available from memory or the user's request, AND
  (b) You add a comment explaining what the user needs to fill in.
- NEVER generate a config with unresolved `${{VAR}}` placeholders without warning the user.
  If you must use placeholders, list them explicitly in the Explanation section.

### General
1. Always include the `env` block with job.mode
2. Use correct connector names (case-sensitive): Jdbc, Kafka, S3File, Clickhouse, etc.
3. Include ALL required parameters for each connector (call get_connector_info to verify)
4. Set reasonable defaults for optional performance params
5. For STREAMING jobs, always set checkpoint.interval
6. When using multi-table, use plugin_output/plugin_input to chain stages
7. Call `get_connector_info` for EVERY connector you use — do not rely on training data alone

## Connector Catalog:
{connector_catalog}

## Output:
Return ONLY the HOCON config inside a ```hocon code block. Add brief comments inline.
After the config block, add a "## Explanation" section that includes:
1. Brief explanation of your choices
2. List of any `${{VAR}}` placeholders that need to be filled in (if any)
3. Any assumptions made about connection details
"""

VALIDATOR_SYSTEM = """You are the **Validator Agent** of SeaTunnel CLI.

Your job is to review a generated SeaTunnel HOCON config and identify issues.

Check for:
1. HOCON syntax correctness (matched braces, proper quoting)
2. All required parameters present for each connector
3. Correct connector names (case-sensitive)
4. Reasonable values (ports, batch sizes, parallelism)
5. Security (no hardcoded passwords — use ${VAR} placeholders)
6. Mode consistency (STREAMING jobs need checkpoint.interval)
7. Plugin input/output chaining correctness (if transforms exist)

Output one of:
- "PASS" — config is valid, no issues
- "PASS_WITH_NOTES: <notes>" — config is valid but has optional improvements
- "FAIL: <issue list>" — config has errors that must be fixed, list each error clearly
"""


class Orchestrator:
    """Coordinates the multi-agent loop for config generation.

    Flow:
      1. PlannerAgent analyzes user request (may ask clarifying questions)
      2. ConfigAgent generates HOCON config
      3. ValidatorAgent validates
      4. If validation fails → loop back to ConfigAgent (max 3 rounds)
      5. Present final config to user
    """

    def __init__(
        self,
        client: LLMProvider,
        on_status: Callable | None = None,
        on_stream: Callable[[str, dict], None] | None = None,
        memory_store: MemoryStore | None = None,
    ):
        self.client = client
        self.conversation_history: list[dict] = []
        self.on_status = on_status or (lambda *a: None)
        self.on_stream = on_stream or (lambda *a: None)
        self.memory_store = memory_store
        self.pending_question: str | None = None

    def load_history(self, history: list[dict]) -> None:
        """Replace conversation history (e.g., when resuming a session)."""
        self.conversation_history = list(history)

    def _build_planner_system(self) -> str:
        base = PLANNER_SYSTEM
        if self.memory_store:
            block = self.memory_store.format_for_prompt(max_tokens=800)
            if block:
                base = base + "\n\n" + block
        return base

    def _build_config_system(self) -> str:
        from .connectors import get_connector_catalog
        system = CONFIG_SYSTEM_TEMPLATE.format(connector_catalog=get_connector_catalog())
        if self.memory_store:
            block = self.memory_store.format_for_prompt(max_tokens=500)
            if block:
                system = system + "\n\n" + block
        return system

    def process_user_input(self, user_input: str) -> dict:
        """Process a user message and return result.

        Returns:
            {
                "type": "question" | "config" | "error",
                "content": str,
                "config": str | None,       # HOCON config if type=="config"
                "explanation": str | None,  # explanation if type=="config"
            }
        """
        self.conversation_history.append({
            "role": "user",
            "content": [{"text": user_input}],
        })

        # Phase 1: Planning
        self.on_status("thinking", "Analyzing your request...")
        plan_result = self._run_planner()

        if plan_result["type"] == "question":
            return plan_result

        if plan_result["type"] == "chat":
            self.conversation_history.append({
                "role": "assistant",
                "content": [{"text": plan_result["content"]}],
            })
            return plan_result

        # Phase 2: Config Generation
        self.on_status("generating", "Generating SeaTunnel config...")
        config_result = self._run_config_generator(plan_result["content"])

        if not config_result.get("config"):
            return {"type": "error", "content": "Failed to generate config.", "config": None, "explanation": None}

        # Phase 3: Validation loop (max 3 rounds)
        config = config_result["config"]
        explanation = config_result.get("explanation", "")

        for round_num in range(3):
            self.on_status("validating", f"Validating config (round {round_num + 1})...")
            validation = self._run_validator(config)

            if validation.startswith("PASS"):
                # Phase 4: Dry-run validation (engine-level)
                self.on_status("validating", "Running dry-run check...")
                dryrun = dry_run_config(config)
                dryrun_note = ""
                if dryrun["phase2_check"] and dryrun["phase2_check"] != "PASS":
                    dryrun_note = f"\n\n**Dry-run note:** {dryrun['phase2_check']}"
                elif dryrun["valid"]:
                    dryrun_note = "\n\n**Dry-run:** PASSED"

                # Add assistant message to history
                self.conversation_history.append({
                    "role": "assistant",
                    "content": [{"text": f"Here is the generated config:\n```hocon\n{config}\n```\n\n{explanation}"}],
                })
                return {
                    "type": "config",
                    "content": validation,
                    "config": config,
                    "explanation": explanation + dryrun_note,
                    "dry_run": dryrun,
                }

            # Validation failed — try to fix
            self.on_status("fixing", f"Fixing issues (round {round_num + 1})...")
            fix_result = self._run_fix(config, validation)
            if fix_result.get("config"):
                config = fix_result["config"]
                if fix_result.get("explanation"):
                    explanation = fix_result["explanation"]
            else:
                break

        # Return best effort after max rounds
        self.conversation_history.append({
            "role": "assistant",
            "content": [{"text": f"Here is the generated config:\n```hocon\n{config}\n```\n\n{explanation}"}],
        })
        return {
            "type": "config",
            "content": "Config generated (validation had warnings)",
            "config": config,
            "explanation": explanation,
        }

    def _run_planner(self) -> dict:
        """Run the planner agent with tool use loop (streaming)."""
        messages = list(self.conversation_history)
        planner_system = self._build_planner_system()

        for _ in range(5):  # max 5 tool-use rounds
            events: list[dict] = []
            text_buffer = ""
            prefix_detected: str | None = None  # "CHAT" or "PLAN" or None
            prefix_stripped = False

            for event in self.client.chat_stream(
                messages=messages,
                system=planner_system,
                tools=TOOLS,
                temperature=0.2,
                max_tokens=2048,
            ):
                events.append(event)

                if event.get("type") == "text_delta":
                    text_buffer += event["text"]
                    if prefix_detected is None:
                        stripped = text_buffer.lstrip()
                        if stripped.startswith("CHAT:"):
                            prefix_detected = "CHAT"
                        elif stripped.startswith("PLAN:"):
                            prefix_detected = "PLAN"
                        elif len(stripped) >= 5 and not ("CHAT:"[:len(stripped)] == stripped or "PLAN:"[:len(stripped)] == stripped):
                            prefix_detected = "PLAN"
                    if prefix_detected == "CHAT":
                        if not prefix_stripped:
                            chat_start = text_buffer.find("CHAT:")
                            if chat_start >= 0:
                                remainder = text_buffer[chat_start + 5:]
                                prefix_stripped = True
                                if remainder.lstrip():
                                    self.on_stream("chat", {"type": "text_delta", "text": remainder.lstrip()})
                        else:
                            self.on_stream("chat", event)

            response = LLMProvider.collect_stream(events)
            assistant_content = response.get("output", {}).get("message", {}).get("content", [])
            stop_reason = response.get("stopReason", "")

            if stop_reason == "tool_use":
                tool_results = []
                question_to_ask = None

                for block in assistant_content:
                    if "toolUse" in block:
                        tool = block["toolUse"]
                        tool_name = tool["name"]
                        tool_input = tool.get("input", {})

                        if tool_name == "ask_user":
                            question_to_ask = tool_input.get("question", "Could you provide more details?")
                            tool_results.append({
                                "toolResult": {
                                    "toolUseId": tool["toolUseId"],
                                    "content": [{"text": "Question will be shown to user."}],
                                }
                            })
                        else:
                            result = _handle_tool_call(tool_name, tool_input)
                            tool_results.append({
                                "toolResult": {
                                    "toolUseId": tool["toolUseId"],
                                    "content": [{"text": result}],
                                }
                            })

                if question_to_ask:
                    return {"type": "question", "content": question_to_ask, "config": None, "explanation": None}

                messages.append({"role": "assistant", "content": assistant_content})
                messages.append({"role": "user", "content": tool_results})
                continue

            plan_text = ""
            for block in assistant_content:
                if "text" in block:
                    plan_text += block["text"]

            if plan_text.strip().startswith("CHAT:"):
                chat_text = plan_text.strip().removeprefix("CHAT:").strip()
                self.on_stream("chat", {"type": "message_stop", "stop_reason": "end_turn"})
                return {"type": "chat", "content": chat_text, "config": None, "explanation": None}

            return {"type": "plan", "content": plan_text, "config": None, "explanation": None}

        return {"type": "plan", "content": "Direct generation mode.", "config": None, "explanation": None}

    def _run_config_generator(self, plan: str) -> dict:
        """Run the config generator agent (streaming)."""
        system = self._build_config_system()

        user_request = ""
        for msg in self.conversation_history:
            if msg["role"] == "user":
                for block in msg.get("content", []):
                    if isinstance(block, dict) and "text" in block:
                        user_request += block["text"] + "\n"

        prompt = f"""## User Request:
{user_request.strip()}

## Planner Analysis:
{plan}

Generate the SeaTunnel HOCON config now. Use tools if you need connector details."""

        messages = [{"role": "user", "content": [{"text": prompt}]}]

        for _ in range(5):
            events: list[dict] = []
            is_final_round = True

            for event in self.client.chat_stream(
                messages=messages,
                system=system,
                tools=TOOLS,
                temperature=0.2,
                max_tokens=4096,
            ):
                events.append(event)
                if event.get("type") == "text_delta":
                    self.on_stream("config", event)
                elif event.get("type") == "tool_start":
                    is_final_round = False

            response = LLMProvider.collect_stream(events)
            assistant_content = response.get("output", {}).get("message", {}).get("content", [])
            stop_reason = response.get("stopReason", "")

            if stop_reason == "tool_use":
                tool_results = []
                for block in assistant_content:
                    if "toolUse" in block:
                        tool = block["toolUse"]
                        result = _handle_tool_call(tool["name"], tool.get("input", {}))
                        tool_results.append({
                            "toolResult": {
                                "toolUseId": tool["toolUseId"],
                                "content": [{"text": result}],
                            }
                        })
                messages.append({"role": "assistant", "content": assistant_content})
                messages.append({"role": "user", "content": tool_results})
                continue

            self.on_stream("config", {"type": "message_stop", "stop_reason": "end_turn"})

            full_text = ""
            for block in assistant_content:
                if "text" in block:
                    full_text += block["text"]

            return self._parse_config_response(full_text)

        return {}

    def _run_validator(self, config: str) -> str:
        """Run the validator agent."""
        # First do local validation
        local_result = validate_hocon(config)

        # Then LLM validation for semantic checks
        prompt = f"""Validate this SeaTunnel HOCON config:

```hocon
{config}
```

Local validation result: {local_result}

Check for semantic correctness, required parameters, and best practices."""

        result = self.client.quick_chat(prompt, system=VALIDATOR_SYSTEM)
        return result.strip()

    def _run_fix(self, config: str, validation_errors: str) -> dict:
        """Attempt to fix config based on validation errors."""
        system = self._build_config_system()

        prompt = f"""The following SeaTunnel config has validation issues. Fix them.

## Current Config:
```hocon
{config}
```

## Validation Errors:
{validation_errors}

Fix ALL the issues and return the corrected config. Keep all existing correct parts unchanged."""

        messages = [{"role": "user", "content": [{"text": prompt}]}]

        for _ in range(3):
            response = self.client.chat(
                messages=messages,
                system=system,
                tools=TOOLS,
                temperature=0.1,
                max_tokens=4096,
            )

            assistant_content = response.get("output", {}).get("message", {}).get("content", [])
            stop_reason = response.get("stopReason", "")

            if stop_reason == "tool_use":
                tool_results = []
                for block in assistant_content:
                    if "toolUse" in block:
                        tool = block["toolUse"]
                        result = _handle_tool_call(tool["name"], tool.get("input", {}))
                        tool_results.append({
                            "toolResult": {
                                "toolUseId": tool["toolUseId"],
                                "content": [{"text": result}],
                            }
                        })
                messages.append({"role": "assistant", "content": assistant_content})
                messages.append({"role": "user", "content": tool_results})
                continue

            full_text = ""
            for block in assistant_content:
                if "text" in block:
                    full_text += block["text"]
            return self._parse_config_response(full_text)

        return {}

    @staticmethod
    def _parse_config_response(text: str) -> dict:
        """Extract HOCON config block and explanation from LLM response."""
        config = None
        explanation = ""

        # Extract code block
        patterns = [
            r"```hocon\n(.*?)```",
            r"```\n(.*?)```",
            r"```conf\n(.*?)```",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.DOTALL)
            if match:
                config = match.group(1).strip()
                break

        # If no code block, try to find config-like content
        if not config and "env {" in text and "source {" in text:
            # Find the config block between first env{ and last }
            start = text.index("env {")
            # Count braces to find the end
            depth = 0
            end = start
            for i in range(start, len(text)):
                if text[i] == "{":
                    depth += 1
                elif text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        end = i + 1
            if end > start:
                config = text[start:end]

        # Extract explanation (everything after the config block)
        explanation_markers = ["## Explanation", "**Explanation", "Explanation:", "### "]
        for marker in explanation_markers:
            idx = text.find(marker)
            if idx != -1:
                explanation = text[idx:].strip()
                break

        return {"config": config, "explanation": explanation}
