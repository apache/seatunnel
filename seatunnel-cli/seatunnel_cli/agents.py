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
    fetch_connector_metadata, format_metadata_for_prompt,
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
            "description": "Get detailed info about a specific connector including parameters and examples. "
                           "IMPORTANT: Always specify connector_type ('source' or 'sink') to get the correct "
                           "type-specific options. Source and sink connectors have different required/optional parameters.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "connector_name": {
                            "type": "string",
                            "description": "Name of the connector, e.g. 'Jdbc', 'Kafka', 'S3File'",
                        },
                        "connector_type": {
                            "type": "string",
                            "enum": ["source", "sink"],
                            "description": "Whether this connector is used as 'source' or 'sink'. "
                                           "Source and sink have different options — always specify this.",
                        },
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
        ctype = tool_input.get("connector_type")
        detail = get_connector_detail(name, connector_type=ctype)
        return detail or f"Connector '{name}' not found. Use list_connectors to see available options."

    elif tool_name == "route_connectors":
        text = tool_input.get("user_text", "")
        matches = route_by_keyword(text)
        if matches:
            return f"Relevant connectors for '{text}': {', '.join(matches)}\nCall get_connector_info for each to get full option details."
        return f"No direct keyword match for '{text}'. Use list_connectors to browse all available connectors."

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


def _get_hocon_value(conf, key: str):
    """Safely get a value from a pyhocon config object by key."""
    try:
        if hasattr(conf, "get"):
            return conf.get(key, None)
        elif isinstance(conf, dict):
            return conf.get(key, None)
    except Exception:
        pass
    return None


def _check_conditional_mismatches(
    section: str, connector_name: str, connector_conf,
    provided_keys: set[str], metadata: dict, errors: list[str],
) -> None:
    """Check for conditional options whose trigger condition doesn't match.

    Reads the metadata's conditional groups and checks: if a conditional option
    key is present in the config, does its trigger condition actually match?
    If not, flag it as an error — these cause runtime type-mismatch failures.
    """
    # Build a map: option_key → set of (trigger_key, trigger_value)
    # An option is valid if at least ONE of its trigger conditions matches.
    cond_map: dict[str, set[tuple[str, str]]] = {}

    for opt in metadata.get("required", []):
        cond_key = opt.get("_condition_key")
        cond_val = opt.get("_condition_value")
        if cond_key and cond_val:
            cond_map.setdefault(opt["key"], set()).add((cond_key, cond_val))

    for cond in metadata.get("conditional", []):
        trigger_key = cond.get("when", "")
        trigger_val = cond.get("equals", "")
        if trigger_key and trigger_val:
            for opt_key in cond.get("then_require", []):
                cond_map.setdefault(opt_key, set()).add((trigger_key, trigger_val))

    # Check each conditional option that appears in the config
    for opt_key, triggers in cond_map.items():
        if opt_key not in provided_keys:
            continue
        # Check if any trigger condition matches
        any_match = False
        for trigger_key, trigger_val in triggers:
            actual_val = _get_hocon_value(connector_conf, trigger_key)
            if actual_val is not None and str(actual_val).strip('"') == trigger_val:
                any_match = True
                break
            # Also check case-insensitive for enums
            if actual_val is not None and str(actual_val).strip('"').upper() == trigger_val.upper():
                any_match = True
                break
        if not any_match:
            trigger_desc = " or ".join(f"{k}={v}" for k, v in triggers)
            errors.append(
                f"{section}.{connector_name}: '{opt_key}' is a conditional option "
                f"(requires {trigger_desc}) but the condition doesn't match in this config"
            )


def _extract_connector_blocks_raw(
    config_str: str,
) -> list[tuple[str, str, str]]:
    """Extract individual connector blocks from raw HOCON text.

    Handles duplicate connector names that pyhocon would merge.
    Uses brace-counting on the raw text.

    Returns list of ``(section, connector_name, block_content)`` where
    *block_content* is the text between the connector's outermost braces
    (inclusive), e.g. ``'url = "..." driver = "..."'``.
    """
    results: list[tuple[str, str, str]] = []
    for section in ("source", "sink"):
        # Find the section's opening brace
        pattern = re.compile(
            rf"(?:^|\n)\s*{section}\s*\{{", re.IGNORECASE,
        )
        m = pattern.search(config_str)
        if not m:
            continue

        # Walk from section opening brace to find matching close
        section_start = m.end() - 1  # position of '{'
        depth = 0
        section_end = section_start
        for i in range(section_start, len(config_str)):
            if config_str[i] == "{":
                depth += 1
            elif config_str[i] == "}":
                depth -= 1
                if depth == 0:
                    section_end = i
                    break

        section_body = config_str[section_start + 1 : section_end]

        # Find each connector block inside the section body
        # Pattern: ConnectorName { ... }
        connector_pattern = re.compile(r"(\w[\w-]*)\s*\{")
        pos = 0
        while pos < len(section_body):
            cm = connector_pattern.search(section_body, pos)
            if not cm:
                break
            connector_name = cm.group(1)
            brace_start = cm.end() - 1  # position of '{'
            # Walk to find matching close brace
            bdepth = 0
            block_end = brace_start
            for j in range(brace_start, len(section_body)):
                if section_body[j] == "{":
                    bdepth += 1
                elif section_body[j] == "}":
                    bdepth -= 1
                    if bdepth == 0:
                        block_end = j
                        break
            block_content = section_body[brace_start + 1 : block_end].strip()
            results.append((section, connector_name, block_content))
            pos = block_end + 1

    return results


def _validate_routing_pairs(
    blocks: list[tuple[str, str, str]],
    errors: list[str],
    warnings: list[str],
) -> None:
    """Validate plugin_output / plugin_input pairing across connector blocks."""
    outputs: dict[str, str] = {}  # label → connector_name
    inputs: dict[str, str] = {}

    label_re = re.compile(
        r'plugin_(?:output|input)\s*=\s*"([^"]+)"',
    )

    for section, connector_name, block_content in blocks:
        for m in label_re.finditer(block_content):
            label = m.group(1)
            if "plugin_output" in m.group(0):
                if label in outputs:
                    errors.append(
                        f"Duplicate plugin_output label \"{label}\" "
                        f"in source.{connector_name} "
                        f"(already used by source.{outputs[label]})"
                    )
                outputs[label] = connector_name
            else:
                inputs[label] = connector_name

    # Only validate pairing when routing labels are actually used
    if not outputs and not inputs:
        return

    unmatched_inputs = set(inputs) - set(outputs)
    if unmatched_inputs:
        for label in unmatched_inputs:
            errors.append(
                f"sink.{inputs[label]}: plugin_input \"{label}\" "
                f"has no matching plugin_output in any source"
            )

    orphan_outputs = set(outputs) - set(inputs)
    if orphan_outputs:
        for label in orphan_outputs:
            warnings.append(
                f"source.{outputs[label]}: plugin_output \"{label}\" "
                f"has no matching plugin_input in any sink"
            )


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

    # ── 3. Connector-level required params + conditional mismatch check ──
    # Use raw block extraction to handle duplicate connector names (e.g.,
    # two Jdbc blocks in source{}) that pyhocon would merge into one.
    raw_blocks = _extract_connector_blocks_raw(config_str)
    from collections import Counter

    block_counts = Counter(
        (section, name) for section, name, _ in raw_blocks
    )
    has_duplicate_connectors = any(c > 1 for c in block_counts.values())

    if has_duplicate_connectors and raw_blocks:
        # Multi-pipeline path: validate each block individually via raw text
        try:
            from pyhocon import ConfigFactory as _CF
        except ImportError:
            _CF = None

        for section, connector_name, block_content in raw_blocks:
            try:
                if _CF:
                    # Wrap block content so pyhocon can parse it
                    block_conf = _CF.parse_string(
                        f"{connector_name} {{ {block_content} }}"
                    )
                    inner = block_conf.get(connector_name, block_conf)
                    provided_keys = _flatten_hocon_keys(inner)
                else:
                    # Fallback: regex-extract keys from raw text
                    provided_keys = set(
                        re.findall(r"^\s*([\w.]+)\s*=", block_content, re.MULTILINE)
                    )

                result = validate_connector_options(
                    connector_name, provided_keys, connector_type=section,
                )
                if result.get("missing_required"):
                    missing = ", ".join(result["missing_required"])
                    errors.append(
                        f"{section}.{connector_name}: missing required options: {missing}"
                    )
            except Exception:
                pass

        # Validate routing labels
        _validate_routing_pairs(raw_blocks, errors, warnings)

    elif parsed is not None:
        # Standard single-connector-per-type path (pyhocon is accurate here)
        for section in ["source", "sink"]:
            try:
                section_conf = parsed.get(section, None)
                if section_conf is None:
                    continue
                for connector_name in section_conf:
                    connector_conf = section_conf[connector_name]
                    provided_keys = _flatten_hocon_keys(connector_conf)
                    result = validate_connector_options(
                        connector_name, provided_keys, connector_type=section,
                    )
                    if result.get("missing_required"):
                        missing = ", ".join(result["missing_required"])
                        errors.append(
                            f"{section}.{connector_name}: missing required "
                            f"options: {missing}"
                        )

                    # ── 3b. Generic conditional mismatch check ──
                    metadata = fetch_connector_metadata(connector_name, section)
                    if metadata:
                        _check_conditional_mismatches(
                            section, connector_name, connector_conf,
                            provided_keys, metadata, errors,
                        )
            except Exception:
                pass

        # Also validate routing for single-connector configs if labels present
        if raw_blocks:
            _validate_routing_pairs(raw_blocks, errors, warnings)

    # Check STREAMING mode needs checkpoint.interval
    if parsed is not None:
        try:
            env = parsed.get("env", {})
            if hasattr(env, "get"):
                mode = env.get("job.mode", "")
                if isinstance(mode, str) and mode.upper() == "STREAMING":
                    interval = env.get("checkpoint.interval", None)
                    if interval is None:
                        warnings.append(
                            "STREAMING job should set env.checkpoint.interval "
                            "(e.g., 10000)"
                        )
        except Exception:
            pass

    # ── 4. Security & placeholder checks ──
    if '""' in config_str:
        warnings.append("Found empty string value. Make sure this is intentional.")

    password_pattern = re.compile(r'password\s*=\s*"(?!\$\{)[^"]{3,}"', re.IGNORECASE)
    if password_pattern.search(config_str):
        warnings.append("Hardcoded password detected. Consider using environment variable: ${PASSWORD}")

    # Check for unresolved ${ENV_VAR} placeholders — these will be passed as literal
    # strings to connectors at runtime, causing authentication/connection failures.
    env_var_pattern = re.compile(r'\$\{([A-Za-z_][A-Za-z0-9_]*)\}')
    unresolved_vars = set()
    for m in env_var_pattern.finditer(config_str):
        var_name = m.group(1)
        if not os.environ.get(var_name):
            unresolved_vars.add(var_name)
    if unresolved_vars:
        var_list = ", ".join(sorted(unresolved_vars))
        errors.append(
            f"Unresolved environment variables: {var_list}. "
            f"Set them before running: export {sorted(unresolved_vars)[0]}=<value>"
        )

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
    from . import get_seatunnel_home
    st_home = get_seatunnel_home()
    if st_home:
        path = os.path.join(st_home, "bin", "seatunnel.sh")
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
- Host → use from memory if available, otherwise localhost

## SECURITY — Credential handling (MANDATORY):
- NEVER include actual passwords, API keys, tokens, or secret values in generated configs.
- ALWAYS use environment variable placeholders for ALL credentials:
  - Passwords: ${PASSWORD}, ${MYSQL_PASSWORD}, ${PG_PASSWORD}
  - API keys: ${ACCESS_KEY}, ${SECRET_KEY}, ${API_KEY}
  - Tokens: ${TOKEN}, ${AUTH_TOKEN}
- Non-secret connection info (host, port, database name) CAN be used directly from memory.
- If the user provides a password in their request, use a placeholder and tell them to set the env var.

## Output Format

For new pipeline requests — output **structured JSON** inside a PLAN: block.

Single pipeline:
```
PLAN:
```json
{
  "pipelines": [
    {
      "id": "pipeline_1",
      "source": {"connector": "<name>", "reason": "<why this connector>"},
      "sink": {"connector": "<name>", "reason": "<why this connector>"},
      "transform": null,
      "tables": ["<table1>", "<table2>"],
      "database": "<database name>"
    }
  ],
  "env": {"mode": "BATCH", "parallelism": 2},
  "shared": {"database": "<db>", "host": "<host>", "port": <port>}
}
```
```

Multiple pipelines (when user requests multiple distinct source→sink routes):
```
PLAN:
```json
{
  "pipelines": [
    {
      "id": "pipeline_1",
      "source": {"connector": "<name>", "reason": "..."},
      "sink": {"connector": "<name>", "reason": "..."},
      "transform": null,
      "tables": ["<table1>", "<table2>"],
      "database": "<db>"
    },
    {
      "id": "pipeline_2",
      "source": {"connector": "<name>", "reason": "..."},
      "sink": {"connector": "<name>", "reason": "..."},
      "transform": null,
      "tables": ["<table3>"],
      "database": "<db>"
    }
  ],
  "env": {"mode": "BATCH", "parallelism": 2},
  "shared": {"database": "<db>", "host": "<host>", "port": <port>}
}
```
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
```json
{
  "pipelines": [
    {"id": "pipeline_1", "source": {"connector": "Jdbc", "reason": "MySQL via JDBC — batch full-table read"}, "sink": {"connector": "S3File", "reason": "S3 with Parquet format"}, "transform": null, "tables": ["users"], "database": ""}
  ],
  "env": {"mode": "BATCH", "parallelism": 2},
  "shared": {"host": "localhost", "port": 3306}
}
```

---

User: "I want real-time sync from MySQL orders to StarRocks, need to capture all changes"
Response:
PLAN:
```json
{
  "pipelines": [
    {"id": "pipeline_1", "source": {"connector": "MySQL-CDC", "reason": "real-time change data capture"}, "sink": {"connector": "StarRocks", "reason": "StarRocks sink with stream load"}, "transform": null, "tables": ["orders"], "database": ""}
  ],
  "env": {"mode": "STREAMING", "parallelism": 2},
  "shared": {"host": "localhost", "port": 3306}
}
```

---

User: "2 Jdbc MySQL sources, 3 tables (db: codex_cli_tables_test), sync to Console and Assert, 2 pipelines: pipeline 1 = Jdbc(audit_log, orders) → Console, pipeline 2 = Jdbc(users) → Assert"
Response:
PLAN:
```json
{
  "pipelines": [
    {"id": "pipeline_1", "source": {"connector": "Jdbc", "reason": "MySQL JDBC for audit_log and orders"}, "sink": {"connector": "Console", "reason": "Console output for debugging"}, "transform": null, "tables": ["audit_log", "orders"], "database": "codex_cli_tables_test"},
    {"id": "pipeline_2", "source": {"connector": "Jdbc", "reason": "MySQL JDBC for users"}, "sink": {"connector": "Assert", "reason": "Assert for data validation"}, "transform": null, "tables": ["users"], "database": "codex_cli_tables_test"}
  ],
  "env": {"mode": "BATCH", "parallelism": 2},
  "shared": {"database": "codex_cli_tables_test", "host": "localhost", "port": 3306}
}
```
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

## CRITICAL Rules — MUST follow to avoid runtime errors:

### 1. Minimalism — ONLY include what's needed
- Include **ALWAYS Required** options + relevant **Key Optional** options.
- **Conditional options**: ONLY include when their trigger condition actually matches.
  The metadata from `get_connector_info` shows conditions like "When `X` = `Y` → `a`, `b`, `c`".
  If your config sets `X` to a different value, do NOT include `a`, `b`, `c`.
  Including conditional options when their trigger doesn't match causes type-mismatch runtime errors.
- Do NOT include all possible options "just in case". Extra options cause runtime errors.

### 2. Option placement — source vs sink have DIFFERENT options
- **NEVER** put sink-only options on a source connector, or vice versa.
- **ALWAYS** call `get_connector_info` with `connector_type='source'` or `connector_type='sink'`
  to get the correct type-specific options. Do not guess — source and sink factories define
  different required/optional parameters for the same connector name.

### 3. Use metadata as the source of truth
- Use EXACTLY the option names from `get_connector_info` results. Do NOT invent option names.
- Check `[aliases: ...]` in metadata — either the primary key or an alias is accepted.
- Respect option types from metadata (string, boolean, list, etc.).
- Refer to the Golden Examples below for known-good patterns.

### 4. Credential handling — SECURITY CRITICAL
- NEVER hardcode passwords, API keys, tokens, or secrets in generated configs.
- ALWAYS use `${{ENV_VAR}}` placeholders for ALL credential values:
  - `password = "${{MYSQL_PASSWORD}}"` (never the actual password)
  - `access_key = "${{AWS_ACCESS_KEY}}"` (never the actual key)
  - `secret_key = "${{AWS_SECRET_KEY}}"` (never the actual secret)
  - `token = "${{AUTH_TOKEN}}"` (never the actual token)
- Non-secret values (host, port, database, table, topic) CAN use actual values from memory.
- After the config, list ALL env vars that need to be set in the Explanation section.

## Golden Examples — verified working configs:

### MySQL → S3 Parquet (BATCH)
```hocon
env {{
  parallelism = 2
  job.mode = "BATCH"
}}

source {{
  Jdbc {{
    url = "jdbc:mysql://mysql-host:3306/mydb"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "${{MYSQL_USER}}"
    password = "${{MYSQL_PASSWORD}}"
    query = "SELECT * FROM users"
    plugin_output = "jdbc_out"
  }}
}}

sink {{
  S3File {{
    plugin_input = "jdbc_out"
    bucket = "s3a://my-bucket"
    path = "/data/users"
    tmp_path = "/tmp/seatunnel"
    fs.s3a.endpoint = "s3.us-east-1.amazonaws.com"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "${{AWS_ACCESS_KEY}}"
    secret_key = "${{AWS_SECRET_KEY}}"
    file_format_type = "PARQUET"
    compress_codec = "SNAPPY"
  }}
}}
```

### Kafka → Clickhouse (STREAMING)
```hocon
env {{
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}}

source {{
  Kafka {{
    bootstrap.servers = "kafka:9092"
    topic = "events"
    format = "json"
    start_mode = "latest"
    plugin_output = "kafka_out"
    schema {{
      fields {{
        id = "bigint"
        name = "string"
        timestamp = "timestamp"
      }}
    }}
  }}
}}

sink {{
  Clickhouse {{
    plugin_input = "kafka_out"
    host = "clickhouse:8123"
    database = "default"
    table = "events"
    username = "${{CLICKHOUSE_USER}}"
    password = "${{CLICKHOUSE_PASSWORD}}"
  }}
}}
```

### Multi-Pipeline Routing (plugin_output / plugin_input)
When generating configs with **multiple source/sink blocks**:
- Each source block MUST have a unique `plugin_output` label
- Each sink block MUST have `plugin_input` matching the corresponding source's `plugin_output`
- Multiple blocks of the SAME connector type are allowed (e.g., two Jdbc sources)
- Labels should be descriptive: "jdbc_audit_log", "jdbc_users", etc.
- ALL source blocks go inside ONE `source {{ }}` section
- ALL sink blocks go inside ONE `sink {{ }}` section

Example — Two Jdbc sources routed to Console and Assert:
```hocon
env {{
  parallelism = 2
  job.mode = "BATCH"
}}

source {{
  Jdbc {{
    url = "jdbc:mysql://localhost:3306/mydb"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "${{MYSQL_USER}}"
    password = "${{MYSQL_PASSWORD}}"
    query = "SELECT * FROM audit_log"
    plugin_output = "jdbc_audit_log"
  }}
  Jdbc {{
    url = "jdbc:mysql://localhost:3306/mydb"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "${{MYSQL_USER}}"
    password = "${{MYSQL_PASSWORD}}"
    query = "SELECT * FROM users"
    plugin_output = "jdbc_users"
  }}
}}

sink {{
  Console {{
    plugin_input = "jdbc_audit_log"
  }}
  Assert {{
    plugin_input = "jdbc_users"
    rules {{
      row_rules = [
        {{
          rule_type = MIN_ROW
          rule_value = 1
        }}
      ]
    }}
  }}
}}
```

## Connector Catalog:
{connector_catalog}

## Output:
Return the HOCON config inside a ```hocon code block. Keep it MINIMAL — only include options that are
actually needed. Add brief inline comments only for non-obvious choices.
After the config block, add a brief "## Explanation" section.
"""

VALIDATOR_SYSTEM = """You are the **Validator Agent** of SeaTunnel CLI.

Your job is to review a generated SeaTunnel HOCON config and catch errors that will cause runtime failures.

## Check for (in order of severity):

### Runtime-breaking errors (MUST fail):
1. **Conditional option mismatch**: options that belong to a conditional group are included but
   their trigger condition value doesn't match. These cause type-mismatch or unknown-option errors at runtime.
   For each option, check: does its trigger condition match the actual value set in the config?
2. **Source/sink option mixing**: sink-only options on a source block, or vice versa.
3. **Missing ALWAYS Required options**: check against connector metadata.
4. **HOCON syntax errors**: unmatched braces, invalid quoting.
5. **Option type mismatch**: boolean value where list is expected, string where int is expected, etc.

### Warnings (PASS with notes):
6. STREAMING jobs without checkpoint.interval
7. Missing `env` block

### NOT an issue (do NOT flag):
- Hardcoded passwords (user explicitly provided them)
- Missing optional parameters (they have defaults)

Output one of:
- "PASS" — config is valid
- "PASS_WITH_NOTES: <notes>" — config works but has improvements
- "FAIL: <issue list>" — config has errors that must be fixed
"""


def _extract_connectors_from_plan(plan_text: str) -> list[tuple[str, str]]:
    """Extract (connector_name, connector_type) pairs from planner output.

    Tries structured JSON first (new format), falls back to legacy regex.
    Returns unique (name, type) pairs for metadata fetching.
    """
    # Try structured JSON path first
    try:
        from .skills import parse_structured_plan

        plan = parse_structured_plan(plan_text)
        if plan.pipelines:
            seen: set[tuple[str, str]] = set()
            connectors: list[tuple[str, str]] = []
            for p in plan.pipelines:
                for name, ctype in [
                    (p.source_connector, "source"),
                    (p.sink_connector, "sink"),
                ]:
                    key = (name, ctype)
                    if key not in seen:
                        connectors.append(key)
                        seen.add(key)
            return connectors
    except Exception:
        pass

    # Fallback: legacy regex parsing
    connectors = []
    for line in plan_text.split("\n"):
        stripped = line.strip().lstrip("- ")
        # Match "Source: ConnectorName (...)" or "Sink: ConnectorName (...)"
        m = re.match(r"(?:Source|source):\s*(\S+)", stripped)
        if m:
            name = m.group(1).rstrip(",")
            connectors.append((name, "source"))
            continue
        m = re.match(r"(?:Sink|sink):\s*(\S+)", stripped)
        if m:
            name = m.group(1).rstrip(",")
            connectors.append((name, "sink"))
    return connectors


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
        self._connector_metadata_block: str = ""

    # ─── Context window management ───
    # conversation_history always holds the FULL session (saved to disk as-is).
    # _trimmed_history() returns a shorter view for LLM calls:
    #   - Recent messages kept verbatim (they carry active context).
    #   - Older messages are summarized into one compact message so the LLM
    #     still knows what happened but doesn't pay full token cost.
    #   - Tool-use pairs (assistant+toolResult) are never split.
    MAX_RECENT_MESSAGES = 20   # keep latest N messages verbatim
    SUMMARY_THRESHOLD = 24     # only summarize when total exceeds this

    def load_history(self, history: list[dict]) -> None:
        """Replace conversation history (e.g., when resuming a session)."""
        self.conversation_history = list(history)

    def _trimmed_history(self) -> list[dict]:
        """Return a context-window-friendly view of conversation_history.

        If the history is short enough, returns a full copy.
        Otherwise, summarizes older messages into a single user message
        and appends the recent messages verbatim.

        Security: all text content is redacted of credential values before
        being sent to the LLM, to prevent leaking user-typed secrets.
        """
        from .memory import _redact_conversation_history
        history = _redact_conversation_history(self.conversation_history)
        if len(history) <= self.SUMMARY_THRESHOLD:
            return list(history)

        # Find safe split point: keep last MAX_RECENT_MESSAGES, but never
        # split a tool-use pair (assistant with toolUse + user with toolResult).
        split = len(history) - self.MAX_RECENT_MESSAGES
        # If the message at split is a toolResult response, move split back
        # to include the preceding assistant message.
        while split > 0:
            msg = history[split]
            content = msg.get("content", [])
            has_tool_result = any(
                isinstance(b, dict) and "toolResult" in b for b in content
            )
            if has_tool_result:
                split -= 1
            else:
                break

        older = history[:split]
        recent = history[split:]

        if not older:
            return list(history)

        # Build a compact summary of older messages
        snippets = []
        for msg in older:
            role = msg.get("role", "")
            for block in msg.get("content", []):
                if isinstance(block, dict) and "text" in block:
                    text = block["text"][:200].replace("\n", " ")
                    snippets.append(f"{role}: {text}")
        summary_text = "\n".join(snippets[-20:])  # cap at ~20 snippets

        summary_msg = {
            "role": "user",
            "content": [{
                "text": (
                    f"[Earlier conversation summary ({len(older)} messages)]\n"
                    f"{summary_text}\n"
                    f"[End of summary — recent messages follow]"
                ),
            }],
        }
        return [summary_msg] + list(recent)

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
        # Inject precise connector metadata fetched in Phase 1.5
        metadata_block = getattr(self, "_connector_metadata_block", "")
        if metadata_block:
            system = system + "\n\n" + metadata_block
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

        # Phase 1.5: Skill-based prompt enrichment (Match → Read → Execute)
        from .skills import SkillRouter, SkillExecutor, parse_structured_plan

        structured_plan = parse_structured_plan(plan_result["content"])

        # Trigger-based skill matching: uses plan + user input keywords
        matched_skills = SkillRouter.match(structured_plan, user_input)
        skill = SkillExecutor(structured_plan, matched_skills)

        # Fill slots (with pipeline expansion) and check for missing info
        missing = skill.fill_and_check(user_input, self.memory_store, self.client)
        if missing:
            question = (
                "I need a few more details to generate the config:\n"
                + "\n".join(f"- {m}" for m in missing)
            )
            return {
                "type": "question",
                "content": question,
                "config": None,
                "explanation": None,
            }

        # Fetch metadata and build enriched prompt
        self._connector_metadata_block = skill.fetch_all_metadata(self.on_status)
        enriched_prompt = skill.build_enriched_prompt(
            user_input, self.on_status, self.memory_store,
        )

        # Phase 2: Config Generation (with enriched prompt from Skill)
        self.on_status("generating", "Generating SeaTunnel config...")
        config_result = self._run_config_generator(enriched_prompt, enriched=True)

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
        messages = self._trimmed_history()
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

    def _run_config_generator(self, plan: str, enriched: bool = False) -> dict:
        """Run the config generator agent (streaming).

        Args:
            plan: Either raw planner text or an enriched prompt from a Skill.
            enriched: If True, *plan* is a self-contained prompt (includes user
                request, golden examples, routing table, etc.) — skip history
                reconstruction.
        """
        system = self._build_config_system()

        if enriched:
            # Skill already assembled the full prompt
            prompt = plan
        else:
            user_request = ""
            for msg in self._trimmed_history():
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
        from .cli import _replace_creds_with_placeholders
        # First do local validation
        local_result = validate_hocon(config)

        # Security: replace credentials with placeholders before sending to LLM
        safe_config, _ = _replace_creds_with_placeholders(config)

        # Then LLM validation for semantic checks
        prompt = f"""Validate this SeaTunnel HOCON config:

```hocon
{safe_config}
```

Local validation result: {local_result}

Check for semantic correctness, required parameters, and best practices."""

        result = self.client.quick_chat(prompt, system=VALIDATOR_SYSTEM)
        return result.strip()

    def _run_fix(self, config: str, validation_errors: str) -> dict:
        """Attempt to fix config based on validation errors."""
        from .cli import _replace_creds_with_placeholders, _restore_creds_from_placeholders
        system = self._build_config_system()

        # Security: replace credentials with ${_CRED_N_} placeholders before sending to LLM.
        # After LLM returns the fixed config, restore the original values.
        safe_config, cred_map = _replace_creds_with_placeholders(config)

        prompt = f"""The following SeaTunnel config has validation issues. Fix them.

## Current Config:
```hocon
{safe_config}
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
            parsed = self._parse_config_response(full_text)
            # Restore original credential values
            if parsed.get("config") and cred_map:
                parsed["config"] = _restore_creds_from_placeholders(parsed["config"], cred_map)
            return parsed

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

        # Security: warn if LLM generated hardcoded credentials.
        # We do NOT redact the config itself — it must remain executable.
        # Credentials are redacted only when persisting to session files (memory.py).
        if config:
            from .memory import contains_credential
            if contains_credential(config):
                warning = (
                    "\n\n**⚠️ Security Warning:** The generated config contains "
                    "hardcoded credentials. Consider using environment variables "
                    "(e.g., `${DB_PASSWORD}`) instead of hardcoding secrets."
                )
                explanation = (explanation or "") + warning

        return {"config": config, "explanation": explanation}
