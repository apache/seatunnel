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

"""Connector knowledge base with runtime reflection support.

Data sources (in priority order):
  1. Runtime API  — SeaTunnel engine REST API or seatunnel-web API.
                    Live, 100% accurate, uses factory.optionRule() internally.
  2. Runtime JSON — connector_metadata.json (exported by SeaTunnelMetadataExporter)
                    Offline but 100% accurate — generated via Java runtime reflection,
                    same PluginDiscovery + factory.optionRule() as engine/web.
                    Auto-generated during CI build (seatunnel-dist package phase).

Retrieval tiers:
  L1 (Index)  — Compact list of all connectors (~1800 tokens). Always in system prompt.
  L2 (Detail) — Full options per connector. On-demand via get_connector_info tool.

Keyword routing:
  User says "mysql" → routes to ["Jdbc", "MySQL-CDC"]
  User says "s3"    → routes to ["S3File", "S3Redshift"]
"""

import json
import logging
import os
import re
import subprocess
import time
import urllib.request
import urllib.error
from pathlib import Path

logger = logging.getLogger(__name__)

# ─── Runtime API client ───

# Default REST API base — SeaTunnel engine HTTP port.
# In cluster mode (seatunnel-server.sh): REST API on port 5801 by default.
# In local mode (-m local): REST API is NOT available.
# Users can override with SEATUNNEL_API_BASE env var.
_ENGINE_API_BASE = os.environ.get("SEATUNNEL_API_BASE", "http://localhost:5801")
_API_CACHE: dict[str, dict] = {}  # memory cache: "source:Jdbc" → parsed response
_ENGINE_AVAILABLE: bool | None = None  # None = not checked yet
_ENGINE_CHECK_TS: float = 0  # timestamp of last check — recheck after 60s

# Disk cache directory for offline reuse of runtime API responses
def _get_disk_cache_dir() -> Path:
    from . import get_data_dir
    return get_data_dir() / "api_cache"


def _check_engine() -> bool:
    """Check if SeaTunnel engine REST API is reachable.

    Re-checks every 60 seconds so a newly started engine is detected mid-session.
    """
    global _ENGINE_AVAILABLE, _ENGINE_CHECK_TS
    now = time.monotonic()
    if _ENGINE_AVAILABLE is not None and (now - _ENGINE_CHECK_TS) < 60:
        return _ENGINE_AVAILABLE
    try:
        req = urllib.request.Request(f"{_ENGINE_API_BASE}/running-jobs", method="GET")
        with urllib.request.urlopen(req, timeout=2) as resp:
            _ENGINE_AVAILABLE = resp.status == 200
    except Exception:
        _ENGINE_AVAILABLE = False
    _ENGINE_CHECK_TS = now
    return _ENGINE_AVAILABLE


def _read_disk_cache(cache_key: str) -> dict | None:
    """Read a cached API response from disk."""
    path = _get_disk_cache_dir() / f"{cache_key.replace(':', '_')}.json"
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _write_disk_cache(cache_key: str, data: dict):
    """Write an API response to disk cache for offline reuse."""
    try:
        cache_dir = _get_disk_cache_dir()
        cache_dir.mkdir(parents=True, exist_ok=True)
        path = cache_dir / f"{cache_key.replace(':', '_')}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.debug(f"Failed to write disk cache for {cache_key}: {e}")


def _fetch_option_rules(plugin_type: str, plugin_name: str) -> dict | None:
    """Fetch option rules from running SeaTunnel engine.

    GET /option-rules?type=source&plugin=FakeSource
    Returns parsed JSON response or None if unavailable.

    Resolution order:
      1. In-memory cache (fastest)
      2. Live API call (authoritative)
      3. Disk cache (offline fallback from previous session)
    """
    cache_key = f"{plugin_type}:{plugin_name}"

    # 1. Memory cache
    if cache_key in _API_CACHE:
        return _API_CACHE[cache_key]

    # 2. Live API
    if _check_engine():
        try:
            url = f"{_ENGINE_API_BASE}/option-rules?type={plugin_type}&plugin={plugin_name}"
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    _API_CACHE[cache_key] = data
                    _write_disk_cache(cache_key, data)
                    return data
        except Exception as e:
            logger.debug(f"option-rules API call failed for {cache_key}: {e}")

    # 3. Disk cache (from a previous session when engine was online)
    disk_data = _read_disk_cache(cache_key)
    if disk_data:
        _API_CACHE[cache_key] = disk_data
        return disk_data

    return None


def _api_response_to_detail(resp: dict) -> dict:
    """Convert /option-rules API response to our internal detail format."""
    rule = resp.get("optionRule", {})

    required = []
    for req_opt in rule.get("requiredOptions", []):
        rule_type = req_opt.get("ruleType", "")
        for opt in req_opt.get("options", []):
            required.append(_api_opt_to_dict(opt, rule_type=rule_type))

    optional = []
    for opt in rule.get("optionalOptions", []):
        optional.append(_api_opt_to_dict(opt))

    conditional = []
    for cond in rule.get("conditionRules", []):
        # Parse the condition trigger from expressionTree or expression string
        when_key, equals_value = _parse_condition_expression(cond)
        nested_rule = cond.get("optionRule", {})
        nested_opts = []
        for req_opt in nested_rule.get("requiredOptions", []):
            for opt in req_opt.get("options", []):
                nested_opts.append(_api_opt_to_dict(opt))
        # Also include nested optional options
        for opt in nested_rule.get("optionalOptions", []):
            nested_opts.append(_api_opt_to_dict(opt))
        if when_key and nested_opts:
            conditional.append({
                "when": when_key,
                "equals": equals_value,
                "then_require": [o["key"] for o in nested_opts],
                "then_options": nested_opts,
            })

    value_constraints = [
        _value_constraint_to_dict(constraint)
        for constraint in rule.get("valueConstraints", [])
    ]

    return {
        "name": resp.get("pluginName", ""),
        "types": [resp.get("pluginType", "unknown")],
        "required": required,
        "optional": optional,
        "exclusive": [],
        "conditional": conditional,
        "value_constraints": value_constraints,
        "examples": [],
        "source": "runtime_api",
    }


def _parse_condition_expression(cond: dict) -> tuple[str, str]:
    """Extract (trigger_key, expected_value) from a conditionRule.

    The API returns conditions in two forms:
      - expressionTree: {"type": "CONDITION", "key": "fs.s3a.aws.credentials.provider",
                         "value": "SimpleAWSCredentialsProvider", "operator": "=="}
      - expression: "fs.s3a.aws.credentials.provider == SimpleAWSCredentialsProvider"

    Returns (key, value) or ("", "") if unparseable.
    """
    # Prefer expressionTree — structured, no ambiguity
    tree = cond.get("expressionTree", {})
    if tree and tree.get("key"):
        return tree.get("key", ""), str(tree.get("value", ""))

    # Fallback: parse expression string "key == value"
    expr = cond.get("expression", "")
    if "==" in expr:
        parts = expr.split("==", 1)
        return parts[0].strip(), parts[1].strip()

    return expr, ""


def _api_opt_to_dict(opt: dict, rule_type: str = "") -> dict:
    """Convert a single option from API response to our format."""
    result = {"key": opt.get("key", "")}
    java_type = opt.get("type", "")
    if java_type:
        # Simplify: "java.lang.String" → "string"
        simple = java_type.rsplit(".", 1)[-1].lower()
        result["type"] = simple
    default = opt.get("defaultValue")
    if default is not None:
        result["default"] = str(default)
    desc = opt.get("description", "")
    if desc:
        result["description"] = desc[:200]
    if rule_type:
        result["rule_type"] = rule_type
    values = opt.get("optionValues")
    if values:
        result["allowed_values"] = values
    # Preserve fallback keys — critical for correct HOCON key usage
    fallbacks = opt.get("fallbackKeys")
    if fallbacks:
        result["fallback_keys"] = fallbacks
    return result


def _value_constraint_to_dict(constraint: dict) -> dict:
    """Convert one value constraint to a compact prompt-friendly form."""
    result = {"expression": constraint.get("expression", "")}
    tree = constraint.get("conditionTree") or {}
    if not isinstance(tree, dict):
        return result

    option = tree.get("option") or {}
    key = tree.get("key") or option.get("key")
    if key:
        result["key"] = key

    expect_value = tree.get("expectValue")
    if expect_value is not None:
        result["expect"] = _constraint_value_to_text(expect_value)

    operator = tree.get("compareOperator")
    if operator:
        result["operator"] = str(operator)

    condition_operator = tree.get("conditionOperator")
    if condition_operator:
        result["condition_operator"] = str(condition_operator)

    return result


def _constraint_value_to_text(value) -> str:
    """Return a stable, prompt-friendly string for a constraint expected value."""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except TypeError:
        return str(value)


# ─── Runtime JSON metadata (from Java exporter) ───

_RUNTIME_METADATA: dict | None = None  # Parsed connector_metadata.json


def _get_runtime_metadata_paths() -> list[str]:
    """Build search paths for connector_metadata.json (evaluated lazily).

    Search order:
      1. ``$SEATUNNEL_METADATA_JSON`` — explicit override
      2. ``<package_dir>/connector_metadata.json`` — bundled with the CLI package
      3. ``<data_dir>/connector_metadata.json`` — CLI data directory
      4. ``$SEATUNNEL_HOME/connector_metadata.json`` — engine install dir
      5. ``$SEATUNNEL_HOME/cli/seatunnel_cli/connector_metadata.json`` — dist layout
    """
    from . import get_data_dir, get_seatunnel_home
    st_home = get_seatunnel_home() or ""
    return [
        os.environ.get("SEATUNNEL_METADATA_JSON", ""),
        str(Path(__file__).parent / "connector_metadata.json"),
        str(get_data_dir() / "connector_metadata.json"),
        str(Path(st_home) / "connector_metadata.json") if st_home else "",
        str(Path(st_home) / "cli" / "seatunnel_cli" / "connector_metadata.json") if st_home else "",
    ]


def _load_runtime_metadata() -> dict | None:
    """Load connector_metadata.json generated by seatunnel-metadata-export.sh.

    This JSON is produced via Java runtime reflection (factory.optionRule()),
    the same mechanism as SeaTunnel Web. It is 100% accurate.
    """
    global _RUNTIME_METADATA
    if _RUNTIME_METADATA is not None:
        return _RUNTIME_METADATA

    for path_str in _get_runtime_metadata_paths():
        if not path_str:
            continue
        p = Path(path_str)
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # Index by (name, type) for fast lookup
                _RUNTIME_METADATA = {}
                for conn in data.get("connectors", []):
                    key = f"{conn['type']}:{conn['name']}"
                    _RUNTIME_METADATA[key] = conn
                logger.info(f"Loaded runtime metadata: {len(_RUNTIME_METADATA)} entries from {p}")
                return _RUNTIME_METADATA
            except Exception as e:
                logger.debug(f"Failed to load runtime metadata from {p}: {e}")

    _RUNTIME_METADATA = {}  # Empty dict = tried but not found
    return _RUNTIME_METADATA


def _runtime_metadata_to_detail(entry: dict) -> dict:
    """Convert a runtime JSON entry to our internal detail format."""
    required = []
    for opt in entry.get("required", []):
        d = _runtime_opt_to_dict(opt)
        # Preserve condition info for conditional required options
        if opt.get("category") == "conditional" and opt.get("conditionTree"):
            cond_node = opt["conditionTree"].get("condition", {})
            if cond_node:
                d["_condition_key"] = cond_node.get("key", "")
                d["_condition_value"] = cond_node.get("expectValue", "")
        required.append(d)

    optional = []
    for opt in entry.get("optional", []):
        optional.append(_runtime_opt_to_dict(opt))

    conditional = []
    for cond_rule in entry.get("conditionRules", []):
        when_key, equals_value = "", ""
        tree = cond_rule.get("expressionTree", {})
        cond_node = tree.get("condition", {})
        if cond_node:
            when_key = cond_node.get("key", "")
            equals_value = cond_node.get("expectValue", "")
        elif cond_rule.get("expression"):
            expr = cond_rule["expression"]
            if "==" in expr:
                parts = expr.split("==", 1)
                when_key = parts[0].strip().strip("'")
                equals_value = parts[1].strip()

        nested_opts = []
        for opt in cond_rule.get("requiredOptions", []):
            nested_opts.append(_runtime_opt_to_dict(opt))
        for opt in cond_rule.get("optionalOptions", []):
            nested_opts.append(_runtime_opt_to_dict(opt))

        if when_key and nested_opts:
            conditional.append({
                "when": when_key,
                "equals": equals_value,
                "then_require": [o["key"] for o in nested_opts],
                "then_options": nested_opts,
            })

    # Also extract conditional options from required list
    for opt in entry.get("required", []):
        if opt.get("category") == "conditional" and opt.get("conditionTree"):
            cond_tree = opt["conditionTree"]
            cond_node = cond_tree.get("condition", {})
            if cond_node:
                cond_key = cond_node.get("key", "")
                cond_val = cond_node.get("expectValue", "")
                # Find or create the conditional group
                found = False
                for c in conditional:
                    if c["when"] == cond_key and c["equals"] == cond_val:
                        if opt["key"] not in c["then_require"]:
                            c["then_require"].append(opt["key"])
                            c["then_options"].append(_runtime_opt_to_dict(opt))
                        found = True
                        break
                if not found:
                    conditional.append({
                        "when": cond_key,
                        "equals": cond_val,
                        "then_require": [opt["key"]],
                        "then_options": [_runtime_opt_to_dict(opt)],
                    })

    value_constraints = [
        _value_constraint_to_dict(constraint)
        for constraint in entry.get("valueConstraints", [])
    ]

    detail = {
        "name": entry.get("name", ""),
        "types": [entry.get("type", "unknown")],
        "required": required,
        "optional": optional,
        "exclusive": [],
        "conditional": conditional,
        "value_constraints": value_constraints,
        "examples": [],
        "source": "runtime_json",
    }

    # Enrich weak descriptions — metadata-driven, not connector-specific rules.
    # Many Java connectors have description == key (e.g., url → "url", bucket → "S3 bucket").
    # These enrichments add format hints so the LLM generates correct values.
    _enrich_descriptions(detail)

    return detail


# Description enrichments for options where Java source has weak descriptions.
# Keyed by option key — applied generically across all connectors that use the key.
# These are factual format descriptions, not connector-specific business rules.
_DESCRIPTION_ENRICHMENTS: dict[str, str] = {
    "url": (
        "JDBC connection URL. Must include the database name. "
        "Format: jdbc:<db>://{host}:{port}/{database} "
        "(see JDBC Driver Reference for per-database URL patterns)"
    ),
    "bucket": (
        "S3 bucket with scheme prefix. Must start with 's3a://' "
        "(e.g., 's3a://my-bucket-name')"
    ),
    "driver": (
        "JDBC driver class name (see JDBC Driver Reference for per-database drivers)"
    ),
}


def _enrich_descriptions(detail: dict) -> None:
    """Enrich weak option descriptions with format hints."""
    for opt in detail.get("required", []) + detail.get("optional", []):
        key = opt.get("key", "")
        desc = opt.get("description", "")
        enrichment = _DESCRIPTION_ENRICHMENTS.get(key)
        if enrichment and (not desc or desc.strip().lower() == key.strip().lower() or len(desc) < 10):
            opt["description"] = enrichment


def _runtime_opt_to_dict(opt: dict) -> dict:
    """Convert a single option from runtime JSON to our internal format."""
    result = {"key": opt.get("key", "")}
    raw_type = opt.get("type", "")
    if raw_type:
        # New metadata already has clean types (string, boolean, enum<X>, list<string>).
        # Legacy metadata has Java full class names — strip package prefix as fallback.
        if raw_type.startswith("java.") or "." in raw_type.split("<")[0]:
            result["type"] = raw_type.rsplit(".", 1)[-1].lower()
        else:
            result["type"] = raw_type
    default = opt.get("defaultValue")
    if default is not None:
        result["default"] = str(default)
    desc = opt.get("description", "")
    if desc:
        result["description"] = desc[:200]
    category = opt.get("category", "")
    if category:
        result["rule_type"] = category
    values = opt.get("optionValues")
    if values:
        result["allowed_values"] = values
    fallbacks = opt.get("fallbackKeys")
    if fallbacks:
        result["fallback_keys"] = fallbacks
    return result


def export_runtime_metadata(output_path: str | None = None) -> str | None:
    """Run seatunnel-metadata-export.sh to generate connector_metadata.json.

    Returns the output file path on success, None on failure.
    """
    sh_path = _find_metadata_export_sh()
    if not sh_path:
        return None

    if output_path is None:
        from . import get_data_dir
        output_path = str(get_data_dir() / "connector_metadata.json")

    try:
        cmd = ["sh", sh_path, "-o", output_path]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if proc.returncode == 0:
            logger.info(f"Exported runtime metadata to {output_path}")
            # Reset cache so next load picks up new file
            global _RUNTIME_METADATA
            _RUNTIME_METADATA = None
            return output_path
        else:
            logger.warning(f"Metadata export failed: {proc.stderr[:300]}")
            return None
    except Exception as e:
        logger.warning(f"Metadata export error: {e}")
        return None


def _find_metadata_export_sh() -> str | None:
    """Locate seatunnel-metadata-export.sh script."""
    seatunnel_home = os.environ.get("SEATUNNEL_HOME", "")
    if seatunnel_home:
        path = os.path.join(seatunnel_home, "bin", "seatunnel-metadata-export.sh")
        if os.path.exists(path):
            return path

    project_root = Path(__file__).parent.parent.parent
    path = str(project_root / "bin" / "seatunnel-metadata-export.sh")
    if os.path.exists(path):
        return path
    return None

# ─── Unified metadata fetcher (two-tier priority) ───

def fetch_connector_metadata(
    plugin_name: str, plugin_type: str,
) -> dict | None:
    """Fetch precise connector option rules via runtime sources.

    Priority:
      1. Runtime API (live /option-rules endpoint — always accurate, from engine/web)
      2. Runtime JSON (connector_metadata.json — bundled with CLI package)

    Returns internal detail dict or None. The dict includes 'source' field
    indicating which tier provided the data.
    """
    # Tier 1: Runtime API (engine or seatunnel-web)
    api_resp = _fetch_option_rules(plugin_type, plugin_name)
    if api_resp:
        detail = _api_response_to_detail(api_resp)
        logger.info(f"Metadata for {plugin_type}:{plugin_name} from runtime API")
        return detail

    # Tier 2: Runtime JSON (connector_metadata.json)
    runtime_meta = _load_runtime_metadata()
    if runtime_meta:
        cache_key = f"{plugin_type}:{plugin_name}"
        entry = runtime_meta.get(cache_key)
        if entry:
            detail = _runtime_metadata_to_detail(entry)
            logger.info(f"Metadata for {plugin_type}:{plugin_name} from runtime JSON")
            return detail

    logger.warning(f"No metadata found for {plugin_type}:{plugin_name}")
    return None


def format_metadata_for_prompt(metadata: dict, plugin_name: str, plugin_type: str) -> str:
    """Format connector metadata for LLM system prompt — accuracy-optimized.

    Strategy:
      1. ALWAYS required — listed first, full detail (these must always be in config)
      2. Conditional required — grouped by trigger condition (only needed when condition matches)
      3. Important optional — username/password/query/table_path with type info
      4. Other optional — compact key list

    This separation is CRITICAL: without it the LLM includes conditional options
    (xml_root_tag, row_delimiter) even when their trigger doesn't match (e.g. PARQUET format).
    """
    lines = [f"### {plugin_name} ({plugin_type.upper()}) [source: {metadata.get('source', 'unknown')}]"]

    # Split required into absolutely_required vs conditional
    abs_required = []
    conditional_opts: dict[str, list[dict]] = {}  # "key=value" → [opts]

    for opt in metadata.get("required", []):
        rule_type = opt.get("rule_type", "absolutely_required")
        if rule_type == "conditional" and opt.get("_condition_key"):
            cond_label = f"`{opt['_condition_key']}` = `{opt.get('_condition_value', '')}`"
            conditional_opts.setdefault(cond_label, []).append(opt)
        else:
            abs_required.append(opt)

    # 1. Always required
    if abs_required:
        lines.append("**ALWAYS Required (must include):**")
        for opt in abs_required:
            lines.append(_format_opt_detail(opt))

    # Exclusive/bundled rules
    if metadata.get("exclusive"):
        excl = ", ".join(f"`{k}`" for k in metadata["exclusive"])
        lines.append(f"**Exclusive (pick one):** {excl}")

    # 2. Conditional required — grouped by trigger
    # Merge from both the required list and the separate conditional list
    for cond in metadata.get("conditional", []):
        eq = cond.get("equals", "")
        cond_label = f"`{cond['when']}` = `{eq}`" if eq else f"`{cond['when']}` is set"
        for opt_info in cond.get("then_options", []):
            conditional_opts.setdefault(cond_label, []).append(opt_info)

    if conditional_opts:
        lines.append("**Conditional (ONLY include when condition matches):**")
        for cond_label, opts in conditional_opts.items():
            # Deduplicate by key
            seen = set()
            unique_opts = []
            for o in opts:
                if o["key"] not in seen:
                    seen.add(o["key"])
                    unique_opts.append(o)
            opt_list = ", ".join(f"`{o['key']}`" for o in unique_opts)
            lines.append(f"  - When {cond_label} → {opt_list}")

    if metadata.get("value_constraints"):
        lines.append("**Value Constraints (must satisfy):**")
        for constraint in metadata["value_constraints"]:
            lines.append(_format_value_constraint(constraint, prefix="  - "))

    # 3. Optional: show important ones with type, rest as key list
    important_keys = {"username", "password", "query", "table_path", "table_list",
                      "user", "bucket", "path", "topic", "database", "table",
                      "access_key", "secret_key", "schema_save_mode", "data_save_mode",
                      "file_format_type", "tmp_path", "custom_filename", "have_partition"}
    important_opts = []
    other_opt_keys = []
    for opt in metadata.get("optional", []):
        if opt["key"] in important_keys:
            important_opts.append(opt)
        else:
            other_opt_keys.append(opt["key"])

    if important_opts:
        lines.append("**Key Optional:**")
        for opt in important_opts:
            lines.append(_format_opt_detail(opt))

    if other_opt_keys:
        lines.append(f"**Other Optional ({len(other_opt_keys)}):** {', '.join(f'`{k}`' for k in other_opt_keys)}")

    return "\n".join(lines)


def _format_value_constraint(constraint: dict, prefix: str = "- ") -> str:
    """Format one value constraint without attempting to evaluate it locally."""
    key = constraint.get("key")
    operator = constraint.get("operator")
    expect = constraint.get("expect")
    expression = constraint.get("expression", "")

    if key and operator and expect and str(operator).lower() == "extension":
        line = f"{prefix}`{key}` {expect}"
    elif key and operator and expect:
        line = f"{prefix}`{key}` {operator} {expect}"
    elif key and expect:
        line = f"{prefix}`{key}` {expect}"
    elif expression:
        line = f"{prefix}{expression}"
    else:
        line = f"{prefix}<unspecified constraint>"

    if expression and expression not in line.replace("`", ""):
        line += f" ({expression})"
    return line


def _format_opt_detail(opt: dict) -> str:
    """Format one option with full detail for LLM prompt."""
    line = f"  - `{opt['key']}` ({opt.get('type', '?')})"
    if opt.get("default") is not None and str(opt["default"]) != "None":
        line += f" [default: {opt['default']}]"
    if opt.get("fallback_keys"):
        line += f" [aliases: {', '.join(opt['fallback_keys'])}]"
    if opt.get("allowed_values"):
        line += f" [values: {', '.join(str(v) for v in opt['allowed_values'])}]"
    if opt.get("description"):
        line += f" — {opt['description']}"
    return line


# ─── Keyword routing: maps user terms → connector names ───

KEYWORD_ALIASES: dict[str, list[str]] = {
    # Databases
    "mysql": ["Jdbc", "MySQL-CDC"],
    "postgres": ["Jdbc", "PostgreSQL-CDC"],
    "postgresql": ["Jdbc", "PostgreSQL-CDC"],
    "oracle": ["Jdbc", "Oracle-CDC"],
    "sqlserver": ["Jdbc", "SqlServer-CDC"],
    "sql server": ["Jdbc", "SqlServer-CDC"],
    "mssql": ["Jdbc", "SqlServer-CDC"],
    "db2": ["Jdbc"],
    "sqlite": ["Jdbc"],
    "dameng": ["Jdbc"],
    "dm": ["Jdbc"],
    "tidb": ["Jdbc", "TiDB-CDC"],
    "mariadb": ["Jdbc"],

    # NoSQL
    "mongo": ["MongoDB", "MongoDB-CDC"],
    "mongodb": ["MongoDB", "MongoDB-CDC"],
    "redis": ["Redis"],
    "cassandra": ["Cassandra"],
    "hbase": ["Hbase"],
    "neo4j": ["Neo4j"],

    # Messaging
    "kafka": ["Kafka"],
    "pulsar": ["Pulsar"],
    "rabbitmq": ["RabbitMQ"],
    "rocketmq": ["RocketMQ"],
    "activemq": ["ActiveMQ"],
    "mqtt": ["Mqtt"],
    "sqs": ["AmazonSqs"],

    # Search
    "elasticsearch": ["Elasticsearch"],
    "es": ["Elasticsearch"],
    "easysearch": ["Easysearch"],

    # File / Object Store
    "s3": ["S3File", "S3Redshift"],
    "hdfs": ["HdfsFile"],
    "local file": ["LocalFile"],
    "csv": ["LocalFile", "S3File", "HdfsFile"],
    "parquet": ["LocalFile", "S3File", "HdfsFile"],
    "json file": ["LocalFile", "S3File", "HdfsFile"],
    "orc": ["LocalFile", "S3File", "HdfsFile"],
    "ftp": ["FtpFile"],
    "sftp": ["SftpFile"],
    "oss": ["OssFile"],

    # OLAP / Data Warehouse
    "clickhouse": ["Clickhouse"],
    "doris": ["Doris"],
    "starrocks": ["StarRocks"],
    "hive": ["Hive"],
    "maxcompute": ["Maxcompute"],
    "redshift": ["S3Redshift"],
    "databend": ["Databend"],

    # Lake
    "iceberg": ["Iceberg"],
    "paimon": ["Paimon"],
    "hudi": ["Hudi"],

    # API
    "http": ["Http"],
    "api": ["Http", "GraphQL"],
    "graphql": ["GraphQL"],
    "webhook": ["Http"],

    # AI / Vector
    "milvus": ["Milvus"],
    "qdrant": ["Qdrant"],
    "pinecone": ["Pinecone"],

    # Other
    "console": ["Console"],
    "fake": ["FakeSource"],
    "test": ["FakeSource", "Console"],
    "dynamodb": ["AmazonDynamoDB"],
    "influxdb": ["InfluxDB"],
    "iotdb": ["IoTDB"],
    "tdengine": ["TDengine"],

    # CDC generic
    "cdc": ["MySQL-CDC", "PostgreSQL-CDC", "Oracle-CDC", "SqlServer-CDC", "MongoDB-CDC", "TiDB-CDC"],
    "binlog": ["MySQL-CDC"],
    "wal": ["PostgreSQL-CDC"],
    "实时同步": ["MySQL-CDC", "PostgreSQL-CDC", "Kafka"],
    "增量": ["MySQL-CDC", "PostgreSQL-CDC", "Jdbc"],
    "全量": ["Jdbc"],
}

# ─── Sink multi-table capability ───
# Sinks that natively support multiple logical tables in one block.
# All other sinks require pipeline expansion (one source→sink per table).
_MULTI_TABLE_SINKS = frozenset({
    "Jdbc", "Doris", "StarRocks", "Paimon", "Iceberg", "Hudi",
})


def sink_supports_multi_table(sink_name: str) -> bool:
    """Return True if *sink_name* can accept multiple tables in one block."""
    return sink_name in _MULTI_TABLE_SINKS


# JDBC driver and URL knowledge (enriches Jdbc connector detail)
JDBC_DRIVERS = {
    "mysql": {"driver": "com.mysql.cj.jdbc.Driver", "url": "jdbc:mysql://{host}:{port}/{database}", "port": 3306},
    "postgresql": {"driver": "org.postgresql.Driver", "url": "jdbc:postgresql://{host}:{port}/{database}", "port": 5432},
    "oracle": {"driver": "oracle.jdbc.driver.OracleDriver", "url": "jdbc:oracle:thin:@{host}:{port}:{sid}", "port": 1521},
    "sqlserver": {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver", "url": "jdbc:sqlserver://{host}:{port};databaseName={database}", "port": 1433},
    "db2": {"driver": "com.ibm.db2.jcc.DB2Driver", "url": "jdbc:db2://{host}:{port}/{database}", "port": 50000},
    "sqlite": {"driver": "org.sqlite.JDBC", "url": "jdbc:sqlite:{path}", "port": None},
    "dm": {"driver": "dm.jdbc.driver.DmDriver", "url": "jdbc:dm://{host}:{port}", "port": 5236},
}


def infer_db_type(text: str) -> str:
    """Infer database type from free-form text (user request / plan)."""
    lower = text.lower()
    for db_type in ("mysql", "postgresql", "postgres", "oracle", "sqlserver", "db2", "dm", "sqlite"):
        if db_type in lower:
            return "postgresql" if db_type == "postgres" else db_type
    if "jdbc" in lower:
        return "mysql"
    return "mysql"


def resolve_jdbc_options(
    user_request: str,
    host: str = "localhost",
    port: str = "",
    database: str = "",
) -> dict[str, str]:
    """Resolve JDBC driver, URL, and port from user context.

    Returns dict with keys: ``url``, ``driver``, ``port``, ``db_type``.
    Uses ``JDBC_DRIVERS`` as the single source of truth.
    """
    db_type = infer_db_type(user_request)
    info = JDBC_DRIVERS.get(db_type, JDBC_DRIVERS["mysql"])
    if not port:
        port = str(info["port"] or "")
    url = info["url"].format(host=host, port=port, database=database, sid=database, path=database)
    return {
        "url": url,
        "driver": info["driver"],
        "port": port,
        "db_type": db_type,
    }


# ─── Transform plugins (kept inline — small, stable set) ───

TRANSFORMS = {
    "Sql": {
        "description": "Transform data using SQL expressions.",
        "required": ["plugin_input", "plugin_output", "query"],
        "examples": '''Sql {
    plugin_input = "fake"
    plugin_output = "sql_out"
    query = "SELECT id, UPPER(name) as name, age + 1 as age FROM fake WHERE age > 18"
}''',
    },
    "Filter": {
        "description": "Filter rows by condition.",
        "required": ["plugin_input", "plugin_output", "fields"],
        "examples": '''Filter {
    plugin_input = "source"
    plugin_output = "filtered"
    fields = ["id", "name", "age"]
}''',
    },
    "Replace": {
        "description": "Replace field values using regex.",
        "required": ["plugin_input", "plugin_output", "replace_field", "pattern", "replacement"],
        "examples": '''Replace {
    plugin_input = "source"
    plugin_output = "replaced"
    replace_field = "phone"
    pattern = "(\\d{3})\\d{4}(\\d{4})"
    replacement = "$1****$2"
}''',
    },
    "Split": {
        "description": "Split a field into multiple fields.",
        "required": ["plugin_input", "plugin_output", "separator", "split_field"],
        "optional": ["output_fields"],
        "examples": '''Split {
    plugin_input = "source"
    plugin_output = "split_out"
    separator = ","
    split_field = "tags"
    output_fields = ["tag1", "tag2", "tag3"]
}''',
    },
    "FieldMapper": {
        "description": "Rename, remove, or reorder fields.",
        "required": ["plugin_input", "plugin_output", "field_mapper"],
        "examples": '''FieldMapper {
    plugin_input = "source"
    plugin_output = "mapped"
    field_mapper = {
        id = id
        user_name = name
        user_age = age
    }
}''',
    },
    "Copy": {
        "description": "Copy one or more fields to new fields.",
        "required": ["plugin_input", "plugin_output", "fields"],
        "examples": '''Copy {
    plugin_input = "source"
    plugin_output = "copied"
    fields {
        name_backup = "name"
    }
}''',
    },
    "LLM": {
        "description": "Transform data using LLM (Large Language Model) inference.",
        "required": ["plugin_input", "plugin_output", "model_provider", "model", "prompt"],
        "optional": ["openai.api_key", "output_data_type"],
        "examples": '''LLM {
    plugin_input = "source"
    plugin_output = "llm_out"
    model_provider = OPENAI
    model = "gpt-4"
    openai.api_key = "${OPENAI_API_KEY}"
    prompt = "Classify the following text into categories: {{content}}"
    output_data_type = "string"
}''',
    },
    "Embedding": {
        "description": "Generate vector embeddings for text fields.",
        "required": ["plugin_input", "plugin_output", "model_provider", "model", "api_key",
                     "single_vectorized_input_number", "vectorized_fields"],
        "examples": '''Embedding {
    plugin_input = "source"
    plugin_output = "embed_out"
    model_provider = OPENAI
    model = "text-embedding-3-small"
    api_key = "${OPENAI_API_KEY}"
    single_vectorized_input_number = 1
    vectorized_fields {
        content_vector = ["content"]
    }
}''',
    },
}


# ─── Public API ───

def get_connector_catalog() -> str:
    """L1: Build compact connector index for system prompt.

    Sources: runtime metadata (connector_metadata.json) + keyword aliases.
    Format per-type:
      - ConnectorName (source) | required: key1, key2 | +N optional
      - ConnectorName (sink)   | required: key3, key4 | +M optional
    """
    engine_status = "connected" if _check_engine() else "offline"
    runtime_meta = _load_runtime_metadata()

    # Build index from runtime metadata
    # Group by connector name → collect types
    connectors: dict[str, dict[str, dict]] = {}  # name → {type → entry}
    if runtime_meta:
        for key, entry in runtime_meta.items():
            name = entry.get("name", "")
            ctype = entry.get("type", "")
            if name and ctype:
                connectors.setdefault(name, {})[ctype] = entry

    count = sum(len(types) for types in connectors.values())
    lines = [f"## Available Connectors ({len(connectors)} connectors, {count} endpoints) [engine: {engine_status}]\n"]
    lines.append(
        "Use `get_connector_info` tool with `connector_type` "
        "('source', 'sink', or 'transform') to get type-specific details."
    )
    if engine_status == "connected":
        lines.append("(Details fetched live from running engine — always up-to-date)\n")
    elif runtime_meta:
        lines.append("(Details from runtime metadata — generated via Java reflection, 100% accurate)\n")
    else:
        lines.append("(No metadata available — run `seatunnel --export-metadata` to generate)\n")

    for name in sorted(connectors.keys()):
        types = connectors[name]
        for ftype in ("source", "sink", "transform"):
            if ftype not in types:
                continue
            entry = types[ftype]
            req_keys = [opt.get("key", "") for opt in entry.get("required", [])]
            opt_count = len(entry.get("optional", []))
            req = ", ".join(req_keys[:6])
            if len(req_keys) > 6:
                req += "..."
            lines.append(f"- **{name}** ({ftype}) | required: {req or 'none'} | +{opt_count} optional")

    lines.append("\n## Available Transforms\n")
    for name, info in TRANSFORMS.items():
        lines.append(f"- **{name}**: {info['description']}")

    lines.append("\n## JDBC Database Support")
    lines.append("Jdbc connector supports: " + ", ".join(JDBC_DRIVERS.keys()))

    return "\n".join(lines)


def get_connector_detail(name: str, connector_type: str | None = None) -> str | None:
    """L2: Get full details for one connector (on-demand, via tool call).

    Args:
        name: Connector name (e.g., 'Jdbc', 'Kafka', 'S3File').
        connector_type: 'source', 'sink', or 'transform' for type-specific options.
                        None returns all available types with separate sections.

    Resolution order:
      1. Runtime API (live /option-rules endpoint — always accurate)
      2. Runtime JSON (connector_metadata.json — Java reflection export)
      3. Transform plugins (inline dict)
      4. Keyword suggestion

    Returns a formatted string with all options, defaults, conditions, and examples.
    """
    # ── 1. Try runtime API ──
    query_types = [connector_type] if connector_type else ["source", "sink", "transform"]
    api_details: dict[str, dict] = {}
    for ptype in query_types:
        api_resp = _fetch_option_rules(ptype, name)
        if api_resp:
            api_details[ptype] = _api_response_to_detail(api_resp)

    if api_details:
        formatted = _format_connector_detail_typed(name, api_details)
        return f"[source: runtime API]\n{formatted}"

    # ── 2. Try runtime JSON ──
    runtime_meta = _load_runtime_metadata()
    if runtime_meta:
        runtime_details: dict[str, dict] = {}
        for ptype in query_types:
            cache_key = f"{ptype}:{name}"
            entry = runtime_meta.get(cache_key)
            if entry:
                runtime_details[ptype] = _runtime_metadata_to_detail(entry)
        if runtime_details:
            formatted = _format_connector_detail_typed(name, runtime_details,
                                                       connector_type=connector_type)
            return f"[source: runtime JSON]\n{formatted}"

    # ── 3. Check transforms ──
    t = TRANSFORMS.get(name)
    if not t:
        for k, v in TRANSFORMS.items():
            if k.lower() == name.lower():
                t = v
                name = k
                break
    if t:
        return _format_transform_detail(name, t)

    # ── 4. Suggest ──
    suggestions = route_by_keyword(name)
    if suggestions:
        return f"Connector '{name}' not found. Did you mean: {', '.join(suggestions)}?\nUse get_connector_info with the exact name."
    return f"Connector '{name}' not found. Use list_connectors to see all available connectors."



def _format_connector_detail_typed(
    name: str,
    typed_details: dict[str, dict],
    examples: list[str] | None = None,
    connector_type: str | None = None,
) -> str:
    """Format per-type connector options into a readable string for the LLM.

    Args:
        name: Connector name.
        typed_details: {type_name: {required, optional, exclusive, conditional}}.
        examples: Config examples from docs.
        connector_type: If set, only format this type's options.
    """
    lines = [f"# {name}"]
    available_types = sorted(typed_details.keys())
    lines.append(f"Types: {', '.join(available_types)}\n")

    # Filter to requested type
    show_types = [connector_type] if connector_type and connector_type in typed_details else available_types

    for ftype in show_types:
        detail = typed_details[ftype]
        type_label = ftype.upper()

        # Required options
        if detail.get("required"):
            lines.append(f"## {type_label} — Required Options")
            for opt in detail["required"]:
                lines.append(_format_option_line(opt))

        # Optional options
        if detail.get("optional"):
            lines.append(f"\n## {type_label} — Optional Options")
            for opt in detail["optional"]:
                lines.append(_format_option_line(opt))

        # Exclusive options
        if detail.get("exclusive"):
            lines.append(f"\n## {type_label} — Mutually Exclusive (pick one): {', '.join(detail['exclusive'])}")

        # Conditional options
        if detail.get("conditional"):
            lines.append(f"\n## {type_label} — Conditional Options")
            for cond in detail["conditional"]:
                deps = ", ".join(cond["then_require"])
                lines.append(f"- When `{cond['when']}` = `{cond['equals']}` → also require: {deps}")

        if detail.get("value_constraints"):
            lines.append(f"\n## {type_label} — Value Constraints")
            for constraint in detail["value_constraints"]:
                lines.append(_format_value_constraint(constraint))

        if len(show_types) > 1:
            lines.append("")  # separator between types

    # JDBC enrichment
    if name == "Jdbc":
        lines.append("\n## JDBC Driver Reference")
        for db, info in JDBC_DRIVERS.items():
            lines.append(f"- **{db}**: driver=`{info['driver']}` url=`{info['url']}` port={info['port']}")

    # Examples from docs
    if examples:
        lines.append("\n## Config Examples (from docs)")
        for i, ex in enumerate(examples[:2]):
            lines.append(f"\n### Example {i+1}:")
            lines.append(f"```hocon\n{ex}\n```")

    return "\n".join(lines)


def _format_option_line(opt: dict) -> str:
    """Format a single option as a markdown list item."""
    desc = opt.get("description", "")
    typ = opt.get("type", "")
    default = opt.get("default")
    line = f"- `{opt['key']}` ({typ})"
    if default is not None:
        line += f" [default: {default}]"
    if opt.get("fallback_keys"):
        line += f" [aliases: {', '.join(opt['fallback_keys'])}]"
    if opt.get("allowed_values"):
        line += f" [values: {', '.join(str(v) for v in opt['allowed_values'])}]"
    if desc:
        line += f" — {desc}"
    return line


def _format_transform_detail(name: str, info: dict) -> str:
    """Format a transform plugin detail."""
    lines = [f"# Transform: {name}\n", info["description"], ""]
    lines.append(f"**Required**: {', '.join(info['required'])}")
    if info.get("optional"):
        lines.append(f"**Optional**: {', '.join(info['optional'])}")
    examples = info.get("examples", "")
    if examples:
        lines.append(f"\n**Example**:\n```hocon\n{examples}\n```")
    return "\n".join(lines)


def route_by_keyword(user_text: str) -> list[str]:
    """Route user's natural language to relevant connector names.

    Used by PlannerAgent to narrow down which connectors to look up.
    Returns up to 6 most relevant connector names.
    """
    text_lower = user_text.lower()
    matches = set()

    # Check keyword aliases
    for keyword, connectors in KEYWORD_ALIASES.items():
        if keyword in text_lower:
            matches.update(connectors)

    # Also check against runtime metadata connector names directly
    runtime_meta = _load_runtime_metadata()
    if runtime_meta:
        for key in runtime_meta:
            # key format: "source:Jdbc"
            parts = key.split(":", 1)
            if len(parts) == 2:
                name = parts[1]
                name_lower = name.lower()
                if name_lower in text_lower or text_lower in name_lower:
                    matches.add(name)

    return sorted(matches)[:6]


def list_connector_names() -> dict:
    """Return categorized connector names from runtime metadata."""
    runtime_meta = _load_runtime_metadata()
    sources = set()
    sinks = set()
    transforms = set(TRANSFORMS.keys())
    if runtime_meta:
        for key in runtime_meta:
            parts = key.split(":", 1)
            if len(parts) == 2:
                ctype, name = parts
                if ctype == "source":
                    sources.add(name)
                elif ctype == "sink":
                    sinks.add(name)
                elif ctype == "transform":
                    transforms.add(name)
    return {"sources": sorted(sources), "sinks": sorted(sinks), "transforms": sorted(transforms)}


def validate_connector_options(
    connector_name: str, provided_keys: set[str], connector_type: str | None = None,
) -> dict:
    """Validate that provided config keys satisfy the connector's requirements.

    Args:
        connector_name: Connector name (e.g., 'Jdbc').
        provided_keys: Set of config keys provided by the user.
        connector_type: 'source' or 'sink' — validates against type-specific options.
                        None falls back to merged validation.

    Returns: {"valid": bool, "missing_required": [...], "unknown_keys": [...]}
    """
    type_detail = None
    if connector_type:
        runtime = fetch_connector_metadata(connector_name, connector_type)
        if runtime:
            type_detail = runtime

    if not type_detail:
        return {"valid": True, "missing_required": [], "unknown_keys": [], "note": "connector metadata not available"}

    # Only absolutely_required options are truly mandatory.
    # Conditional required options depend on trigger conditions.
    abs_required_keys = set()
    all_required_keys = set()
    for opt in type_detail.get("required", []):
        all_required_keys.add(opt["key"])
        rule_type = opt.get("rule_type", "absolutely_required")
        if rule_type == "absolutely_required":
            abs_required_keys.add(opt["key"])

    optional_keys = {opt["key"] for opt in type_detail.get("optional", [])}
    all_known = all_required_keys | optional_keys

    # Also add conditional option keys from the conditional list
    for cond in type_detail.get("conditional", []):
        for k in cond.get("then_require", []):
            all_known.add(k)

    # Build fallback map: primary_key -> set of fallback keys
    # A required key is satisfied if the primary key OR any fallback key is present
    fallback_map: dict[str, set[str]] = {}
    for opt in type_detail.get("required", []) + type_detail.get("optional", []):
        fb = opt.get("fallback_keys", [])
        if fb:
            fallback_map[opt["key"]] = set(fb)
            all_known.update(fb)

    missing = set()
    for key in abs_required_keys:
        if key in provided_keys:
            continue
        fallbacks = fallback_map.get(key, set())
        if not (fallbacks & provided_keys):
            missing.add(key)

    unknown = provided_keys - all_known

    return {
        "valid": len(missing) == 0,
        "missing_required": sorted(missing),
        "unknown_keys": sorted(unknown),
    }
