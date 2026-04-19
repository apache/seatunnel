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

"""Build connector catalog by scanning Java source code.

Scans seatunnel-connectors-v2/ to extract:
  - Factory identifiers (connector names)
  - Option definitions (key, type, default, description)
  - OptionRules (required, optional, conditional, exclusive)
  - Config examples from docs/

Outputs: connector_catalog.json (two-tier: index + full details)

Usage:
    python -m seatunnel_cli.build_catalog [--seatunnel-root /path/to/seatunnel]
"""

import json
import os
import re
import sys
from pathlib import Path


# ─── Regex patterns for Java source extraction ───

# Match: Options.key("name").stringType().defaultValue(x).withDescription("desc")
# Handles multiline definitions
RE_OPTION_DEF = re.compile(
    r'Options\.key\(\s*"([^"]+)"\s*\)'  # key name
    r'\s*\.(\w+Type)\(\s*([^)]*)\s*\)'  # type method + optional arg
    r'(?:\s*\.(\w+)\(\s*([^)]*)\s*\))*',  # chained calls (default, description, etc.)
    re.DOTALL,
)

# Simpler: extract key name (string literal)
RE_KEY = re.compile(r'Options\.key\(\s*"([^"]+)"\s*\)')

# Extract key from constant reference: Options.key(SomeClass.SOME_KEY)
RE_KEY_CONST = re.compile(r'Options\.key\(\s*([\w.]+)\s*\)')

# Extract type method
RE_TYPE = re.compile(r'\.\s*(stringType|intType|longType|floatType|doubleType|booleanType|enumType|singleChoice|listType|mapType|mapObjectType|bigDecimalType|durationType|type)\s*\(')

# Extract default value
RE_DEFAULT = re.compile(r'\.defaultValue\(\s*(.+?)\s*\)')

# Extract noDefaultValue
RE_NO_DEFAULT = re.compile(r'\.noDefaultValue\(\)')

# Extract description
RE_DESCRIPTION = re.compile(r'\.withDescription\(\s*"((?:[^"\\]|\\.)*)"\s*\)')

# Extract fallback keys: .withFallbackKeys("key1", "key2")
RE_FALLBACK_KEYS = re.compile(r'\.withFallbackKeys\(\s*"([^"]+)"(?:\s*,\s*"([^"]+)")*\s*\)')

# Extract Option<T> FIELD_NAME — multiple declaration styles:
#   public static final Option<String> X =        (class fields)
#   public static Option<Integer> X =             (non-final class fields)
#   Option<Map<String, Object>> X =               (interface fields, implicitly public static final)
# Field name must be UPPER_CASE to avoid matching local variables
# Handles up to 3 levels of nested generics: Option<List<Map<String, Object>>>
RE_OPTION_FIELD = re.compile(
    r'(?:public\s+)?(?:static\s+)?(?:final\s+)?(?:SingleChoiceOption|Option)<((?:[^<>]|<(?:[^<>]|<[^<>]*>)*>)*)>\s+([A-Z][A-Z0-9_]*)\s*='
)

# Factory identifier
RE_FACTORY_ID = re.compile(r'factoryIdentifier\s*\(\s*\)\s*\{[^}]*return\s+"([^"]+)"', re.DOTALL)
RE_FACTORY_ID_CONST = re.compile(
    r'factoryIdentifier\s*\(\s*\)\s*\{[^}]*?return\s+([\w.]+(?:\s*\.\s*\w+)*)',
    re.DOTALL,
)

# OptionRule patterns — use balanced-paren extraction instead of regex
RE_RULE_CALL = re.compile(r'\.(required|optional|exclusive|conditional)\s*\(', re.DOTALL)


def _extract_balanced_args(text: str, start: int) -> str | None:
    """Extract the balanced parenthesized args starting at text[start] == '('."""
    if start >= len(text) or text[start] != "(":
        return None
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return text[start + 1:i]
    return None

# Factory class type detection — match direct implements OR extends known base classes
RE_IMPLEMENTS_SOURCE = re.compile(
    r'implements\s+.*TableSourceFactory'
    r'|extends\s+\w*(?:Source|Incremental|ChangeStream)\w*Factory'
)
RE_IMPLEMENTS_SINK = re.compile(
    r'implements\s+.*TableSinkFactory'
    r'|extends\s+\w*Sink\w*Factory'
)


def find_seatunnel_root() -> Path:
    """Find the seatunnel project root directory."""
    # Try relative to this file
    candidates = [
        Path(__file__).parent.parent.parent,  # seatunnel-cli/../
        Path.cwd(),
        Path.cwd().parent,
    ]
    for c in candidates:
        if (c / "seatunnel-connectors-v2").is_dir():
            return c
    raise FileNotFoundError("Cannot find seatunnel project root. Use --seatunnel-root flag.")


def scan_option_fields(java_content: str) -> dict[str, dict]:
    """Extract all Option field definitions from a Java file.

    Returns: {FIELD_NAME: {key, java_type, value_type, default, description}}
    """
    options = {}

    # Split by option field declarations
    for match in RE_OPTION_FIELD.finditer(java_content):
        java_type = match.group(1).strip()
        field_name = match.group(2).strip()

        # Get the full definition text (from this match to next option or end of block)
        start = match.start()
        # Find the semicolon that ends this statement
        semi_pos = java_content.find(";", match.end())
        if semi_pos == -1:
            continue
        definition = java_content[start:semi_pos + 1]

        opt = {"field_name": field_name, "java_type": java_type}

        # Extract key (string literal or constant reference)
        key_match = RE_KEY.search(definition)
        if key_match:
            opt["key"] = key_match.group(1)
        else:
            const_match = RE_KEY_CONST.search(definition)
            if const_match:
                opt["key_const_ref"] = const_match.group(1)

        # Extract type method
        type_match = RE_TYPE.search(definition)
        if type_match:
            opt["value_type"] = type_match.group(1).replace("Type", "")

        # Extract default
        default_match = RE_DEFAULT.search(definition)
        if default_match:
            raw = default_match.group(1).strip()
            # Clean up common patterns
            raw = raw.rstrip("dDfFlL")  # remove numeric suffixes
            if raw.startswith('"') and raw.endswith('"'):
                raw = raw[1:-1]
            opt["default"] = raw
        elif RE_NO_DEFAULT.search(definition):
            opt["default"] = None

        # Extract description
        desc_match = RE_DESCRIPTION.search(definition)
        if desc_match:
            opt["description"] = desc_match.group(1).replace('\\"', '"').replace("\\n", " ")

        # Extract fallback keys
        fallback_match = RE_FALLBACK_KEYS.search(definition)
        if fallback_match:
            fallbacks = [g for g in fallback_match.groups() if g]
            if fallbacks:
                opt["fallback_keys"] = fallbacks

        if "key" in opt or "key_const_ref" in opt:
            options[field_name] = opt

    return options


def scan_factory(java_content: str, file_path: str,
                 identifier_lookup: dict[str, str] | None = None) -> dict | None:
    """Extract factory metadata: identifier, type (source/sink), and option rules.

    Args:
        identifier_lookup: Pre-built map of ClassName.FIELD -> "string value"
                           for resolving cross-file constant references.
    """
    # Get identifier
    id_match = RE_FACTORY_ID.search(java_content)
    identifier = None
    if id_match:
        identifier = id_match.group(1)
    else:
        id_const_match = RE_FACTORY_ID_CONST.search(java_content)
        if id_const_match:
            # Normalize whitespace: multiline refs like "org...source\n.Class.FIELD"
            const_ref = re.sub(r'\s+', '', id_const_match.group(1))
            # Strip fully-qualified package prefix, keep ClassName.FIELD
            parts = const_ref.split(".")
            # Find the first capitalized part as the class name start
            short_parts = []
            for i, p in enumerate(parts):
                if p and p[0].isupper():
                    short_parts = parts[i:]
                    break
            short_ref = ".".join(short_parts) if short_parts else const_ref

            # Try resolution in priority order
            if identifier_lookup:
                # 1. Exact match on full ref
                for ref in [const_ref, short_ref]:
                    if ref in identifier_lookup:
                        identifier = identifier_lookup[ref]
                        break
                # 2. Match on last N parts (ClassName.FIELD)
                if not identifier:
                    field_name = parts[-1]
                    for n in range(min(3, len(short_parts)), 0, -1):
                        candidate = ".".join(short_parts[-n:])
                        if candidate in identifier_lookup:
                            identifier = identifier_lookup[candidate]
                            break
                # 3. Any entry ending with .FIELD_NAME
                if not identifier:
                    for key, val in identifier_lookup.items():
                        if key.endswith(f".{field_name}") and field_name not in ("IDENTIFIER",):
                            identifier = val
                            break
                # 4. For bare IDENTIFIER, search imports to find the source class
                if not identifier and field_name == "IDENTIFIER":
                    if len(short_parts) >= 2:
                        class_hint = short_parts[-2]
                        for key, val in identifier_lookup.items():
                            if key.endswith(".IDENTIFIER") and class_hint in key:
                                identifier = val
                                break
                    if not identifier:
                        # Search imports for the class that defines IDENTIFIER
                        for imp_match in re.finditer(r'import\s+([\w.]+)\.(\w+);', java_content):
                            imp_class = imp_match.group(2)
                            lookup_key = f"{imp_class}.IDENTIFIER"
                            if lookup_key in identifier_lookup:
                                identifier = identifier_lookup[lookup_key]
                                break

            # 5. Try within same file
            if not identifier:
                const_name = parts[-1]
                const_pattern = re.compile(rf'{const_name}\s*=\s*"([^"]+)"')
                const_match = const_pattern.search(java_content)
                if const_match:
                    identifier = const_match.group(1)

            if not identifier:
                identifier = parts[-1]

    if not identifier:
        return None

    # Detect type
    connector_type = "unknown"
    if RE_IMPLEMENTS_SOURCE.search(java_content):
        connector_type = "source"
    elif RE_IMPLEMENTS_SINK.search(java_content):
        connector_type = "sink"

    # Extract option rule
    rule = {"required": [], "optional": [], "exclusive": [], "conditional": []}

    # Find the optionRule method body
    rule_match = re.search(r'optionRule\s*\(\s*\)\s*\{([\s\S]*?)\n\s*\}', java_content)
    if rule_match:
        rule_body = rule_match.group(1)

        for m in RE_RULE_CALL.finditer(rule_body):
            call_type = m.group(1)
            paren_start = rule_body.index("(", m.start() + 1)
            args = _extract_balanced_args(rule_body, paren_start)
            if args is None:
                continue

            if call_type in ("required", "optional", "exclusive"):
                refs = _parse_option_refs(args)
                rule[call_type].extend(refs)
            elif call_type == "conditional":
                parts = args.split(",", 2)
                if len(parts) >= 3:
                    trigger = parts[0].strip()
                    value = parts[1].strip()
                    deps = _parse_option_refs(parts[2])
                    rule["conditional"].append({
                        "when": trigger,
                        "equals": value,
                        "then_require": deps,
                    })

    return {
        "identifier": identifier,
        "type": connector_type,
        "rule": rule,
        "file": str(file_path),
    }


def _parse_option_refs(text: str) -> list[str]:
    """Parse comma-separated option references like 'FooOptions.BAR, BazOptions.QUX'."""
    refs = []
    for part in text.split(","):
        part = part.strip()
        if part and not part.startswith("//"):
            # Extract the last segment: "FooOptions.BAR" -> "BAR"
            refs.append(part)
    return refs


def resolve_option_ref(
    ref: str,
    all_options: dict[str, dict[str, dict]],
    inheritance_map: dict[str, list[str]] | None = None,
) -> dict | None:
    """Resolve an option reference like 'JdbcSourceOptions.URL' to its definition.

    all_options: {file_stem: {FIELD_NAME: option_dict}}
    inheritance_map: {ClassName: [ParentClass, GrandParentClass, ...]}
    """
    parts = ref.split(".")
    if len(parts) >= 2:
        field_name = parts[-1].strip()
        class_hint = parts[-2].strip() if len(parts) > 1 else ""
    else:
        field_name = ref.strip()
        class_hint = ""

    # Build search order: the referenced class + its parent classes
    search_classes = [class_hint] if class_hint else []
    if class_hint and inheritance_map:
        search_classes.extend(inheritance_map.get(class_hint, []))

    # Search option files matching the class hierarchy
    for hint in search_classes:
        for file_stem, opts in all_options.items():
            if hint.lower() in file_stem.lower() and field_name in opts:
                return opts[field_name]

    # Fallback only if no class hint at all
    if not class_hint:
        for file_stem, opts in all_options.items():
            if field_name in opts:
                return opts[field_name]

    return None


def scan_docs_examples(docs_dir: Path, connector_name: str) -> list[str]:
    """Find config examples from documentation for a connector."""
    examples = []
    # Try various doc file patterns
    patterns = [
        connector_name,
        connector_name.replace("-", ""),
        connector_name.lower(),
        connector_name.replace("-", "_"),
    ]

    for p in patterns:
        for md_file in docs_dir.rglob(f"*{p}*.md"):
            try:
                content = md_file.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            # Extract hocon/conf code blocks
            for m in re.finditer(r'```(?:hocon|conf)?\n(.*?)```', content, re.DOTALL):
                block = m.group(1).strip()
                if "source" in block or "sink" in block or "{" in block:
                    if len(block) < 3000:  # skip overly large blocks
                        examples.append(block)
    return examples[:3]  # max 3 examples per connector


def build_catalog(seatunnel_root: Path, quiet: bool = False) -> dict:
    """Main catalog build function.

    Args:
        seatunnel_root: Path to SeaTunnel project root.
        quiet: If True, suppress progress output to stdout.

    Returns:
        {
            "index": [ {name, type, description_short}, ... ],  # L1: compact
            "connectors": { name: { full details } },            # L2: detailed
        }
    """
    _log = (lambda *a, **kw: None) if quiet else print

    connectors_dir = seatunnel_root / "seatunnel-connectors-v2"
    docs_dir = seatunnel_root / "docs" / "en"

    # Phase 1: Scan all *Options.java files to build option definitions
    _log("Phase 1: Scanning Option definitions...")
    all_options: dict[str, dict[str, dict]] = {}  # {file_stem: {FIELD: opt_dict}}
    option_files = list(connectors_dir.rglob("*Options.java")) + list(connectors_dir.rglob("*Config.java"))
    # Also scan common options in seatunnel-api
    api_dir = seatunnel_root / "seatunnel-api"
    if api_dir.exists():
        option_files.extend(api_dir.rglob("*Options.java"))

    for f in option_files:
        try:
            content = f.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        opts = scan_option_fields(content)
        if opts:
            if f.stem in all_options:
                existing = all_options[f.stem]
                for k, v in opts.items():
                    existing.setdefault(k, v)
            else:
                all_options[f.stem] = opts

    _log(f"  Found {sum(len(v) for v in all_options.values())} options across {len(all_options)} files")

    # Phase 1b: Build class inheritance map for Options/Config classes
    # e.g. MySqlIncrementalSourceOptions -> [JdbcSourceOptions, SourceOptions]
    _log("Phase 1b: Building Options class inheritance map...")
    re_extends = re.compile(r'class\s+(\w+)\s+extends\s+(\w+)')
    re_implements = re.compile(r'(?:class|interface)\s+(\w+)(?:\s+extends\s+\w+)?\s+implements\s+([\w\s,]+)')
    inheritance_map: dict[str, list[str]] = {}
    # parent_map: {class -> set of parents} — extends + implements
    parent_map: dict[str, set[str]] = {}
    for f in option_files:
        try:
            content = f.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        m = re_extends.search(content)
        if m:
            parent_map.setdefault(m.group(1), set()).add(m.group(2))
        m2 = re_implements.search(content)
        if m2:
            cls_name = m2.group(1)
            for iface in m2.group(2).split(","):
                iface = iface.strip()
                if iface and iface[0].isupper():
                    parent_map.setdefault(cls_name, set()).add(iface)
    for cls in parent_map:
        chain: list[str] = []
        visited = {cls}
        frontier = list(parent_map.get(cls, set()))
        while frontier:
            parent = frontier.pop(0)
            if parent in visited:
                continue
            visited.add(parent)
            chain.append(parent)
            frontier.extend(parent_map.get(parent, set()))
        if chain:
            inheritance_map[cls] = chain
    _log(f"  Found {len(inheritance_map)} inheritance chains")

    # Phase 1.5: Build identifier lookup table for cross-file constant resolution
    # Handles: static final String IDENTIFIER = "MySQL-CDC"
    #          enum values: S3("S3File")
    #          interface constants: String MYSQL = "MySQL"
    _log("Phase 1.5: Building identifier constant lookup...")
    identifier_lookup: dict[str, str] = {}

    re_class_or_enum = re.compile(r'(?:class|interface|enum)\s+(\w+)')
    re_string_const = re.compile(
        r'(?:public\s+)?(?:static\s+)?(?:final\s+)?String\s+(\w+)\s*=\s*"([^"]+)"'
    )
    re_enum_value = re.compile(
        r'(\w+)\s*\(\s*"([^"]+)"'
    )

    scan_dirs = [connectors_dir]
    api_dir = seatunnel_root / "seatunnel-api"
    if api_dir.exists():
        scan_dirs.append(api_dir)

    for scan_dir in scan_dirs:
        for f in scan_dir.rglob("*.java"):
            try:
                content = f.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            class_match = re_class_or_enum.search(content)
            if not class_match:
                continue
            class_name = class_match.group(1)

            # String constants: static final String IDENTIFIER = "value"
            for m in re_string_const.finditer(content):
                field_name, value = m.group(1), m.group(2)
                identifier_lookup[f"{class_name}.{field_name}"] = value

            # Enum constructor args: S3("S3File") -> FileSystemType.S3 = "S3File"
            if "enum " in content:
                for m in re_enum_value.finditer(content):
                    enum_name, value = m.group(1), m.group(2)
                    if enum_name[0].isupper():
                        identifier_lookup[f"{class_name}.{enum_name}"] = value
                        # Also map the .getXxx() pattern: FileSystemType.S3.getFileSystemPluginName
                        identifier_lookup[f"{class_name}.{enum_name}.getFileSystemPluginName"] = value

    _log(f"  Found {len(identifier_lookup)} string constants")

    # Phase 1.6: Resolve constant key references in option definitions
    resolved_count = 0
    for file_stem, opts in all_options.items():
        for field_name, opt in opts.items():
            const_ref = opt.pop("key_const_ref", None)
            if const_ref and "key" not in opt:
                parts = const_ref.split(".")
                ref_field = parts[-1]
                ref_class = parts[-2] if len(parts) >= 2 else ""
                resolved_key = identifier_lookup.get(const_ref)
                if not resolved_key and ref_class:
                    resolved_key = identifier_lookup.get(f"{ref_class}.{ref_field}")
                if not resolved_key:
                    for k, v in identifier_lookup.items():
                        if k.endswith(f".{ref_field}"):
                            resolved_key = v
                            break
                if resolved_key:
                    opt["key"] = resolved_key
                    resolved_count += 1
    _log(f"  Resolved {resolved_count} constant key references")

    # Phase 2: Scan all *Factory.java files to get connector identifiers and rules
    _log("Phase 2: Scanning Factory definitions...")
    factories = []
    for f in connectors_dir.rglob("*Factory.java"):
        try:
            content = f.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        factory = scan_factory(content, f, identifier_lookup)
        if factory and factory["identifier"]:
            factories.append(factory)

    _log(f"  Found {len(factories)} connector factories")

    # Phase 3: Merge factories with option details
    _log("Phase 3: Building connector catalog...")
    catalog_index = []
    catalog_details = {}

    # Group factories by identifier (same connector may have source + sink)
    from collections import defaultdict
    grouped = defaultdict(list)
    for f in factories:
        grouped[f["identifier"]].append(f)

    for identifier, factory_list in sorted(grouped.items()):
        types = list(set(f["type"] for f in factory_list))

        # Merge rules from all factories for this connector
        all_required_refs = []
        all_optional_refs = []
        all_exclusive_refs = []
        all_conditional = []
        for f in factory_list:
            r = f["rule"]
            all_required_refs.extend(r["required"])
            all_optional_refs.extend(r["optional"])
            all_exclusive_refs.extend(r["exclusive"])
            all_conditional.extend(r["conditional"])

        # Resolve option references to actual definitions
        required_opts = []
        optional_opts = []
        for ref in all_required_refs:
            opt = resolve_option_ref(ref, all_options, inheritance_map)
            if opt and "key" in opt:
                required_opts.append(opt)
            else:
                required_opts.append({"key": ref.split(".")[-1].lower(), "field_name": ref, "unresolved": True})

        for ref in all_optional_refs:
            opt = resolve_option_ref(ref, all_options, inheritance_map)
            if opt and "key" in opt:
                optional_opts.append(opt)
            else:
                optional_opts.append({"key": ref.split(".")[-1].lower(), "field_name": ref, "unresolved": True})

        # Deduplicate by key
        seen_keys = set()
        dedup_required = []
        for o in required_opts:
            if o["key"] not in seen_keys:
                seen_keys.add(o["key"])
                dedup_required.append(o)
        dedup_optional = []
        for o in optional_opts:
            if o["key"] not in seen_keys:
                seen_keys.add(o["key"])
                dedup_optional.append(o)

        # Build description from required params
        req_keys = [o["key"] for o in dedup_required]
        opt_keys = [o["key"] for o in dedup_optional]

        # Get first description from any option
        desc_parts = []
        for o in dedup_required + dedup_optional:
            if o.get("description"):
                desc_parts.append(o["description"])
                break

        # Scan for doc examples
        examples = scan_docs_examples(docs_dir, identifier)

        # Build detail entry
        detail = {
            "name": identifier,
            "types": types,
            "required": [_opt_to_dict(o) for o in dedup_required],
            "optional": [_opt_to_dict(o) for o in dedup_optional],
            "exclusive": [ref.split(".")[-1] for ref in all_exclusive_refs],
            "conditional": [
                {
                    "when": c["when"].split(".")[-1],
                    "equals": c["equals"].split(".")[-1],
                    "then_require": [r.split(".")[-1] for r in c["then_require"]],
                }
                for c in all_conditional
            ],
            "examples": examples,
            "factory_files": [f["file"] for f in factory_list],
        }
        catalog_details[identifier] = detail

        # Build index entry (compact, for system prompt)
        catalog_index.append({
            "name": identifier,
            "types": types,
            "required_keys": req_keys[:10],
            "optional_count": len(opt_keys),
        })

    _log(f"  Built catalog: {len(catalog_index)} connectors")
    return {"index": catalog_index, "connectors": catalog_details}


def _opt_to_dict(opt: dict) -> dict:
    """Convert an option to a clean dict for JSON output."""
    result = {"key": opt["key"]}
    if opt.get("value_type"):
        result["type"] = opt["value_type"]
    if opt.get("default") is not None:
        result["default"] = opt["default"]
    if opt.get("description"):
        result["description"] = opt["description"][:200]  # truncate long descriptions
    if opt.get("fallback_keys"):
        result["fallback_keys"] = opt["fallback_keys"]
    if opt.get("unresolved"):
        result["unresolved"] = True
    return result


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Build SeaTunnel connector catalog from source")
    parser.add_argument("--seatunnel-root", type=Path, help="Path to seatunnel project root")
    parser.add_argument("--output", type=Path, help="Output JSON file path")
    args = parser.parse_args()

    root = args.seatunnel_root or find_seatunnel_root()
    print(f"SeaTunnel root: {root}")

    catalog = build_catalog(root)

    output = args.output or Path(__file__).parent / "connector_catalog.json"
    with open(output, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)

    print(f"\nCatalog written to: {output}")
    print(f"  Index entries: {len(catalog['index'])}")
    print(f"  Detail entries: {len(catalog['connectors'])}")

    # Print summary
    total_opts = sum(
        len(c["required"]) + len(c["optional"])
        for c in catalog["connectors"].values()
    )
    print(f"  Total options: {total_opts}")


if __name__ == "__main__":
    main()
