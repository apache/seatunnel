#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# !/usr/bin/python
"""
Comprehensive connector registration completeness check.

Verifies that every new connector module is registered in ALL required configuration
files. A connector that misses even one of these registrations will be silently broken:
not distributed, not discoverable by name, and not installable via install-plugin.sh.

Registration points checked (all are ERRORS):
  1. seatunnel-connectors-v2/pom.xml              -- Maven module (required for compile/build)
  2. seatunnel-dist/pom.xml  <scope>provided>     -- Dist packaging (connectors/ directory)
  3. plugin-mapping.properties                    -- Runtime connector-name -> jar resolution
  4. config/plugin_config                         -- install-plugin.sh downloadable list

Modes of operation:
  --base-ref <ref>   CI diff mode: check only connectors newly added against <ref>.
                     Use this in CI via GITHUB_BASE_REF. Avoids false positives from
                     pre-existing historical omissions in the repository.
  (no arg)           Static mode: check all connector modules. Some checks may report
                     pre-existing omissions that are known and intentional.

Usage:
    python tools/check_connector_registration.py
    python tools/check_connector_registration.py --base-ref origin/dev
    python tools/check_connector_registration.py --base-ref upstream/dev

Exit codes:
    0 - All checked connectors pass all critical registration checks
    1 - One or more critical registrations are missing
"""

import argparse
import os
import re
import subprocess
import sys

# ---------------------------------------------------------------------------
# Exclusion sets: modules that are intentionally NOT standalone connectors
# and therefore do NOT need all registration entries.
# ---------------------------------------------------------------------------

# Base/utility modules that are never deployed as standalone connectors.
# These lack factory implementations and are dependencies of real connectors.
EXCLUDED_FROM_ALL = {
    "connector-common",
    "connector-file-base",
    "connector-file-base-hadoop",
    "connector-cdc-base",
}

# Connectors bundled into the seatunnel-dist core jar (always available,
# no separate install-plugin.sh download needed).
# These appear in seatunnel-dist/pom.xml but not in config/plugin_config.
BUNDLED_IN_DIST = {
    "connector-console",
    "connector-fake",
    "connector-assert",
    "connector-socket",
}


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def get_artifact_id(pom_path):
    """
    Extract the project-level artifactId from a pom.xml file.
    Strips the <parent> block first to avoid matching the parent artifactId.
    """
    with open(pom_path, "r", encoding="utf-8") as f:
        content = f.read()
    # Remove <parent> block so we only match the project-level artifactId
    content_no_parent = re.sub(r"<parent>.*?</parent>", "", content, flags=re.DOTALL)
    m = re.search(r"<artifactId>\s*([^\s<]+)\s*</artifactId>", content_no_parent)
    return m.group(1).strip() if m else None


def find_all_connector_modules(connectors_dir):
    """
    Walk seatunnel-connectors-v2/ and return a dict of {artifactId -> abs_dir_path}
    for every connector module that has both a pom.xml and a src/ directory.
    Excludes modules in EXCLUDED_FROM_ALL.
    """
    modules = {}
    for root, dirs, files in os.walk(connectors_dir):
        # Avoid descending into build artifacts
        dirs[:] = [d for d in dirs if d != "target"]
        if "pom.xml" not in files:
            continue
        if not os.path.isdir(os.path.join(root, "src")):
            continue
        artifact_id = get_artifact_id(os.path.join(root, "pom.xml"))
        if (
            artifact_id
            and artifact_id.startswith("connector-")
            and artifact_id not in EXCLUDED_FROM_ALL
        ):
            modules[artifact_id] = root
    return modules


def get_new_connector_modules_from_diff(repo_root, base_ref, connectors_dir):
    """
    Return {artifactId -> abs_dir_path} for connector modules whose pom.xml was
    ADDED (not modified) in the diff between base_ref and HEAD.
    Only top-level connector directories (depth=1 under seatunnel-connectors-v2/)
    are considered as "new" connectors; subdirectory additions within an existing
    connector are not treated as new standalone connectors.
    """
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "--diff-filter=A", f"{base_ref}...HEAD"],
            capture_output=True,
            text=True,
            cwd=repo_root,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(
            f"WARNING: git diff failed (base_ref={base_ref}): {e}. "
            "Falling back to static full scan."
        )
        return None

    new_modules = {}
    # Match exactly: seatunnel-connectors-v2/connector-<name>/pom.xml (no deeper path)
    pattern = re.compile(
        r"seatunnel-connectors-v2/(connector-[^/]+)/pom\.xml$"
    )
    for line in result.stdout.strip().splitlines():
        m = pattern.match(line)
        if not m:
            continue
        connector_dir = m.group(1)
        abs_pom = os.path.join(repo_root, "seatunnel-connectors-v2", connector_dir, "pom.xml")
        if not os.path.isfile(abs_pom):
            continue
        artifact_id = get_artifact_id(abs_pom)
        if (
            artifact_id
            and artifact_id.startswith("connector-")
            and artifact_id not in EXCLUDED_FROM_ALL
        ):
            new_modules[artifact_id] = os.path.join(
                repo_root, "seatunnel-connectors-v2", connector_dir
            )
    return new_modules


# ---------------------------------------------------------------------------
# Per-registration-point checkers
# ---------------------------------------------------------------------------


def is_module_declared_in_parent_pom(connector_abs_dir):
    """
    Return True if the connector's parent directory has a pom.xml that declares
    this connector as a <module>.  This handles both top-level connectors
    (parent = seatunnel-connectors-v2/) and nested sub-connectors
    (parent = seatunnel-connectors-v2/connector-cdc/, etc.).
    """
    parent_dir = os.path.dirname(connector_abs_dir)
    parent_pom = os.path.join(parent_dir, "pom.xml")
    if not os.path.isfile(parent_pom):
        return False
    connector_dir_name = os.path.basename(connector_abs_dir)
    with open(parent_pom, "r", encoding="utf-8") as f:
        content = f.read()
    # Match <module>connector-dir-name</module> (exact name, may have whitespace)
    return bool(
        re.search(
            r"<module>\s*" + re.escape(connector_dir_name) + r"\s*</module>",
            content,
        )
    )


def build_dist_registered(dist_pom_path):
    """
    Return set of artifactIds with scope=provided in seatunnel-dist/pom.xml.
    The assembly-bin-ci.xml collects all scope=provided connector-* dependencies
    from this pom into the connectors/ directory of the dist package.
    """
    with open(dist_pom_path, "r", encoding="utf-8") as f:
        content = f.read()
    # Find all <dependency> blocks that contain scope=provided and a connector- artifactId
    registered = set()
    for dep_block in re.findall(r"<dependency>.*?</dependency>", content, re.DOTALL):
        if "<scope>provided</scope>" not in dep_block:
            continue
        m = re.search(r"<artifactId>\s*(connector-[^\s<]+)\s*</artifactId>", dep_block)
        if m:
            registered.add(m.group(1).strip())
    return registered


def build_plugin_mapping_jars(plugin_mapping_path):
    """
    Return set of jar names (connector-*) that appear as VALUES in plugin-mapping.properties.
    e.g. 'seatunnel.source.Kafka = connector-kafka'  → 'connector-kafka'
    """
    with open(plugin_mapping_path, "r", encoding="utf-8") as f:
        content = f.read()
    return set(re.findall(r"=\s*(connector-[^\s#\n]+)", content))


def build_plugin_config_set(plugin_config_path):
    """Return set of connector names listed in config/plugin_config."""
    names = set()
    with open(plugin_config_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("connector-"):
                names.add(line)
    return names


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check connector registration completeness."
    )
    parser.add_argument(
        "--base-ref",
        metavar="REF",
        default=os.environ.get("GITHUB_BASE_REF", ""),
        help=(
            "Git ref to diff against (e.g. 'origin/dev'). "
            "When set, only newly added connectors are checked. "
            "Defaults to GITHUB_BASE_REF env var."
        ),
    )
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    connectors_dir = os.path.join(repo_root, "seatunnel-connectors-v2")
    dist_pom = os.path.join(repo_root, "seatunnel-dist", "pom.xml")
    plugin_mapping = os.path.join(repo_root, "plugin-mapping.properties")
    plugin_config = os.path.join(repo_root, "config", "plugin_config")

    # --- Validate required files exist ---
    for path, label in [
        (connectors_dir, "seatunnel-connectors-v2/"),
        (dist_pom, "seatunnel-dist/pom.xml"),
        (plugin_mapping, "plugin-mapping.properties"),
        (plugin_config, "config/plugin_config"),
    ]:
        if not os.path.exists(path):
            print(f"ERROR: Required file not found: {path}")
            sys.exit(1)

    # --- Determine which connectors to check ---
    base_ref = args.base_ref.strip()
    diff_mode = False

    if base_ref:
        # CI diff mode: only check connectors newly added in this PR
        # Prefix with 'origin/' if it looks like a plain branch name and not already prefixed
        if "/" not in base_ref:
            base_ref = "origin/" + base_ref
        print(f"CI diff mode: checking connectors newly added against {base_ref}")
        connectors_to_check = get_new_connector_modules_from_diff(
            repo_root, base_ref, connectors_dir
        )
        if connectors_to_check is None:
            # git diff failed; fall back to static mode
            connectors_to_check = find_all_connector_modules(connectors_dir)
        elif not connectors_to_check:
            print("No new connector modules detected in this diff. Nothing to check.")
            sys.exit(0)
        else:
            diff_mode = True
            print(
                f"Found {len(connectors_to_check)} new connector(s): "
                + ", ".join(sorted(connectors_to_check))
            )
    else:
        # Static mode: check all connectors with source code
        print("Static mode: checking all connector modules with source code.")
        connectors_to_check = find_all_connector_modules(connectors_dir)

    print(f"Total connectors to check: {len(connectors_to_check)}")
    print()

    # --- Load registration data from all files ---
    dist_registered = build_dist_registered(dist_pom)
    pm_jars = build_plugin_mapping_jars(plugin_mapping)
    pc_names = build_plugin_config_set(plugin_config)

    # --- Check each connector ---
    errors = {}   # {connector: [missing_locations]}

    for artifact_id, connector_dir in sorted(connectors_to_check.items()):
        connector_errors = []

        # Check 1: parent pom.xml module declaration
        # Each connector must be declared as <module> in its parent directory's pom.xml.
        # Connectors discovered via os.walk but missing from the pom hierarchy
        # will be silently ignored by Maven (not compiled, not packaged).
        if not is_module_declared_in_parent_pom(connector_dir):
            connector_errors.append(
                "parent pom.xml (missing <module> entry; "
                "Maven will not compile this connector)"
            )

        # Check 2: seatunnel-dist/pom.xml scope=provided dependency
        # This controls whether the connector jar ends up in the dist connectors/ dir.
        if artifact_id not in dist_registered:
            connector_errors.append(
                "seatunnel-dist/pom.xml (missing <scope>provided</scope> dependency; "
                "connector jar will NOT be included in the distribution package)"
            )

        # Check 3: plugin-mapping.properties name->jar mapping
        # Required for users to reference the connector by PluginName in job configs.
        if artifact_id not in pm_jars:
            connector_errors.append(
                "plugin-mapping.properties (missing name→jar mapping; "
                "users cannot reference this connector by name in job configs)"
            )

        # Check 4: config/plugin_config install list
        # Required for install-plugin.sh to download the connector.
        # Skip connectors that are bundled into the dist core (always available).
        if artifact_id not in BUNDLED_IN_DIST and artifact_id not in pc_names:
            connector_errors.append(
                "config/plugin_config (missing entry; "
                "install-plugin.sh cannot download this connector)"
            )

        if connector_errors:
            errors[artifact_id] = connector_errors

    # --- Print results ---
    if not errors:
        mode_desc = "newly added" if diff_mode else "all"
        print(
            f"OK: All {len(connectors_to_check)} {mode_desc} connector module(s) "
            "pass all critical registration checks."
        )
        sys.exit(0)

    print("ERRORS: The following connectors are missing critical registrations.")
    print(
        "Each missing registration silently breaks a specific aspect of the connector."
    )
    print()

    for connector, locs in sorted(errors.items()):
        print(f"  {connector}:")
        for loc in locs:
            print(f"    ERROR: missing from {loc}")
        print()

    print("-" * 70)
    print(f"Total connectors with registration errors: {len(errors)}")
    print()
    print("Fix guide:")
    print(
        "  1. seatunnel-connectors-v2/pom.xml: add <module>CONNECTOR_NAME</module>"
    )
    print(
        "  2. seatunnel-dist/pom.xml: add <dependency> with <scope>provided</scope>"
    )
    print(
        "  3. plugin-mapping.properties: add 'seatunnel.source.NAME = CONNECTOR_NAME'"
    )
    print(
        "     and/or 'seatunnel.sink.NAME = CONNECTOR_NAME'"
    )
    print(
        "  4. config/plugin_config: add a line with CONNECTOR_NAME under --connectors-v2--"
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
