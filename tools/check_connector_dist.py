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
Check that all connector modules with source files are registered in seatunnel-dist/pom.xml.

When a new connector is added under seatunnel-connectors-v2/, it must also be declared as a
<scope>provided</scope> dependency in seatunnel-dist/pom.xml so that the maven-assembly-plugin
includes its jar in the official distribution package's connectors/ directory.

This script detects connectors that exist in seatunnel-connectors-v2/ (i.e., they have both a
pom.xml and a src/ directory) but are not declared in seatunnel-dist/pom.xml, preventing the
silent omission that caused connector-typesense to be missing from the dist for months.

Usage:
    python tools/check_connector_dist.py

Exit codes:
    0 - All connector modules are registered in seatunnel-dist/pom.xml
    1 - One or more connector modules are missing from seatunnel-dist/pom.xml
"""

import os
import re
import sys

# Connector modules explicitly excluded from the distribution assembly.
# These are base/common modules without standalone factory registrations.
# See: seatunnel-dist/src/main/assembly/assembly-bin-ci.xml <excludes> section.
EXCLUDED_FROM_DIST = {
    "connector-common",
    "connector-file-base",
    "connector-file-base-hadoop",
}


def get_artifact_id(pom_path):
    """
    Extract the project-level artifactId from a pom.xml file.
    Strips the <parent> block first to avoid matching the parent's artifactId.
    """
    with open(pom_path, "r", encoding="utf-8") as f:
        content = f.read()
    # Remove the <parent> block so we only match the project-level artifactId
    content_no_parent = re.sub(r"<parent>.*?</parent>", "", content, flags=re.DOTALL)
    m = re.search(r"<artifactId>\s*([^\s<]+)\s*</artifactId>", content_no_parent)
    if m:
        return m.group(1).strip()
    return None


def find_connector_modules(connectors_dir):
    """
    Walk seatunnel-connectors-v2/ and return the artifactIds of all connector
    modules that have actual source code (i.e., both pom.xml and a src/ directory).
    Parent aggregator poms (which only contain <modules> and no src/) are excluded
    automatically because they lack a src/ directory.
    """
    connector_modules = []
    for root, _dirs, files in os.walk(connectors_dir):
        if "pom.xml" not in files:
            continue
        # Only treat directories that have a src/ as real connector modules
        if not os.path.isdir(os.path.join(root, "src")):
            continue
        artifact_id = get_artifact_id(os.path.join(root, "pom.xml"))
        if artifact_id and artifact_id.startswith("connector-"):
            connector_modules.append(artifact_id)
    return connector_modules


def get_dist_registered_connectors(dist_pom_path):
    """
    Parse all <artifactId> values matching connector-* from seatunnel-dist/pom.xml.
    Returns a set of artifactId strings.
    """
    with open(dist_pom_path, "r", encoding="utf-8") as f:
        content = f.read()
    return set(re.findall(r"<artifactId>(connector-[^<]+)</artifactId>", content))


def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    connectors_dir = os.path.join(repo_root, "seatunnel-connectors-v2")
    dist_pom = os.path.join(repo_root, "seatunnel-dist", "pom.xml")

    if not os.path.isdir(connectors_dir):
        print("ERROR: seatunnel-connectors-v2 directory not found: " + connectors_dir)
        sys.exit(1)

    if not os.path.isfile(dist_pom):
        print("ERROR: seatunnel-dist/pom.xml not found: " + dist_pom)
        sys.exit(1)

    all_modules = find_connector_modules(connectors_dir)
    # Filter out modules that are intentionally excluded from the dist assembly
    to_check = [m for m in all_modules if m not in EXCLUDED_FROM_DIST]
    registered = get_dist_registered_connectors(dist_pom)
    missing = sorted([m for m in to_check if m not in registered])

    if missing:
        print(
            "ERROR: The following connector modules have source files but are NOT registered"
        )
        print(
            "       in seatunnel-dist/pom.xml. Without this registration the connector jar"
        )
        print(
            "       will not be included in the official distribution package (connectors/ dir)."
        )
        print()
        for m in missing:
            print("  - " + m)
        print()
        print(
            "Fix: Add each missing connector as a <dependency> with <scope>provided</scope>"
        )
        print(
            "     in seatunnel-dist/pom.xml following the pattern of existing connectors."
        )
        print(
            "     Example:"
        )
        print("       <dependency>")
        print("           <groupId>org.apache.seatunnel</groupId>")
        print("           <artifactId>" + missing[0] + "</artifactId>")
        print("           <version>${project.version}</version>")
        print("           <scope>provided</scope>")
        print("       </dependency>")
        sys.exit(1)
    else:
        print(
            "OK: All "
            + str(len(to_check))
            + " connector modules are registered in seatunnel-dist/pom.xml."
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
