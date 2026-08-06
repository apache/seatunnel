#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Assert that a shaded jar carries no references to the ORIGINAL com.fasterxml.jackson
package, i.e. that its Jackson relocation actually reached every class.

Usage: check_shaded_jackson_refs.py <shaded.jar> [<shaded.jar> ...]

Why this exists
---------------
seatunnel-hadoop-aws bundles no Jackson classes of its own. It only *references* them,
and it reaches the implementation through hadoop-common, which the seatunnel-hadoop3-*-uber
jar supplies - with Jackson relocated. So the uber's

    org.apache.hadoop.util.JsonSerialization#getMapper

is declared to return the RELOCATED ObjectMapper. If hadoop-aws is shaded without the
matching relocation, RoleModel's call site keeps the original descriptor

    invokevirtual JsonSerialization.getMapper:()Lcom/fasterxml/jackson/databind/ObjectMapper;

which resolves against nothing at runtime: every fs.s3a.assumed.role.* path dies with
NoSuchMethodError. That is a link-time descriptor mismatch, so it is invisible at build
time and invisible to any test that does not exercise assumed-role - which is why it
survived undetected across Hadoop versions.

A jar-level check catches it deterministically, in seconds, with no container and no AWS
credentials, and it keeps catching it after future Hadoop upgrades drag new Jackson
references into the jar.

Two distinct failure modes, both fixed by the same relocation and only one of them loud:

  * RoleModel keeps the original descriptor on its getMapper() call - NoSuchMethodError.
    This is what hadoop-aws 3.1.4 exhibits (RoleModel plus two nested types).
  * On newer hadoop-aws (checked against 3.4.3), SuccessData / PendingSet /
    SinglePendingCommit additionally carry unrelocated @JsonProperty annotations. The
    relocated Jackson looks for relocated annotations, does not find them, and silently
    falls back to default property naming and inclusion when writing the S3A committer's
    _SUCCESS and .pending files - no exception, just wrong output. That second mode is
    the reason to keep this check after the next Hadoop upgrade, not only for the
    version in tree today.

What it checks
--------------
Every CONSTANT_Utf8 entry of every class under org/apache/hadoop/, for the byte pattern
"com/fasterxml/jackson" (and its dotted form). That covers class references, method and
field descriptors, generic signatures, annotations, and string constants alike - a
targeted javap of one known class would not.

Scope note: only org/apache/hadoop/ classes are checked, deliberately. The invariant
being defended is that the Hadoop classes in this jar agree with the Hadoop classes in
the uber jar. The AWS SDK ships its own vendored Jackson under com.amazonaws.thirdparty
/ software.amazon.awssdk.thirdparty and resolves those references among its own classes;
whether that vendoring has leaks of its own is not this jar's contract, and flagging it
here would make the check permanently red for a reason nobody can act on.

Exit 0 if clean, 1 if any original-package reference remains.
"""
import os
import re
import sys
import zipfile

ORIGINAL = (b"com/fasterxml/jackson", b"com.fasterxml.jackson")
SCOPE = "org/apache/hadoop/"
# Mirrors ${seatunnel.shade.package} in the root pom. Kept as the full prefix rather
# than the bare word "shade": matching one common word would stop recognising correct
# relocations the moment that property is renamed, and the check would then fail every
# build for a reason with no obvious hint in the code.
SHADED_PREFIX = rb"org[/.]apache[/.]seatunnel[/.]shade"
# The relocated form legitimately contains the original substring as a suffix
# (org/apache/seatunnel/shade/hadoop/com/fasterxml/jackson/...), so a naive search
# would report every correctly-relocated reference as a violation.
RELOCATED = re.compile(SHADED_PREFIX + rb"[A-Za-z0-9_$/.]*?(?:com[/.]fasterxml[/.]jackson)")


def offending_refs(class_bytes):
    """Return original-package references, ignoring ones that are part of a relocated name."""
    masked = RELOCATED.sub(b"#", class_bytes)
    return [m for m in ORIGINAL if m in masked]


def check(path):
    bad = []
    scanned = 0
    with zipfile.ZipFile(path) as z:
        for name in z.namelist():
            if not name.endswith(".class") or not name.startswith(SCOPE):
                continue
            scanned += 1
            data = z.read(name)
            if offending_refs(data):
                bad.append(name)
    return scanned, bad


def main(paths):
    failed = False
    for path in paths:
        print(f"{path}")
        if not os.path.isfile(path):
            # Most likely the jar was never built, or something cleaned it away between
            # the build and this check - note that tools/dependencies/checkLicense.sh
            # runs `mvnw clean`, so this check has to come before it in CI.
            failed = True
            print("  FAIL: no such file. The shaded jar must exist before this runs;")
            print("  build it with `./mvnw -pl seatunnel-shade/seatunnel-hadoop-aws -am")
            print("  -DskipTests package`, and make sure nothing has run `mvn clean` in")
            print("  between (tools/dependencies/checkLicense.sh does).")
            continue
        try:
            scanned, bad = check(path)
        except zipfile.BadZipFile as e:
            failed = True
            print(f"  FAIL: not a readable jar/zip ({e}).")
            continue
        if scanned == 0:
            # Without this the check would pass vacuously on a jar whose layout changed,
            # which is exactly when it most needs to speak up.
            failed = True
            print(f"  FAIL: no classes under {SCOPE} were found in this jar - nothing was")
            print("  actually checked. Either the wrong artifact was passed, or the jar's")
            print("  layout changed and SCOPE needs updating.")
            continue
        if bad:
            failed = True
            print(f"  FAIL: {len(bad)} of {scanned} class(es) under {SCOPE} still reference "
                  f"the original com.fasterxml.jackson package:")
            for name in sorted(bad)[:20]:
                print(f"    - {name}")
            if len(bad) > 20:
                print(f"    ... and {len(bad) - 20} more")
            print("  The Jackson relocation in this module's maven-shade-plugin config must")
            print("  use the SAME shadedPattern as seatunnel-hadoop3-*-uber, since that jar")
            print("  supplies the classes these references resolve against.")
        else:
            print(f"  PASS: {scanned} class(es) under {SCOPE}, none referencing the original "
                  f"com.fasterxml.jackson package")
    return 1 if failed else 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1:]))
