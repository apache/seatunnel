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
Risk triage labels for pull requests.

Every PR gets exactly one label `risk: <1..5>-<name>` and one structured comment that shows how the
level was computed, so committers can process the review queue sorted by risk. Nothing here approves,
blocks or merges a PR: merge permission stays with committers.

How the level is computed (see compute_level):

  level = base                         when the base is 5 (platform code, fixed)
  level = min(4, base + adjustments)   otherwise

  base (who is affected if the PR is wrong)
    1  nothing at runtime: docs, examples, labeler metadata, tests
    2  a brand-new connector, or one existing connector unit, or a registration-only change
    3  an unclassified path, or tooling around the runtime (CLI, benchmarks, examples, deploy)
    4  a shared connector module (connector-common, *-base, JDBC core, fake/assert/console),
       edge agent, build wrapper and repository settings
    5  platform: engine, api, core, common, config, formats, transforms, translation, dist, build,
       CI, tools, license files, root or non-connector pom.xml

  adjustments (each rule counts once per PR, computed from the whole PR, not from single lines)
    +2  option key removed, or default/type changed (a key that only moved between files is neutral)
    +2  existing checkpoint/split/state/commit-info class or serialVersionUID modified
    +2  new supply chain: an artifact or (artifact, version) pair no pom of the base branch uses,
        repositories/profiles/antrun/exec in a pom
    +2  existing tests weakened (assertion removed, test disabled, timeout loosened, test file deleted)
    +2  registration entry removed (module, dist dependency, plugin mapping, service, plugin_config)
    +2  diff cannot be inspected (binary or oversized), or text addressed to a reviewer/AI
    +1  security/concurrency/classloading words in modified existing code
    +1  production code larger than 250 lines (1500 for a brand-new connector)
    +1  production code changed without any added test method
    +1  several connector units, or production file deleted/renamed
    +1  new connector without docs, or new option without docs
    +1  pom dependency change that reuses already-known artifacts
    +1  author is not an owner/member/collaborator/previous contributor
  A PR to a branch other than the default branch gets a floor of 3.

Keyword findings that are only suggestive (hot-path readers/writers, type mapping, empty catch,
removed throw, credentials in a brand-new connector) are notes in the comment and never change the
level.

SECURITY MODEL: the script only reads the PR through the GitHub API. It never checks out or executes
PR code, so it is safe under `pull_request_target`. Run it from the base branch.

Sub-commands:
  evaluate --pr N [--apply]   Print the comment for PR N. With --apply, also create the five labels
                              if missing, upsert the comment and replace the PR's risk label.
  classify --files-json F     Offline evaluation of a saved `pulls/N/files` payload.

Configuration (env):
  GITHUB_TOKEN, GITHUB_REPOSITORY
  PR_RISK_DEFAULT_BASE   branch that gets no floor (default: dev)
  PR_RISK_REPO_ROOT      checkout of the base branch used to learn the already-used artifacts
                         (default: current directory)
"""

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

MARKER = "pr-risk"
LABEL_PREFIX = "risk: "
LEVEL_NAMES = {1: "trivial", 2: "low", 3: "medium", 4: "high", 5: "critical"}
LEVEL_META = {
    1: ("0e8a16", "No runtime effect: docs, examples, metadata, tests"),
    2: ("7bc043", "Contained: new connector or one existing connector unit"),
    3: ("fbca04", "Needs a focused read: sizeable change or missing evidence"),
    4: ("f9841c", "Touches a contract, shared module, supply chain or several units"),
    5: ("d73a4a", "Platform, build, CI or license files"),
}

# Author associations that raise no trust adjustment.
TRUSTED_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR", "CONTRIBUTOR"}

# ------------------------------------------------------------------------------------------
# Path policy
# ------------------------------------------------------------------------------------------

# Platform and tooling paths: (regex, reason, base level). 5 means a mistake affects every job or the
# build itself; lower levels are tooling around the runtime (CLI, benchmarks, examples, Helm, wrappers).
PLATFORM_BLOCKS = [
    (r"(^|/)pom\.xml$", "root or non-connector pom.xml (build and dependency graph)", 5),
    (r"^\.github/", "CI and repository automation", 5),
    (r"^tools/", "CI, release and compatibility tooling", 5),
    (r"^seatunnel-engine/", "engine runtime (Zeta): scheduling, checkpoint, state, RPC", 5),
    (r"^seatunnel-api/", "public API / SPI contract", 5),
    (r"^seatunnel-core/", "core runtime and CLI wiring", 5),
    (r"^seatunnel-common/", "shared common code used by every module", 5),
    (r"^seatunnel-config/", "config framework", 5),
    (r"^seatunnel-formats/", "shared format code", 5),
    (r"^seatunnel-transforms-v2/", "transform runtime", 5),
    (r"^seatunnel-translation/", "Flink/Spark translation layer", 5),
    (r"^seatunnel-plugin-discovery/", "plugin discovery", 5),
    (r"^seatunnel-dist/", "distribution packaging", 5),
    (r"^seatunnel-ci-tools/", "CI tooling module", 5),
    (r"^seatunnel-(edge-agent|trace)/", "edge agent and tracing runtime", 4),
    (r"^seatunnel-(cli|benchmarks|examples)/", "CLI, benchmarks and examples (tooling around the runtime)", 3),
    (r"^deploy/", "deployment charts and manifests", 3),
    (r"^(mvnw|\.mvn/|\.asf\.yaml|CODEOWNERS)", "build wrapper and repository settings", 4),
    (r"^(plugin-mapping\.properties|config/|bin/|plugins/|LICENSE|NOTICE|licenses/)",
     "distribution, startup scripts or license files", 5),
    (r"META-INF/services/", "SPI service registration outside connectors", 5),
    (r"(^|/)CODEOWNERS$", "code ownership", 4),
]
# Test suites of platform modules: test-only changes cannot alter runtime behavior.
PLATFORM_TEST_RE = re.compile(
    r"^seatunnel-e2e/(seatunnel-engine-e2e|seatunnel-core-e2e|seatunnel-transforms-v2-e2e|seatunnel-edge-agent-e2e)/.*"
    r"|^seatunnel-[a-z0-9-]+(/[^/]+)*/src/test/.*"
)

# Registration files a connector change touches; they are metadata unless an entry is removed.
REGISTRATION_FILES = {
    "seatunnel-connectors-v2/pom.xml": "module",
    "seatunnel-e2e/seatunnel-connector-v2-e2e/pom.xml": "module",
    "seatunnel-dist/pom.xml": "dist",
    "plugin-mapping.properties": "mapping",
    "config/plugin_config": "plugin_config",
}
METADATA_FILES = {".github/workflows/labeler/label-scope-conf.yml"}
EXAMPLE_RE = re.compile(r"^seatunnel-examples/.*/src/main/resources/examples/[^/]+\.conf$")
DOCS_CONNECTOR_RE = re.compile(r"^docs/en/(connectors|connector-v2)/")
DOC_RE = re.compile(r"(\.md$)|(^docs/.*\.(png|jpe?g|gif)$)")
# pom of a connector module or of its E2E module; a freshly added one defines a new module.
MODULE_POM_RE = re.compile(
    r"^seatunnel-connectors-v2/(connector-[^/]+/)?connector-[^/]+/pom\.xml$"
    r"|^seatunnel-e2e/seatunnel-connector-v2-e2e/connector-[^/]+-e2e/pom\.xml$"
)
SERVICES_RE = re.compile(r"^seatunnel-connectors-v2/.*/src/main/resources/META-INF/services/[^/]+$")
MODULE_LINE_RE = re.compile(r"<module>")
ARTIFACT_RE = re.compile(r"<artifactId>\s*([^<\s]+)\s*</artifactId>")
PAIR_RE = re.compile(r"<artifactId>\s*([^<\s]+)\s*</artifactId>\s*<version>\s*([^<\s$][^<\s]*)\s*</version>")
# Anything in a pom that can execute code at build time or pull from another registry.
POM_DANGER_RE = re.compile(r"<repositor|<pluginRepositor|<profile|antrun|exec-maven|<systemPath|<scope>system")

# Connector modules shared by many other modules or used as fixtures across E2E suites.
SHARED_CONNECTOR_MODULES = {"connector-common", "connector-fake", "connector-assert", "connector-console"}
SHARED_MODULE_SUFFIX = re.compile(r"-(base|base-hadoop)$")
CONNECTORS_ROOT = "seatunnel-connectors-v2/"
E2E_ROOT = "seatunnel-e2e/seatunnel-connector-v2-e2e/"
JDBC_DIALECT_RE = re.compile(r"/jdbc/(?:internal/dialect|catalog)/([^/]+)/[^/]+")
JDBC_SHARED_DIRS = {"dialectenum", "utils"}

# ------------------------------------------------------------------------------------------
# Content facts (applied to changed lines only)
# ------------------------------------------------------------------------------------------

# Classes whose objects are persisted by checkpoints or savepoints; modifying them can break restore.
PERSISTED_FILE_RE = re.compile(
    r"(Split|State|CommitInfo|Checkpoint)\.java$|(SplitSerializer|StateSerializer|CommitInfoSerializer)\.java$|/state/"
)
SERIAL_RE = re.compile(r"serialVersionUID")
OPTION_FILE_RE = re.compile(r"(Option|Options|Config|Configs|Factory|Rule)[A-Za-z0-9]*\.java$")
OPTION_KEY_RE = re.compile(r"Options\.key\(\"([^\"]+)\"\)")
OPTION_TOKEN_RE = re.compile(r"defaultValue\([^)]*\)|noDefaultValue\(\)|\.\w+Type\([^)]*\)")
CONCURRENCY_RE = re.compile(
    r"\b(synchronized|volatile|ReentrantLock|ReadWriteLock|StampedLock|Semaphore|CountDownLatch|ExecutorService|"
    r"ScheduledExecutor\w*|Executors|ThreadPool\w*|CompletableFuture|BlockingQueue|ConcurrentHashMap|"
    r"Atomic(Reference|Long|Integer|Boolean)|Thread|ThreadLocal)\b|\.wait\(|notifyAll"
)
SECURITY_RE = re.compile(
    r"(?i)password|passwd|secret|credential|keytab|kerberos|truststore|keystore|jaas|sasl|token|principal"
    r"|(?<![a-z])(ssl|tls)|authenticat|authoriz"
)
CLASSLOAD_RE = re.compile(
    r"Class\.forName|ClassLoader|ServiceLoader|setAccessible|\.getDeclared\w+\(|java\.lang\.reflect|MethodHandles"
    r"|Proxy\.newProxyInstance|System\.(exit|setProperty|getenv)|Runtime\.getRuntime|ProcessBuilder"
)
# Process-level calls are worth a note even inside brand-new connector code.
PROCESS_RE = re.compile(r"System\.exit|ProcessBuilder|Runtime\.getRuntime|setAccessible")
# Lines that carry detection power. `fail(...)` in a catch block is deliberately not counted: polling helpers
# such as Awaitility fail on timeout by themselves, so dropping it is a rewrite, not a weakening.
ASSERT_RE = re.compile(
    r"\b(assert\w*|verify)\b"
    r"|@Test\b|@TestTemplate\b|@ParameterizedTest\b|@TestFactory\b"
)
# Option declarations and constants only name a credential option; they do not handle credentials.
DECLARATION_RE = re.compile(
    r"\.(required|optional|exclusive|conditional|bundled)\(|Options\.key\(|^\s*(public |private |protected )?static final"
)
DISABLE_RE = re.compile(r"@Disabled|@Ignore|@DisabledOnContainer|@DisabledOnOs|@DisabledIf|Assumptions\.|assumeTrue|assumeFalse")
# Longer timeouts do not reduce what a test can detect (a hang still fails), so they are only a note.
TIMING_RE = re.compile(r"(?i)timeout|atMost|pollInterval|Duration\.|retry|retries")
# Looser numeric tolerance does reduce what a test can detect.
TOLERANCE_RE = re.compile(r"(?i)tolerance|epsilon|within\(|offset\(")
TEST_ANNOTATION_RE = re.compile(r"@Test\b|@TestTemplate\b|@ParameterizedTest\b|@TestFactory\b")
TEST_RESOURCE_EXT = (".conf", ".sql", ".json", ".csv", ".txt", ".yaml", ".yml", ".properties", ".xml")
HOT_PATH_FILE_RE = re.compile(r"(SourceReader|SinkWriter|Reader|Writer|Converter|Deserializ|Serializ|Codec)[A-Za-z0-9]*\.java$")
TYPE_MAPPING_FILE_RE = re.compile(r"(TypeMapper|TypeConverter|Converter|Mapping)[A-Za-z0-9]*\.java$")
EMPTY_CATCH_RE = re.compile(r"catch\s*\([^)]*\)\s*\{\s*\}")
THROW_RE = re.compile(r"\bthrow new\b")

# Text addressed to a reviewer or an AI assistant inside an untrusted PR is data, never an instruction.
INJECTION_RE = re.compile(
    r"(?i)ignore (all |any )?(previous|prior|above) (instructions|rules)|merge (this|it) (now|immediately)"
    r"|(approve|label) this (pr|pull request)|bypass (the )?(review|ci)|you are (an?|the) (ai|assistant|llm|bot)"
)

# Shorter lines ("}", "return x;") appear on both sides of almost every diff and say nothing about moves.
MOVED_MIN_LENGTH = 12
SIZE_LIMIT = 250
SIZE_LIMIT_NEW_CONNECTOR = 1500


class Config(object):
    """Runtime knobs. GitHub renders an unset repository variable as an empty string, so empty means default."""

    def __init__(self, env=None):
        env = os.environ if env is None else env
        self.default_base = env.get("PR_RISK_DEFAULT_BASE") or "dev"
        self.trusted_associations = set(TRUSTED_ASSOCIATIONS)


class Known(object):
    """Artifacts and (artifact, version) pairs already used by poms of the base branch."""

    def __init__(self, artifacts=(), pairs=()):
        self.artifacts = set(artifacts)
        self.pairs = set(pairs)


def load_known(root):
    known = Known()
    for current, dirs, names in os.walk(root):
        dirs[:] = [d for d in dirs if d not in ("target", "node_modules", ".git")]
        if "pom.xml" in names:
            with open(os.path.join(current, "pom.xml"), encoding="utf-8", errors="replace") as fh:
                text = fh.read()
            known.artifacts.update(ARTIFACT_RE.findall(text))
            known.pairs.update(PAIR_RE.findall(text))
    return known


# ------------------------------------------------------------------------------------------
# Pure classification and scoring (no network): this is what the unit tests exercise
# ------------------------------------------------------------------------------------------


def label_name(level):
    return "%s%d-%s" % (LABEL_PREFIX, level, LEVEL_NAMES[level])


def _resolve_connector(path):
    """Split a seatunnel-connectors-v2 path into (module, sub), or None for root-level files."""
    parts = path[len(CONNECTORS_ROOT):].split("/")
    if len(parts) < 2:
        return None
    idx = 0
    # Grouping directories (connector-cdc, connector-file, connector-http, ...) hold the real modules
    # as `connector-*` children; a plain module has `src` as its first child instead.
    if len(parts) > 2 and parts[1].startswith("connector-"):
        idx = 1
    return parts[idx], "/".join(parts[idx + 1:])


def _unit_for(module, sub):
    """Blast-radius unit of a production file. `shared:` units are the wide-blast ones."""
    if module in SHARED_CONNECTOR_MODULES or SHARED_MODULE_SUFFIX.search(module):
        return "shared:" + module
    if module == "connector-jdbc":
        match = JDBC_DIALECT_RE.search("/" + sub)
        if not match or match.group(1) in JDBC_SHARED_DIRS:
            return "shared:connector-jdbc"
        return "connector-jdbc:" + match.group(1)
    return module


def classify_path(path):
    """Return (kind, module, unit, reason).

    kind is one of doc|metadata|registration|pom|services|test|main|shared|platform|other.
    """
    if DOC_RE.search(path) and not path.startswith(".github/"):
        return "doc", None, None, None
    if path in METADATA_FILES or EXAMPLE_RE.match(path):
        return "metadata", None, None, None
    if path in REGISTRATION_FILES:
        return "registration", None, None, None
    if MODULE_POM_RE.match(path):
        return "pom", None, None, None
    if SERVICES_RE.match(path):
        module, sub = _resolve_connector(path)
        return "services", module, _unit_for(module, sub), None
    if PLATFORM_TEST_RE.match(path) and not path.endswith("pom.xml"):
        return "test", None, None, None
    for pattern, reason, _level in PLATFORM_BLOCKS:
        if re.search(pattern, path):
            return "platform", None, None, reason

    if path.startswith(E2E_ROOT):
        top = path[len(E2E_ROOT):].split("/")[0]
        match = re.match(r"^(connector-.+)-e2e$", top)
        if not match:
            return "shared", None, "shared:connector-e2e-infra", "shared connector E2E infrastructure"
        return "test", match.group(1), None, None

    if path.startswith(CONNECTORS_ROOT):
        resolved = _resolve_connector(path)
        if resolved is None:
            return "other", None, None, "file at connectors-v2 root"
        module, sub = resolved
        if sub.startswith("src/test/"):
            return "test", module, None, None
        if not sub.startswith("src/main/"):
            return "other", None, None, "unexpected file in a connector module"
        unit = _unit_for(module, sub)
        return ("shared" if unit.startswith("shared:") else "main"), module, unit, None

    return "other", None, None, "path is not part of any known area"


def platform_level(path):
    for pattern, _reason, level in PLATFORM_BLOCKS:
        if re.search(pattern, path):
            return level
    return 5


def changed_lines(patch):
    added, removed = [], []
    for line in (patch or "").splitlines():
        if line.startswith("@@") or line.startswith("\\"):
            continue
        if line.startswith("+"):
            added.append(line[1:])
        elif line.startswith("-"):
            removed.append(line[1:])
    return added, removed


def is_code_line(content, java):
    text = content.strip()
    if not text:
        return False
    if java and (re.match(r"^(//|/\*|\*|\*/)", text) or text.startswith("import ")):
        return False
    return True


class Scorer(object):
    """Collects adjustments and notes; every rule counts at most once per PR."""

    def __init__(self):
        self.adjustments = {}
        self.notes = {}

    def adjust(self, points, rule, path, detail):
        entry = self.adjustments.get(rule)
        if entry is None:
            self.adjustments[rule] = {"points": points, "rule": rule, "file": path, "detail": detail, "more": 0}
        else:
            entry["more"] += 1

    def note(self, rule, path, detail):
        entry = self.notes.get(rule)
        if entry is None:
            self.notes[rule] = {"rule": rule, "file": path, "detail": detail, "more": 0}
        else:
            entry["more"] += 1


def evaluate(pr, files, cfg=None, known=None):
    """Pure risk evaluation.

    `pr` needs title/body/base/author_association. `known` is a Known set learned from the base
    branch; without it every third-party artifact of a pom change counts as new.
    """
    cfg = cfg or Config({})
    known = known or Known()
    sc = Scorer()

    if INJECTION_RE.search("%s\n%s" % (pr.get("title") or "", pr.get("body") or "")):
        sc.adjust(2, "untrusted-instruction", "-", "PR title/body contains text addressed to a reviewer or AI")

    # Pre-pass: a module is "new" only if its own pom.xml is added by this PR.
    new_modules, new_e2e = set(), set()
    for f in files:
        if f.get("status") == "added" and MODULE_POM_RE.match(f["filename"]):
            name = f["filename"].split("/")[-2]
            (new_e2e if f["filename"].startswith(E2E_ROOT) else new_modules).add(name)

    moved = moved_lines(files)
    docs, added_docs, tests = [], [], []
    platform, other, shared_units, units = [], [], set(), set()
    registration_touched = False
    total_lines = main_code_lines = 0
    added_test_annotation = has_e2e_test = False
    option_removed, option_added = {}, set()
    option_tokens = {}  # path -> (removed tokens, added tokens, keys on both sides)
    new_option_in_existing = False

    for f in files:
        path = f["filename"]
        status = f.get("status", "modified")
        patch = f.get("patch")
        lines = f.get("additions", 0) + f.get("deletions", 0)
        total_lines += lines
        kind, module, unit, reason = classify_path(path)
        java = path.endswith(".java")
        added, removed = changed_lines(patch)
        gone = status in ("removed", "renamed") or bool(f.get("previous_filename"))

        if patch is None and lines > 0 and not (kind == "doc" and status == "added"):
            sc.adjust(2, "uninspectable-diff", path, "binary or oversized diff cannot be inspected")
        for text in added:
            if INJECTION_RE.search(text):
                sc.adjust(2, "untrusted-instruction", path, "added text addressed to a reviewer or AI")
                break

        if kind in ("doc", "metadata"):
            docs.append(path)
            if status == "added" and DOCS_CONNECTOR_RE.match(path):
                added_docs.append(path)
        elif kind == "platform":
            platform.append((path, reason, platform_level(path)))
        elif kind == "other":
            other.append((path, reason))
        elif kind == "registration":
            registration_touched = True
            _registration_facts(sc, path, added, removed, known)
        elif kind == "services":
            registration_touched = True
            if any(x.strip() and not x.strip().startswith("#") for x in removed):
                sc.adjust(2, "registration-removed", path, "service registration entry removed")
        elif kind == "pom":
            _pom_facts(sc, path, status, added, removed, known)
            if status != "added" and any(not MODULE_LINE_RE.search(x) for x in added + removed if x.strip()):
                units.add(path.split("/")[-2])
            if path.startswith(E2E_ROOT):
                tests.append(path)
        elif kind == "test":
            tests.append(path)
            _test_facts(sc, path, status, added, removed, gone)
            if any(TEST_ANNOTATION_RE.search(x) for x in added):
                added_test_annotation = True
                has_e2e_test = has_e2e_test or path.startswith(E2E_ROOT)
        elif kind in ("main", "shared"):
            if module in new_modules and unit.startswith("shared:"):
                unit = module  # nothing depends on a module that does not exist yet
                kind = "main"
            if kind == "shared":
                shared_units.add(unit)
            else:
                units.add(unit)
            fresh = module in new_modules
            main_code_lines += sum(1 for x in added + removed if is_code_line(x, java) and x.strip() not in moved)
            if gone:
                sc.adjust(1, "production-removed", path, "production file deleted or renamed")
            if java:
                _main_facts(sc, path, status, fresh, added, removed, moved)
                if OPTION_FILE_RE.search(path):
                    for x in removed:
                        for key in OPTION_KEY_RE.findall(x):
                            if status == "modified":
                                option_removed.setdefault(key, path)
                    for x in added:
                        for key in OPTION_KEY_RE.findall(x):
                            option_added.add(key)
                            if status == "modified":
                                new_option_in_existing = True
                    if status == "modified":
                        option_tokens[path] = (
                            sorted(t for x in removed for t in OPTION_TOKEN_RE.findall(x)),
                            sorted(t for x in added for t in OPTION_TOKEN_RE.findall(x)),
                            {k for x in removed for k in OPTION_KEY_RE.findall(x)}
                            & {k for x in added for k in OPTION_KEY_RE.findall(x)},
                        )

    # Option contract: a key that only moved to another file is neutral; a vanished key or a changed
    # default/type of a key that stays in the same file is a break for existing jobs.
    for key, path in sorted(option_removed.items()):
        if key not in option_added:
            sc.adjust(2, "option-removed", path, "option key %s no longer exists in the PR" % key)
    for path, (old, new, both) in sorted(option_tokens.items()):
        if both and old != new:
            sc.adjust(2, "option-changed", path, "default value or type of an existing option changed")

    # ---- base level
    if platform:
        base = max(level for _p, _r, level in platform)
        pr_class = "platform" if base == 5 else "tooling"
    elif other:
        base, pr_class = 3, "other"
    elif shared_units:
        base, pr_class = 4, "shared"
    elif new_modules:
        base, pr_class = 2, "new-connector"
    elif units:
        base, pr_class = 2, "connector"
    elif registration_touched:
        base, pr_class = 2, "registration"
    elif tests:
        base, pr_class = 1, "tests"
    else:
        base, pr_class = 1, "docs"

    # ---- whole-PR adjustments
    if len(units | shared_units) > 1:
        sc.adjust(1, "multi-unit", "-", "touches several connector units: %s" % ", ".join(sorted(units | shared_units)))
    limit = SIZE_LIMIT_NEW_CONNECTOR if new_modules else SIZE_LIMIT
    if main_code_lines > limit:
        sc.adjust(1, "size", "-", "%d production code lines (limit %d)" % (main_code_lines, limit))
    if main_code_lines > 0 and not added_test_annotation:
        sc.adjust(1, "no-regression-test", "-", "production code changed but no test method was added or changed")
    if new_modules and not added_docs:
        sc.adjust(1, "no-docs", "-", "a new connector should add its page under docs/en/connectors/")
    if new_option_in_existing and not docs:
        sc.adjust(1, "option-without-docs", "-", "new option added to an existing connector but no docs changed")
    if pr.get("author_association") not in cfg.trusted_associations:
        sc.adjust(1, "new-contributor", "-", "author association %s" % pr.get("author_association"))

    # ---- notes (never change the level)
    if new_modules and not has_e2e_test:
        sc.note("no-e2e", "-", "new connector has no added E2E test method")
    if shared_units:
        sc.note("dependents-not-tested", "-", "CI selects tests by changed path, so dependents of %s are not run"
                % ", ".join(sorted(shared_units)))
    for path, why, _level in platform[:3]:
        sc.note("platform-path", path, why)
    for path, why in other[:3]:
        sc.note("unclassified-path", path, why)

    level, adjustments = compute_level(base, sc.adjustments.values())
    if pr.get("base") != cfg.default_base and level < 3:
        level = 3
        adjustments = adjustments + [{"points": 0, "rule": "non-default-base", "file": "-",
                                      "detail": "target branch %s is not %s: floor of 3" % (pr.get("base"), cfg.default_base),
                                      "more": 0}]
    return {
        "level": level,
        "label": label_name(level),
        "class": pr_class,
        "base": base,
        "adjustments": adjustments,
        "notes": list(sc.notes.values()),
        "units": sorted(units | shared_units | new_modules),
        "stats": {"files": len(files), "lines": total_lines, "main_code_lines": main_code_lines},
    }


def compute_level(base, adjustments):
    """base 5 is fixed; otherwise base plus adjustments, capped at 4 (only platform code is critical)."""
    adjustments = sorted(adjustments, key=lambda a: (-a["points"], a["rule"]))
    if base >= 5:
        return 5, adjustments
    return min(4, base + sum(a["points"] for a in adjustments)), adjustments


def _supply_facts(sc, path, added, known):
    text = "\n".join(added)
    for danger in POM_DANGER_RE.findall(text):
        sc.adjust(2, "new-supply-chain", path, "pom declares %s (code execution or another registry)" % danger.strip("<"))
    for artifact in ARTIFACT_RE.findall(text):
        if artifact.startswith(("connector-", "seatunnel-")) or artifact in ("connectors-v2",):
            continue
        if artifact not in known.artifacts:
            sc.adjust(2, "new-supply-chain", path, "artifact %s is not used by any existing pom" % artifact)
    for artifact, version in PAIR_RE.findall(text):
        if (artifact, version) not in known.pairs and not artifact.startswith(("connector-", "seatunnel-")):
            sc.adjust(2, "new-supply-chain", path, "%s:%s is not a version used by any existing pom" % (artifact, version))


def _pom_facts(sc, path, status, added, removed, known):
    if any("<module>" in x for x in removed):
        sc.adjust(2, "registration-removed", path, "a <module> entry was removed")
    other_added = [x for x in added if "<module>" not in x]
    _supply_facts(sc, path, other_added, known)
    if status != "added" and any(ARTIFACT_RE.search(x) or "<version>" in x for x in other_added + removed):
        sc.adjust(1, "dependency-change", path, "dependency or version lines of an existing pom changed")


def _registration_facts(sc, path, added, removed, known):
    if any(x.strip() and not x.strip().startswith("#") for x in removed):
        sc.adjust(2, "registration-removed", path, "a registration entry was removed or rewritten")
    if path == "seatunnel-dist/pom.xml":
        _supply_facts(sc, path, added, known)


def _test_facts(sc, path, status, added, removed, gone):
    """Weakening is a net loss of detection power in one test file, not a rewritten line.

    A removed assertion that is replaced by an equivalent one (reformatted, switched to polling) keeps the
    file's assertion count; a deleted test file, fewer assertion lines, a newly disabled test, a looser
    numeric tolerance, or shrinking test data does not.
    """
    if gone:
        sc.adjust(2, "test-weakening", path, "test file deleted or renamed")
        return
    if status == "added":
        return
    code_removed = [x for x in removed if is_code_line(x, True)]
    code_added = [x for x in added if is_code_line(x, True)]
    lost = sum(1 for x in code_removed if ASSERT_RE.search(x)) - sum(1 for x in code_added if ASSERT_RE.search(x))
    if lost > 0:
        sc.adjust(2, "test-weakening", path, "%d fewer assertion/test-method lines than before" % lost)
        return
    for text in code_added:
        if DISABLE_RE.search(text):
            sc.adjust(2, "test-weakening", path, "disables or conditionalises an existing test: %s" % text.strip())
            return
    if any(TOLERANCE_RE.search(x) for x in code_added + code_removed):
        sc.adjust(2, "test-weakening", path, "numeric tolerance of an existing test changed")
        return
    if path.endswith(TEST_RESOURCE_EXT) and sum(1 for x in removed if x.strip()) > sum(1 for x in added if x.strip()):
        sc.adjust(2, "test-weakening", path, "test fixture shrank (could narrow data coverage)")
        return
    if any(TIMING_RE.search(x) for x in code_added + code_removed):
        sc.note("timing-changed", path, "timeout / retry / polling changed in an existing test")


def moved_lines(files):
    """Stripped production lines that are removed somewhere and added somewhere else in the same PR.

    A relocated definition (an option moved to a shared config class, a method moved to a helper) is
    not new behavior, so its lines must not trigger size or sensitive-code adjustments.
    """
    added_all, removed_all = set(), set()
    for f in files:
        kind = classify_path(f["filename"])[0]
        if kind in ("main", "shared") and f["filename"].endswith(".java"):
            added, removed = changed_lines(f.get("patch"))
            added_all.update(x.strip() for x in added)
            removed_all.update(x.strip() for x in removed)
    return {x for x in added_all & removed_all if len(x) >= MOVED_MIN_LENGTH}


def _main_facts(sc, path, status, fresh, added, removed, moved):
    code = [x for x in added + removed if is_code_line(x, True) and x.strip() not in moved]
    if not fresh and status == "modified":
        if PERSISTED_FILE_RE.search(path) and code:
            sc.adjust(2, "persisted-format", path, "checkpoint / split / state / commit-info class modified")
        if any(SERIAL_RE.search(x) for x in code):
            sc.adjust(2, "persisted-format", path, "serialVersionUID touched")
        for name, regex, detail in (
            ("concurrency", CONCURRENCY_RE, "threading / locking / async primitive touched"),
            ("security", SECURITY_RE, "security-sensitive identifier touched"),
            ("classloading", CLASSLOAD_RE, "reflection / classloading / process-level API touched"),
        ):
            for text in code:
                if regex.search(text) and not (name == "security" and DECLARATION_RE.search(text)):
                    sc.adjust(1, "sensitive-code", path, "%s (%s): %s" % (detail, name, text.strip()))
                    break
    else:
        for name, regex in (("concurrency", CONCURRENCY_RE), ("security", SECURITY_RE), ("classloading", CLASSLOAD_RE)):
            if any(regex.search(x) for x in code):
                sc.note(name, path, "%s words in new code; check by eye" % name)
        if any(PROCESS_RE.search(x) for x in code):
            sc.note("process-level-call", path, "System.exit / ProcessBuilder / Runtime.getRuntime / setAccessible in new code")
    if HOT_PATH_FILE_RE.search(path):
        sc.note("hot-path", path, "per-row reader/writer/serialization path: check allocation, locking and complexity")
    if TYPE_MAPPING_FILE_RE.search(path) and status == "modified":
        sc.note("type-mapping", path, "type mapping / converter change can alter output data of existing jobs")
    if any(EMPTY_CATCH_RE.search(x) for x in added):
        sc.note("empty-catch", path, "added an empty catch block (error swallowing)")
    if sum(1 for x in removed if THROW_RE.search(x)) > sum(1 for x in added if THROW_RE.search(x)):
        sc.note("removed-throw", path, "fewer throw statements: confirm this is not a silent degradation")


def render_report(result, head_sha):
    lines = [
        "<!-- %s sha=%s level=%d -->" % (MARKER, head_sha, result["level"]),
        "### PR risk: `%s`" % result["label"],
        "",
        "Head commit `%s`. Class: `%s`. Units: %s. Files: %d, changed lines: %d, production code lines: %d."
        % (head_sha[:12], result["class"], ", ".join(result["units"]) or "-", result["stats"]["files"],
           result["stats"]["lines"], result["stats"]["main_code_lines"]),
        "",
        "| Step | Points | Why |",
        "|---|---|---|",
        "| base | %d | %s |" % (result["base"], BASE_REASON[result["class"]]),
    ]
    for a in result["adjustments"]:
        where = "" if a["file"] == "-" else "`%s`: " % a["file"]
        more = " (and %d more)" % a["more"] if a["more"] else ""
        lines.append("| %s | %s | %s%s%s |" % (a["rule"], "+%d" % a["points"] if a["points"] else "floor", where, a["detail"], more))
    lines.append("| **result** | **%d** | capped at 4 unless the base is 5 |" % result["level"])
    if result["notes"]:
        lines += ["", "Notes for the reviewer (they do not change the level):"]
        for n in result["notes"]:
            where = "" if n["file"] == "-" else "`%s`: " % n["file"]
            more = " (and %d more)" % n["more"] if n["more"] else ""
            lines.append("- [%s] %s%s%s" % (n["rule"], where, n["detail"], more))
    lines += ["", "This is a triage aid computed from the diff only; no PR code is executed. It does not approve, block "
              "or merge anything. Merge decisions stay with committers."]
    return "\n".join(lines)


BASE_REASON = {
    "docs": "docs, examples or metadata only",
    "tests": "tests only",
    "registration": "registration files only",
    "connector": "one existing connector unit",
    "new-connector": "brand-new connector module",
    "shared": "shared connector module (dependents are affected)",
    "other": "path outside every known area",
    "platform": "platform code (fixed)",
    "tooling": "tooling around the runtime (CLI, benchmarks, examples, deploy, repository settings)",
}


def plan_label_changes(existing, level):
    """Return (to_add, to_remove) so that exactly one `risk: *` label remains and other labels are untouched."""
    target = label_name(level)
    to_remove = [n for n in existing if n.startswith(LABEL_PREFIX) and n != target]
    return ([] if target in existing else [target]), to_remove


# ------------------------------------------------------------------------------------------
# GitHub access
# ------------------------------------------------------------------------------------------


class ApiError(RuntimeError):
    pass


class GitHub(object):
    def __init__(self, repo, token):
        self.repo = repo
        self.token = token
        self.api = os.environ.get("GITHUB_API_URL", "https://api.github.com")

    def call(self, method, path, body=None):
        url = path if path.startswith("http") else self.api + path
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(url, data=data, method=method, headers={
            "Authorization": "Bearer " + self.token,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "seatunnel-pr-risk-label",
        })
        try:
            with urllib.request.urlopen(request, timeout=60) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as err:
            raw = err.read()
            try:
                return err.code, json.loads(raw)
            except ValueError:
                return err.code, {"message": raw.decode("utf-8", "replace")}

    def get(self, path):
        status, data = self.call("GET", path)
        if status >= 300:
            raise ApiError("GET %s -> %s %s" % (path, status, data))
        return data

    def paginate(self, path):
        items, page = [], 1
        sep = "&" if "?" in path else "?"
        while True:
            batch = self.get("%s%sper_page=100&page=%d" % (path, sep, page))
            items.extend(batch)
            if len(batch) < 100:
                return items
            page += 1


def evaluate_pr(gh, number, cfg):
    pr = gh.get("/repos/%s/pulls/%d" % (gh.repo, number))
    files = gh.paginate("/repos/%s/pulls/%d/files" % (gh.repo, number))
    view = {"title": pr.get("title"), "body": pr.get("body"), "base": pr["base"]["ref"],
            "author_association": pr.get("author_association")}
    # The workflow checks out the base branch, so these are the artifacts that are already vetted.
    known = load_known(os.environ.get("PR_RISK_REPO_ROOT", "."))
    return pr, evaluate(view, files, cfg, known)


def ensure_labels(gh):
    for level, (color, description) in LEVEL_META.items():
        status, data = gh.call("POST", "/repos/%s/labels" % gh.repo,
                               {"name": label_name(level), "color": color, "description": description})
        if status not in (201, 422):  # 422 means the label already exists
            raise ApiError("create label %s -> %s %s" % (label_name(level), status, data))


def upsert_report(gh, number, body):
    for c in gh.paginate("/repos/%s/issues/%d/comments" % (gh.repo, number)):
        if "<!-- %s " % MARKER in (c.get("body") or "") and c["user"]["type"] == "Bot":
            gh.call("PATCH", "/repos/%s/issues/comments/%d" % (gh.repo, c["id"]), {"body": body})
            return
    gh.call("POST", "/repos/%s/issues/%d/comments" % (gh.repo, number), {"body": body})


def apply_label(gh, number, level):
    existing = [x["name"] for x in gh.paginate("/repos/%s/issues/%d/labels" % (gh.repo, number))]
    to_add, to_remove = plan_label_changes(existing, level)
    # Add before removing so the PR never has zero risk labels in between.
    if to_add:
        gh.call("POST", "/repos/%s/issues/%d/labels" % (gh.repo, number), {"labels": to_add})
    for name in to_remove:
        gh.call("DELETE", "/repos/%s/issues/%d/labels/%s" % (gh.repo, number, urllib.parse.quote(name, safe="")))


def main(argv=None):
    parser = argparse.ArgumentParser(description="PR risk triage labels")
    parser.add_argument("command", choices=["evaluate", "classify"])
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--pr", type=int)
    parser.add_argument("--apply", action="store_true", help="evaluate: create labels, upsert the comment, set the label")
    parser.add_argument("--files-json", help="classify: saved pulls/N/files payload")
    parser.add_argument("--title", default="")
    parser.add_argument("--body", default="")
    parser.add_argument("--base", default="dev")
    parser.add_argument("--author-association", default="MEMBER")
    args = parser.parse_args(argv)
    cfg = Config()

    if args.command == "classify":
        with open(args.files_json) as fh:
            files = json.load(fh)
        pr = {"title": args.title, "body": args.body, "base": args.base, "author_association": args.author_association}
        result = evaluate(pr, files, cfg, load_known(os.environ.get("PR_RISK_REPO_ROOT", ".")))
        print(json.dumps(result, indent=2))
        return 0

    gh = GitHub(args.repo, os.environ["GITHUB_TOKEN"])
    pr, result = evaluate_pr(gh, args.pr, cfg)
    report = render_report(result, pr["head"]["sha"])
    print(report)
    if args.apply:
        ensure_labels(gh)
        upsert_report(gh, args.pr, report)
        apply_label(gh, args.pr, result["level"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
