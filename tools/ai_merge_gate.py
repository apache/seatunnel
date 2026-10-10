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
Gatekeeper for the `ai-merge` pull request label.

`ai-merge` means: this PR belongs to a risk class whose safety properties can be checked
mechanically, so once the designated approver has approved the current head commit and CI is
green, it can be merged without a further human code review.

Five PR classes can carry the label (everything else is rejected, the allow-list is closed):
  docs           only documentation (*.md, docs/** images)
  tests          only connector tests / connector E2E (src/test, seatunnel-connector-v2-e2e/connector-*-e2e)
  connector      a bug fix or small improvement inside exactly ONE connector blast-radius unit, plus
                 its own tests/docs. A unit is a connector module, or one dialect of connector-jdbc.
  new-connector  ONE brand-new connector module with its tests and docs, including the registration
                 changes it needs: the module pom, the <module> entries, the seatunnel-dist
                 dependency, plugin-mapping.properties, config/plugin_config and its SPI service
                 lines. Registration edits must be purely additive and name only the new module, and
                 the new pom may only use third-party artifacts that other poms in the repo already
                 use (so no new jar, license or supply-chain decision hides in an unreviewed PR).
  shared         a small change to a shared connector module (connector-common, *-base, JDBC core,
                 the fake/assert/console fixtures). CI only runs the tests of the changed module, so
                 this class is eligible only when AI_MERGE_WIDE_CHECKS names the CI checks that
                 exercise the dependents, and those checks are green on the head commit.

Anything touching engine/api/core/common/config/formats/transforms/translation, pom.xml or
registration files outside a new-connector PR, .github/**, tools/**, license files, checkpoint/state
/serialization classes of existing connectors, option removals/default changes, security-sensitive
code, concurrency, classloading, test weakening or file deletion/rename is rejected.

SECURITY MODEL: this script only reads the PR through the GitHub REST/GraphQL API. It never checks
out or executes PR code, so it is safe under `pull_request_target`. It must always be run from the
base branch.

Sub-commands:
  evaluate  --pr N [--comment]   Mechanical eligibility of a PR (exit 0 eligible, 1 not).
  gate      --pr N [--merge]     Full merge gate: label provenance + eligibility + approval +
                                 CI + mergeability (+ squash merge pinned to the head SHA).
  enforce                        Event hook (reads GITHUB_EVENT_PATH): strips the label on new
                                 pushes, validates label provenance and eligibility on `labeled`.
  sweep    [--merge]             Run `gate` for every open PR carrying the label.
  classify --files-json F        Offline evaluation of a saved `pulls/N/files` payload.

Configuration (env or flags; unset security-relevant values fail closed):
  GITHUB_TOKEN, GITHUB_REPOSITORY
  AI_MERGE_LABEL_ACTORS     comma list of logins allowed to apply the label (required)
  AI_MERGE_APPROVERS        comma list of logins whose APPROVED review on the head SHA is required
  AI_MERGE_REQUIRED_CHECKS  comma list of check-run names that must be `success` (default: Build)
  AI_MERGE_NEW_CONNECTOR_CHECKS  extra required checks for new-connector PRs
                            (default: Code style,Dependency licenses; Code style runs
                            tools/check_connector_registration.py)
  AI_MERGE_WIDE_CHECKS      check-run names proving dependents of a shared module were tested; the
                            shared class stays rejected while this is empty
  AI_MERGE_BASE             only PRs targeting this branch qualify (default: dev)
  AI_MERGE_ENABLED          must be `true` for --merge to actually merge (otherwise dry run)
"""

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request

LABEL = "ai-merge"
MARKER = "ai-merge-eligibility"

# Author associations that may receive the label. First-time contributors are excluded by default
# because the label removes human code review and the diff author is untrusted.
TRUSTED_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR", "CONTRIBUTOR"}

# ------------------------------------------------------------------------------------------
# Path policy
# ------------------------------------------------------------------------------------------

# Paths that can never carry the label. The allow-list below is the real gate; this list exists
# so that the rejection message names the actual reason.
HARD_BLOCKS = [
    (r"(^|/)pom\.xml$", "pom.xml changes are only accepted to define or register a brand-new connector module"),
    (r"^\.github/", "CI and repository automation: a PR must not change its own verifier"),
    (r"^tools/", "CI, release and compatibility tooling"),
    (r"^seatunnel-engine/", "engine runtime (Zeta): scheduling, checkpoint, state, RPC"),
    (r"^seatunnel-api/", "public API / SPI contract"),
    (r"^seatunnel-core/", "core runtime and CLI wiring"),
    (r"^seatunnel-common/", "shared common code used by every module"),
    (r"^seatunnel-config/", "config framework"),
    (r"^seatunnel-formats/", "shared format code"),
    (r"^seatunnel-transforms-v2/", "transform runtime"),
    (r"^seatunnel-translation/", "Flink/Spark translation layer"),
    (r"^seatunnel-plugin-discovery/", "plugin discovery"),
    (r"^seatunnel-dist/", "distribution packaging"),
    (r"^seatunnel-(ci-tools|cli|edge-agent|trace|benchmarks|examples)/", "tooling and runtime support modules"),
    (
        r"^seatunnel-e2e/(seatunnel-engine-e2e|seatunnel-e2e-common|seatunnel-core-e2e|"
        r"seatunnel-transforms-v2-e2e|seatunnel-edge-agent-e2e)/",
        "shared or engine E2E infrastructure",
    ),
    (
        r"^(plugin-mapping\.properties|config/|bin/|plugins/|deploy/|LICENSE|NOTICE|licenses/|mvnw|\.mvn/|\.asf\.yaml)",
        "plugin registration, distribution, license or wrapper files",
    ),
    (r"META-INF/services/", "SPI service registration"),
    (r"(^|/)CODEOWNERS$", "code ownership"),
]

# Registration files a brand-new connector has to touch. They are only acceptable inside a
# new-connector PR, purely additive, and naming only the new module (see _check_registration).
REGISTRATION_FILES = {
    "seatunnel-connectors-v2/pom.xml": "module",
    "seatunnel-e2e/seatunnel-connector-v2-e2e/pom.xml": "module",
    "seatunnel-dist/pom.xml": "dist",
    "plugin-mapping.properties": "mapping",
    "config/plugin_config": "plugin_config",
    ".github/workflows/labeler/label-scope-conf.yml": "labeler",
}
# Example job configs shipped with a new connector; only brand-new files are accepted.
EXAMPLE_RE = re.compile(r"^seatunnel-examples/.*/src/main/resources/examples/[^/]+\.conf$")
DOCS_CONNECTOR_RE = re.compile(r"^docs/en/(connectors|connector-v2)/")
LABELER_STRUCTURE_RE = re.compile(r"^\s*(-\s+)?(all|any|changed-files):\s*$|^[A-Za-z0-9_.-]+:\s*$")
# Group poms (connector-cdc, connector-file, connector-http, ...) list their children as <module>.
GROUP_POM_RE = re.compile(r"^seatunnel-connectors-v2/connector-[^/]+/pom\.xml$")
# pom of a connector module or of its E2E module; a freshly added one defines a new module.
MODULE_POM_RE = re.compile(
    r"^seatunnel-connectors-v2/(connector-[^/]+/)?connector-[^/]+/pom\.xml$"
    r"|^seatunnel-e2e/seatunnel-connector-v2-e2e/connector-[^/]+-e2e/pom\.xml$"
)
SERVICES_RE = re.compile(r"^seatunnel-connectors-v2/.*/src/main/resources/META-INF/services/[^/]+$")
MODULE_LINE_RE = re.compile(r"^\s*<module>(connector-[A-Za-z0-9_.-]+)</module>\s*$")
DIST_LINE_RES = [
    re.compile(r"^\s*</?dependency>\s*$"),
    re.compile(r"^\s*<groupId>org\.apache\.seatunnel</groupId>\s*$"),
    re.compile(r"^\s*<version>\$\{project\.version\}</version>\s*$"),
    re.compile(r"^\s*<scope>provided</scope>\s*$"),
]
ARTIFACT_LINE_RE = re.compile(r"^\s*<artifactId>([^<\s]+)</artifactId>\s*$")
MAPPING_LINE_RE = re.compile(r"^seatunnel\.(source|sink|transform)\.[A-Za-z0-9_.-]+\s*=\s*(connector-[A-Za-z0-9_.-]+)\s*$")
PLUGIN_CONFIG_LINE_RE = re.compile(r"^(connector-[A-Za-z0-9_.-]+)\s*$")
# Anything in a new module pom that can execute code at build time or pull from another registry.
POM_DANGER_RE = re.compile(r"<repositor|<pluginRepositor|<profile|antrun|exec-maven|<systemPath|<scope>system")
VERSION_LINE_RE = re.compile(r"^\s*<version>([^<]*)</version>\s*$")
# Process-level calls stay blocked even inside brand-new connector code.
NEW_CODE_BLOCK_RE = re.compile(r"System\.exit|ProcessBuilder|Runtime\.getRuntime|setAccessible")
# Removing or altering a public/protected signature of a shared module breaks every dependent.
SIGNATURE_RE = re.compile(r"^\s*(public|protected)\b.*[({]\s*$")

# Connector modules that are shared by many other modules or used as fixtures across E2E suites.
SHARED_CONNECTOR_MODULES = {"connector-common", "connector-fake", "connector-assert", "connector-console"}
SHARED_MODULE_SUFFIX = re.compile(r"-(base|base-hadoop)$")

CONNECTORS_ROOT = "seatunnel-connectors-v2/"
E2E_ROOT = "seatunnel-e2e/seatunnel-connector-v2-e2e/"
JDBC_DIALECT_RE = re.compile(r"/jdbc/(?:internal/dialect|catalog)/([^/]+)/[^/]+")
JDBC_SHARED_DIRS = {"dialectenum", "utils"}

DOC_RE = re.compile(r"(\.md$)|(^docs/.*\.(png|jpe?g|gif)$)")

# ------------------------------------------------------------------------------------------
# Content policy (applied to changed lines only)
# ------------------------------------------------------------------------------------------

# Checkpointed state, splits, commit infos and serializers define on-disk / on-wire formats.
CHECKPOINT_FILE_RE = re.compile(r"(State|Split|CommitInfo|Checkpoint)[A-Za-z0-9]*\.java$|Serializer\.java$|/state/")
SERIAL_RE = re.compile(r"serialVersionUID")
# Option, config and factory definitions are the user-facing job contract.
OPTION_FILE_RE = re.compile(r"(Option|Options|Config|Configs|Factory|Rule)[A-Za-z0-9]*\.java$")
OPTION_REMOVED_RE = re.compile(
    r"Options\.key|\.key\(|defaultValue|noDefaultValue|Type\(|OptionRule|\.required\(|\.optional\("
    r"|\.exclusive\(|\.conditional\(|\.bundled\(|[Ii]dentifier|DEFAULT_|PLUGIN_NAME"
)
OPTION_ADDED_RE = re.compile(r"Options\.key\(")
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
ASSERT_RE = re.compile(
    r"\b(assert\w*|(Assertions?|Assert|Assumptions?)\.|verify|fail|assume\w*|expect\w*)\b"
    r"|@Test\b|@TestTemplate\b|@ParameterizedTest\b|@TestFactory\b"
)
DISABLE_RE = re.compile(r"@Disabled|@Ignore|@DisabledOnContainer|@DisabledOnOs|@DisabledIf|Assumptions\.|assumeTrue|assumeFalse")
TIMING_RE = re.compile(r"(?i)timeout|atMost|pollInterval|Duration\.|retry|retries|tolerance|epsilon|within\(|offset\(")
TEST_ANNOTATION_RE = re.compile(r"@Test\b|@TestTemplate\b|@ParameterizedTest\b|@TestFactory\b")
HOT_PATH_FILE_RE = re.compile(r"(SourceReader|SinkWriter|Reader|Writer|Converter|Deserializ|Serializ|Codec)[A-Za-z0-9]*\.java$")
TYPE_MAPPING_FILE_RE = re.compile(r"(TypeMapper|TypeConverter|Converter|Mapping)[A-Za-z0-9]*\.java$")
EMPTY_CATCH_RE = re.compile(r"catch\s*\([^)]*\)\s*\{\s*\}")
THROW_RE = re.compile(r"\bthrow new\b")
TEST_RESOURCE_EXT = (".conf", ".sql", ".json", ".csv", ".txt", ".yaml", ".yml", ".properties", ".xml")

# Text addressed to a reviewer/AI inside an untrusted PR is data, never an instruction.
INJECTION_RE = re.compile(
    r"(?i)ignore (all |any )?(previous|prior|above) (instructions|rules)|ai[- ]merge|merge (this|it) (now|immediately)"
    r"|(approve|label) this (pr|pull request)|bypass (the )?(review|ci|gate)|system prompt"
    r"|you are (an?|the) (ai|assistant|llm|bot)"
)

# Size caps, counted as added + deleted lines.
CAPS = {
    "docs": {"files": 40, "lines": 1500},
    "tests": {"files": 25, "lines": 1200},
    "connector": {"files": 25, "lines": 1200, "main_code_lines": 250},
    "new-connector": {"files": 120, "lines": 8000},
    "shared": {"files": 15, "lines": 500, "main_code_lines": 80},
}


class Config(object):
    """Runtime policy knobs; defaults fail closed for identities."""

    def __init__(self, env=None):
        env = os.environ if env is None else env

        def get(key, default=""):
            # GitHub passes an unset repository variable as an empty string, which must mean "use the default".
            return env.get(key) or default

        self.base = get("AI_MERGE_BASE", "dev")
        self.label_actors = _csv(get("AI_MERGE_LABEL_ACTORS"))
        self.approvers = _csv(get("AI_MERGE_APPROVERS", "DanielLeens"))
        self.required_checks = _csv(get("AI_MERGE_REQUIRED_CHECKS", "Build"))
        self.new_connector_checks = _csv(get("AI_MERGE_NEW_CONNECTOR_CHECKS", "Code style,Dependency licenses"))
        # Empty means "no proof that dependents of a shared module are tested": the shared class fails closed.
        self.wide_checks = _csv(get("AI_MERGE_WIDE_CHECKS"))
        self.trusted_associations = set(TRUSTED_ASSOCIATIONS)
        self.merge_enabled = get("AI_MERGE_ENABLED", "false").lower() == "true"


def _csv(value):
    return [x.strip() for x in value.split(",") if x.strip()]


# ------------------------------------------------------------------------------------------
# Pure classification (no network): this is what the unit tests exercise
# ------------------------------------------------------------------------------------------


def _resolve_connector(path):
    """Split a seatunnel-connectors-v2 path into (module, sub) or None for root-level files."""
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
    """Return (category, module, unit, reason).

    category is one of doc|test|main|services|registration|pom|blocked. `registration`, `pom` and
    `services` are provisional: evaluate() only accepts them inside a new-connector PR.
    """
    if DOC_RE.search(path) and not path.startswith(".github/"):
        return "doc", None, None, None
    if path in REGISTRATION_FILES:
        return "registration", None, None, None
    if EXAMPLE_RE.match(path):
        return "example", None, None, None
    if MODULE_POM_RE.match(path):
        return "pom", None, None, None
    if SERVICES_RE.match(path):
        resolved = _resolve_connector(path)
        return "services", resolved[0], _unit_for(resolved[0], resolved[1]), None
    for pattern, reason in HARD_BLOCKS:
        if re.search(pattern, path):
            return "blocked", None, None, reason

    if path.startswith(E2E_ROOT):
        top = path[len(E2E_ROOT):].split("/")[0]
        match = re.match(r"^(connector-.+)-e2e$", top)
        if not match:
            return "blocked", None, None, "shared connector E2E infrastructure"
        return "test", match.group(1), None, None

    if path.startswith(CONNECTORS_ROOT):
        resolved = _resolve_connector(path)
        if resolved is None:
            return "blocked", None, None, "file at connectors-v2 root"
        module, sub = resolved
        if sub.startswith("src/test/"):
            return "test", module, None, None
        if not sub.startswith("src/main/"):
            return "blocked", None, None, "unexpected file in connector module"
        return "main", module, _unit_for(module, sub), None

    return "blocked", None, None, "outside the ai-merge allow-list"


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


def load_known_artifacts(root):
    """Collect every artifactId already declared by a pom under `root` (a checkout of the base branch)."""
    known = set()
    for current, dirs, names in os.walk(root):
        dirs[:] = [d for d in dirs if d not in ("target", "node_modules", ".git")]
        if "pom.xml" in names:
            with open(os.path.join(current, "pom.xml"), encoding="utf-8", errors="replace") as fh:
                known.update(re.findall(r"<artifactId>\s*([^<\s]+)\s*</artifactId>", fh.read()))
    return known


def evaluate(pr, files, cfg=None, known_artifacts=None):
    """Pure eligibility evaluation. `pr` needs title/body/draft/base/author_association.

    `known_artifacts` is the set of artifactIds already used by poms of the base branch; it decides
    whether a new connector pom introduces a third-party dependency that needs a human decision.
    """
    cfg = cfg or Config({})
    blockers, warnings = [], []

    def block(path, rule, detail):
        blockers.append({"file": path, "rule": rule, "detail": detail})

    def warn(path, rule, detail):
        warnings.append({"file": path, "rule": rule, "detail": detail})

    if pr.get("draft"):
        block("-", "pr-state", "draft PR")
    if pr.get("base") != cfg.base:
        block("-", "base-branch", "target branch is %s, only %s qualifies" % (pr.get("base"), cfg.base))
    if pr.get("author_association") not in cfg.trusted_associations:
        block("-", "author", "author association %s is not trusted for unreviewed merge" % pr.get("author_association"))
    if not files:
        block("-", "empty", "no changed files")
    if len(files) >= 300:
        block("-", "size", "300 or more files")

    if INJECTION_RE.search("%s\n%s" % (pr.get("title") or "", pr.get("body") or "")):
        block("-", "untrusted-instruction", "PR title/body contains text addressed to a reviewer or AI")

    # Pre-pass: a module is "new" only if its own pom.xml is added by this PR.
    new_modules, new_e2e = set(), set()
    for f in files:
        if f.get("status") == "added" and MODULE_POM_RE.match(f["filename"]):
            name = f["filename"].split("/")[-2]
            (new_e2e if f["filename"].startswith(E2E_ROOT) else new_modules).add(name)
    added_paths = {f["filename"] for f in files if f.get("status") == "added"}

    mains, tests, docs, added_docs = [], [], [], []
    units, test_modules, main_modules = set(), set(), set()
    total_lines = main_code_lines = 0
    new_option_files, added_test_annotation, has_e2e_test = [], False, False

    for f in files:
        path = f["filename"]
        status = f.get("status", "modified")
        patch = f.get("patch")
        lines = f.get("additions", 0) + f.get("deletions", 0)
        total_lines += lines
        category, module, unit, reason = classify_path(path)
        java = path.endswith(".java")
        added, removed = changed_lines(patch)
        if module in new_modules and unit and unit.startswith("shared:"):
            unit = module  # nothing depends on a module that does not exist yet

        if status in ("removed", "renamed") or f.get("previous_filename"):
            block(path, "delete-or-rename", "file %s; deletions and renames are never unreviewed" % status)
        if category == "blocked":
            block(path, "path", reason)
        if patch is None and lines > 0 and not (category == "doc" and status == "added"):
            block(path, "no-diff", "binary or oversized diff cannot be inspected")
        for text in added:
            if INJECTION_RE.search(text):
                block(path, "untrusted-instruction", "added text addressed to a reviewer or AI")
                break

        if category == "doc":
            docs.append(path)
            if status == "added" and DOCS_CONNECTOR_RE.match(path):
                added_docs.append(path)
        elif category == "example":
            docs.append(path)
            if status != "added":
                block(path, "path", "only brand-new example configs are accepted, existing ones must not change")
        elif category == "pom":
            _check_pom(path, status, added, removed, new_modules, new_e2e, known_artifacts, block)
            if path.startswith(E2E_ROOT):
                tests.append(path)
        elif category == "registration":
            _check_registration(path, REGISTRATION_FILES[path], added, removed, new_modules, new_e2e, block)
        elif category == "services":
            _check_services(path, status, added, removed, added_paths, new_modules, module, block)
        elif category == "test":
            tests.append(path)
            test_modules.add(module)
            _check_test_file(path, status, added, removed, block, warn)
            if any(TEST_ANNOTATION_RE.search(x) for x in added):
                added_test_annotation = True
                has_e2e_test = has_e2e_test or path.startswith(E2E_ROOT)
        elif category == "main":
            mains.append(path)
            units.add(unit)
            main_modules.add(module)
            code = sum(1 for x in added + removed if is_code_line(x, java))
            main_code_lines += code
            fresh = module in new_modules and status == "added"
            _check_main_file(path, status, added, removed, block, warn, relaxed=fresh,
                             wide=bool(unit and unit.startswith("shared:")))
            if java and not fresh and OPTION_FILE_RE.search(path) and any(OPTION_ADDED_RE.search(x) for x in added):
                new_option_files.append(path)
            if module in new_modules and status != "added":
                block(path, "new-connector", "file of a brand-new module must be added, not modified")

    wide_units = {u for u in units if u.startswith("shared:")}
    if len(units) > 1 or len(new_modules) > 1 or (new_modules and units - new_modules):
        block("-", "multi-unit", "touches more than one connector blast-radius unit: %s"
              % ", ".join(sorted(units | new_modules)))
    # Tests may only come from the unit under change; E2E for another connector is unrelated evidence.
    # A brand-new E2E module (added in this PR) is exempt from name matching: it is new code with no history.
    foreign_tests = {m for m in test_modules - main_modules - {None} if m + "-e2e" not in new_e2e}
    if mains and foreign_tests:
        block("-", "foreign-tests", "tests of other modules mixed into a connector change: %s"
              % ", ".join(sorted(foreign_tests)))
    if main_code_lines > 0 and not added_test_annotation:
        block("-", "no-regression-test", "production code changed but no test method was added or changed")
    if new_option_files and not docs:
        block(new_option_files[0], "option-without-docs", "new option added but no documentation changed")
    if new_modules and not added_docs:
        block("-", "new-connector-docs", "a new connector must add its page under docs/en/connectors/")
    if new_modules and not has_e2e_test:
        warn("-", "no-e2e", "new connector has no added E2E test method; confirm that is unavoidable")
    if wide_units:
        wide_modules = {u.split(":", 1)[1] for u in wide_units}
        if not cfg.wide_checks:
            block("-", "wide-blast", "shared module %s changed but AI_MERGE_WIDE_CHECKS is empty: CI only runs the "
                  "changed module, nothing proves its dependents still work" % ", ".join(sorted(wide_modules)))
        if wide_modules - test_modules:
            block("-", "wide-needs-module-test", "a shared module change needs a test inside that same module: %s"
                  % ", ".join(sorted(wide_modules - test_modules)))

    if new_modules:
        pr_class = "new-connector"
    elif wide_units:
        pr_class = "shared"
    elif mains:
        pr_class = "connector"
    elif tests:
        pr_class = "tests"
    else:
        pr_class = "docs"
    caps = CAPS[pr_class]
    if len(files) > caps["files"]:
        block("-", "size", "%d files exceed the %s cap of %d" % (len(files), pr_class, caps["files"]))
    if total_lines > caps["lines"]:
        block("-", "size", "%d changed lines exceed the %s cap of %d" % (total_lines, pr_class, caps["lines"]))
    if "main_code_lines" in caps and main_code_lines > caps["main_code_lines"]:
        block("-", "size", "%d production code lines exceed the %s cap of %d"
              % (main_code_lines, pr_class, caps["main_code_lines"]))

    return {
        "eligible": not blockers,
        "class": pr_class,
        "wide": bool(wide_units),
        "units": sorted(units | new_modules),
        "blockers": blockers,
        "warnings": warnings,
        "stats": {"files": len(files), "lines": total_lines, "main_code_lines": main_code_lines},
    }


def required_checks(result, cfg):
    """Check-run names that must be green for this PR: base set plus the class-specific proof."""
    names = list(cfg.required_checks)
    if result["class"] == "new-connector":
        names += cfg.new_connector_checks
    if result["wide"]:
        names += cfg.wide_checks
    return names


def _check_pom(path, status, added, removed, new_modules, new_e2e, known_artifacts, block):
    group_pom = GROUP_POM_RE.match(path) and status == "modified"
    if group_pom:
        _check_registration(path, "module", added, removed, new_modules, new_e2e, block)
        return
    if status != "added":
        block(path, "path", "pom.xml changes are only accepted when they define or register a brand-new "
              "connector module in the same PR")
        return
    own = path.split("/")[-2]
    for text in added:
        if POM_DANGER_RE.search(text):
            block(path, "pom-build-logic", "new module pom declares repositories, profiles, antrun/exec or "
                  "system scope: %s" % text.strip())
        version = VERSION_LINE_RE.match(text)
        if version and not version.group(1).startswith("${"):
            block(path, "pom-version", "literal version %s pins a dependency outside dependencyManagement" %
                  version.group(1))
        artifact = ARTIFACT_LINE_RE.match(text)
        if artifact:
            name = artifact.group(1)
            if name == own or name.startswith(("connector-", "seatunnel-")) or name == "connectors-v2":
                continue
            if known_artifacts is None or name not in known_artifacts:
                block(path, "new-third-party-dependency",
                      "artifact %s is not used by any existing pom: a new jar needs a license and supply-chain "
                      "decision by a human" % name)


def _check_registration(path, kind, added, removed, new_modules, new_e2e, block):
    if not new_modules and not new_e2e:
        block(path, "path", "registration and distribution files are only accepted when the same PR adds a "
              "brand-new connector module")
        return
    if any(x.strip() for x in removed):
        block(path, "registration", "registration files must be purely additive")
        return
    allowed = new_e2e if path.startswith(E2E_ROOT) else new_modules
    for text in (x for x in added if x.strip()):
        ok = False
        if kind == "module":
            match = MODULE_LINE_RE.match(text)
            ok = bool(match) and match.group(1) in allowed
        elif kind == "dist":
            artifact = ARTIFACT_LINE_RE.match(text)
            ok = (artifact.group(1) in new_modules) if artifact else any(r.match(text) for r in DIST_LINE_RES)
        elif kind == "mapping":
            match = MAPPING_LINE_RE.match(text)
            ok = text.lstrip().startswith("#") or (bool(match) and match.group(2) in new_modules)
        elif kind == "labeler":
            names = [m[len("connector-"):] for m in new_modules] + sorted(new_e2e)
            ok = bool(LABELER_STRUCTURE_RE.match(text)) or any(n in text for n in names)
        elif kind == "plugin_config":
            match = PLUGIN_CONFIG_LINE_RE.match(text)
            ok = text.lstrip().startswith("#") or (bool(match) and match.group(1) in new_modules)
        if not ok:
            block(path, "registration", "added line is not a plain registration of the new module: %s" % text.strip())
            return


def _check_services(path, status, added, removed, added_paths, new_modules, module, block):
    if module not in new_modules:
        block(path, "path", "SPI service registration is only accepted for a brand-new connector module")
        return
    if any(x.strip() for x in removed):
        block(path, "registration", "service files must be purely additive")
        return
    root = path.split("/src/main/resources/")[0]
    for text in (x.strip() for x in added):
        if not text or text.startswith("#"):
            continue
        expected = "%s/src/main/java/%s.java" % (root, text.replace(".", "/"))
        if expected not in added_paths:
            block(path, "registration", "service entry %s does not name a class added by this PR" % text)


def _check_main_file(path, status, added, removed, block, warn, relaxed=False, wide=False):
    """`relaxed` is true only for files of a brand-new module: there is no old behavior, state or option
    contract to break, so contract rules become warnings. Process-level calls stay blocked."""
    if not path.endswith(".java"):
        return
    if CHECKPOINT_FILE_RE.search(path):
        (warn if relaxed else block)(path, "checkpoint-state",
                                     "checkpoint / split / commit-info / serializer class defines a persisted format")
    if not relaxed:
        for text in added + removed:
            if SERIAL_RE.search(text):
                block(path, "serialization", "serialVersionUID touched")
                break
        if OPTION_FILE_RE.search(path):
            if status == "added" and path.endswith("Factory.java"):
                block(path, "new-factory", "new factory registration")
            for text in removed:
                if OPTION_REMOVED_RE.search(text):
                    block(path, "option-contract",
                          "option / default / factory definition removed or changed: %s" % text.strip())
                    break
    if wide:
        for text in removed:
            if SIGNATURE_RE.match(text):
                block(path, "shared-signature", "public/protected signature of a shared module removed or changed: %s"
                      % text.strip())
                break
    for name, regex, detail in (
        ("concurrency", CONCURRENCY_RE, "threading / locking / async primitive touched"),
        ("security", SECURITY_RE, "security-sensitive identifier touched"),
        ("classloading", CLASSLOAD_RE, "reflection / classloading / process-level API touched"),
    ):
        for text in added + removed:
            if regex.search(text):
                hard = not relaxed or (name == "classloading" and NEW_CODE_BLOCK_RE.search(text))
                (block if hard else warn)(path, name, "%s: %s" % (detail, text.strip()))
                break
    if HOT_PATH_FILE_RE.search(path):
        warn(path, "hot-path", "per-row reader/writer/serialization path: justify allocation, locking and complexity")
    if TYPE_MAPPING_FILE_RE.search(path):
        warn(path, "type-mapping", "type mapping / converter change can alter output data semantics for existing jobs")
    if any(EMPTY_CATCH_RE.search(x) for x in added):
        warn(path, "empty-catch", "added an empty catch block (error swallowing)")
    if sum(1 for x in removed if THROW_RE.search(x)) > sum(1 for x in added if THROW_RE.search(x)):
        warn(path, "removed-throw", "fewer throw statements: confirm this is not a silent degradation")


def _check_test_file(path, status, added, removed, block, warn):
    for text in removed:
        if ASSERT_RE.search(text):
            block(path, "test-weakening", "assertion or test method removed/changed: %s" % text.strip())
            break
    if status != "added":
        for text in added:
            if DISABLE_RE.search(text):
                block(path, "test-weakening", "disables or conditionalises an existing test: %s" % text.strip())
                break
        for text in removed:
            if TIMING_RE.search(text):
                block(path, "test-weakening", "timeout / retry / tolerance changed in an existing test: %s" % text.strip())
                break
        if path.endswith(TEST_RESOURCE_EXT) and removed:
            block(path, "test-weakening", "existing test fixture lines removed (could narrow data coverage)")


def render_report(result, head_sha):
    verdict = "ELIGIBLE" if result["eligible"] else "NOT ELIGIBLE"
    lines = [
        "<!-- %s sha=%s result=%s class=%s -->"
        % (MARKER, head_sha, "eligible" if result["eligible"] else "ineligible", result["class"]),
        "### ai-merge mechanical eligibility: %s" % verdict,
        "",
        "Head commit: `%s`. Class: `%s`%s. Units: %s. Files: %d, changed lines: %d, production code lines: %d."
        % (head_sha, result["class"], " (wide blast radius)" if result["wide"] else "",
           ", ".join(result["units"]) or "-", result["stats"]["files"],
           result["stats"]["lines"], result["stats"]["main_code_lines"]),
        "",
    ]
    if result["blockers"]:
        lines.append("Blockers:")
        for b in result["blockers"]:
            lines.append("- `%s` [%s] %s" % (b["file"], b["rule"], b["detail"]))
        lines.append("")
    if result["warnings"]:
        lines.append("Warnings (the AI verdict must address each one before the label is applied):")
        for w in result["warnings"]:
            lines.append("- `%s` [%s] %s" % (w["file"], w["rule"], w["detail"]))
        lines.append("")
    lines.append("This is the mechanical half of the check. The label also needs the AI review verdict and the "
                 "approver's approval of this exact head commit; any new push invalidates everything.")
    return "\n".join(lines)


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
            "User-Agent": "seatunnel-ai-merge-gate",
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

    def paginate(self, path, key=None):
        items, page = [], 1
        sep = "&" if "?" in path else "?"
        while True:
            data = self.get("%s%sper_page=100&page=%d" % (path, sep, page))
            batch = data[key] if key else data
            items.extend(batch)
            if len(batch) < 100:
                return items
            page += 1

    def graphql(self, query, variables):
        status, data = self.call("POST", "/graphql", {"query": query, "variables": variables})
        if status >= 300 or (data or {}).get("errors"):
            raise ApiError("graphql failed: %s %s" % (status, data))
        return data["data"]


def pr_view(pr):
    return {
        "title": pr.get("title"),
        "body": pr.get("body"),
        "draft": pr.get("draft"),
        "base": pr["base"]["ref"],
        "author_association": pr.get("author_association"),
    }


def evaluate_pr(gh, number, cfg):
    pr = gh.get("/repos/%s/pulls/%d" % (gh.repo, number))
    files = gh.paginate("/repos/%s/pulls/%d/files" % (gh.repo, number))
    # The workflow checks out the base branch, so these are the artifacts that are already vetted.
    known = load_known_artifacts(os.environ.get("AI_MERGE_REPO_ROOT", "."))
    return pr, evaluate(pr_view(pr), files, cfg, known)


def upsert_report(gh, number, body):
    comments = gh.paginate("/repos/%s/issues/%d/comments" % (gh.repo, number))
    for c in comments:
        if "<!-- %s " % MARKER in (c.get("body") or "") and c["user"]["type"] == "Bot":
            gh.call("PATCH", "/repos/%s/issues/comments/%d" % (gh.repo, c["id"]), {"body": body})
            return
    gh.call("POST", "/repos/%s/issues/%d/comments" % (gh.repo, number), {"body": body})


def remove_label(gh, number):
    gh.call("DELETE", "/repos/%s/issues/%d/labels/%s" % (gh.repo, number, LABEL))


# ------------------------------------------------------------------------------------------
# Merge gate
# ------------------------------------------------------------------------------------------

FAILED_CONCLUSIONS = {"failure", "cancelled", "timed_out", "action_required", "stale", "startup_failure"}


def run_gate(gh, number, cfg):
    """Return dict(ok, pending, reasons, head, result). `ok` means safe to merge right now."""
    reasons, pending = [], []
    pr, result = evaluate_pr(gh, number, cfg)
    head = pr["head"]["sha"]
    author = pr["user"]["login"]

    if pr["state"] != "open":
        reasons.append("PR is not open")
    if LABEL not in [x["name"] for x in pr["labels"]]:
        reasons.append("label %s is not present" % LABEL)
    if not result["eligible"]:
        reasons.append("mechanical eligibility failed: %d blocker(s), see evaluate report" % len(result["blockers"]))

    # Label provenance: applied by an allowed actor, not the author, and after the head commit was pushed.
    events = gh.paginate("/repos/%s/issues/%d/events" % (gh.repo, number))
    labeled = [e for e in events if e["event"] == "labeled" and e["label"]["name"] == LABEL]
    if not cfg.label_actors:
        reasons.append("AI_MERGE_LABEL_ACTORS is not configured (fail closed)")
    elif not labeled:
        reasons.append("no labeled event found")
    else:
        last = labeled[-1]
        actor = last["actor"]["login"]
        if actor not in cfg.label_actors:
            reasons.append("label applied by %s who is not an allowed label actor" % actor)
        if actor == author:
            reasons.append("label applied by the PR author")
        suites = gh.paginate("/repos/%s/commits/%s/check-suites" % (gh.repo, head), "check_suites")
        if not suites:
            reasons.append("no check suite for the head commit yet")
        elif last["created_at"] <= min(s["created_at"] for s in suites):
            reasons.append("label predates the current head commit (stale label)")

    # Approval: latest meaningful review per user, bound to the head SHA.
    reviews = gh.paginate("/repos/%s/pulls/%d/reviews" % (gh.repo, number))
    latest = {}
    for r in sorted(reviews, key=lambda x: x.get("submitted_at") or ""):
        if r["state"] in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED"):
            latest[r["user"]["login"]] = r
    if not cfg.approvers:
        reasons.append("AI_MERGE_APPROVERS is not configured (fail closed)")
    elif not any(u in latest and latest[u]["state"] == "APPROVED" and latest[u]["commit_id"] == head
                 and u != author for u in cfg.approvers):
        reasons.append("no APPROVED review by %s on head %s" % ("/".join(cfg.approvers), head[:10]))
    for user, r in latest.items():
        if r["state"] == "CHANGES_REQUESTED":
            reasons.append("changes requested by %s" % user)

    # CI: every check run completed on this exact SHA, none failed, required ones succeeded.
    runs = gh.paginate("/repos/%s/commits/%s/check-runs" % (gh.repo, head), "check_runs")
    by_name = {}
    for run in runs:
        by_name.setdefault(run["name"], []).append(run)
        if run["status"] != "completed":
            pending.append("check %s is %s" % (run["name"], run["status"]))
        elif run["conclusion"] in FAILED_CONCLUSIONS:
            reasons.append("check %s concluded %s" % (run["name"], run["conclusion"]))
    for name in required_checks(result, cfg):
        if not any(r["status"] == "completed" and r["conclusion"] == "success" for r in by_name.get(name, [])):
            (pending if name not in by_name else reasons).append("required check %s is not green" % name)
    combined = gh.get("/repos/%s/commits/%s/status" % (gh.repo, head))
    if combined["total_count"] and combined["state"] in ("failure", "error"):
        reasons.append("commit status is %s" % combined["state"])
    elif combined["total_count"] and combined["state"] == "pending":
        pending.append("commit status is pending")

    # Mergeability and review threads.
    if pr.get("draft"):
        reasons.append("PR is a draft")
    state = pr.get("mergeable_state")
    if state == "dirty":
        reasons.append("merge conflict with %s" % pr["base"]["ref"])
    elif state in (None, "unknown"):
        pending.append("mergeability not computed yet")
    elif state != "clean" and not pending and not reasons:
        reasons.append("mergeable_state is %s, expected clean" % state)
    owner, name = gh.repo.split("/")
    threads = gh.graphql(
        "query($o:String!,$r:String!,$n:Int!){repository(owner:$o,name:$r){pullRequest(number:$n){"
        "reviewThreads(first:100){totalCount nodes{isResolved}}}}}", {"o": owner, "r": name, "n": number},
    )["repository"]["pullRequest"]["reviewThreads"]
    if threads["totalCount"] > 100:
        reasons.append("more than 100 review threads, cannot verify they are resolved")
    elif any(not t["isResolved"] for t in threads["nodes"]):
        reasons.append("unresolved review threads")

    return {"ok": not reasons and not pending, "pending": pending, "reasons": reasons, "head": head, "result": result}


def merge_pr(gh, number, head, cfg):
    if not cfg.merge_enabled:
        print("DRY RUN: would squash-merge PR #%d at %s (AI_MERGE_ENABLED is not true)" % (number, head))
        return True
    # `sha` pins the merge to the evaluated head; GitHub rejects it with 409 if the head moved meanwhile.
    status, data = gh.call("PUT", "/repos/%s/pulls/%d/merge" % (gh.repo, number),
                           {"sha": head, "merge_method": "squash"})
    print("merge PR #%d -> %s %s" % (number, status, (data or {}).get("message")))
    return status == 200


def cmd_gate(gh, number, cfg, merge):
    outcome = run_gate(gh, number, cfg)
    print("PR #%d head %s: %s" % (number, outcome["head"][:10],
                                  "READY" if outcome["ok"] else ("PENDING" if not outcome["reasons"] else "BLOCKED")))
    for item in outcome["reasons"] + outcome["pending"]:
        print("  - " + item)
    if outcome["ok"] and merge:
        return 0 if merge_pr(gh, number, outcome["head"], cfg) else 1
    return 0 if outcome["ok"] else 1


def cmd_enforce(gh, cfg):
    with open(os.environ["GITHUB_EVENT_PATH"]) as fh:
        event = json.load(fh)
    action = event.get("action")
    number = event["pull_request"]["number"]
    labels = [x["name"] for x in event["pull_request"]["labels"]]
    if action == "synchronize" and LABEL in labels:
        remove_label(gh, number)
        gh.call("POST", "/repos/%s/issues/%d/comments" % (gh.repo, number), {
            "body": "The `%s` label was removed because new commits were pushed. It must be re-evaluated and "
                    "re-applied for the new head commit." % LABEL})
        print("label removed after synchronize")
        return 0
    if action == "labeled" and event.get("label", {}).get("name") == LABEL:
        actor = event["sender"]["login"]
        author = event["pull_request"]["user"]["login"]
        pr, result = evaluate_pr(gh, number, cfg)
        upsert_report(gh, number, render_report(result, pr["head"]["sha"]))
        if actor not in cfg.label_actors or actor == author or not result["eligible"]:
            remove_label(gh, number)
            why = "label actor not allowed" if (actor not in cfg.label_actors or actor == author) else "see report"
            gh.call("POST", "/repos/%s/issues/%d/comments" % (gh.repo, number), {
                "body": "The `%s` label was removed: %s." % (LABEL, why)})
            print("label removed: " + why)
            return 1
    return 0


def cmd_sweep(gh, cfg, merge):
    issues = gh.paginate("/repos/%s/issues?labels=%s&state=open" % (gh.repo, LABEL))
    failed = 0
    for issue in issues:
        if "pull_request" in issue:
            failed += cmd_gate(gh, issue["number"], cfg, merge) != 0
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="ai-merge label gatekeeper")
    parser.add_argument("command", choices=["evaluate", "gate", "enforce", "sweep", "classify"])
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--pr", type=int)
    parser.add_argument("--comment", action="store_true", help="evaluate: upsert the report comment")
    parser.add_argument("--merge", action="store_true", help="gate/sweep: squash merge when READY")
    parser.add_argument("--files-json", help="classify: saved pulls/N/files payload")
    parser.add_argument("--title", default="")
    parser.add_argument("--body", default="")
    parser.add_argument("--author-association", default="MEMBER")
    args = parser.parse_args(argv)
    cfg = Config()

    if args.command == "classify":
        with open(args.files_json) as fh:
            files = json.load(fh)
        pr = {"title": args.title, "body": args.body, "draft": False, "base": cfg.base,
              "author_association": args.author_association}
        known = load_known_artifacts(os.environ.get("AI_MERGE_REPO_ROOT", "."))
        result = evaluate(pr, files, cfg, known)
        print(json.dumps(result, indent=2))
        return 0 if result["eligible"] else 1

    gh = GitHub(args.repo, os.environ["GITHUB_TOKEN"])
    if args.command == "evaluate":
        pr, result = evaluate_pr(gh, args.pr, cfg)
        report = render_report(result, pr["head"]["sha"])
        print(report)
        if args.comment:
            upsert_report(gh, args.pr, report)
        return 0 if result["eligible"] else 1
    if args.command == "gate":
        return cmd_gate(gh, args.pr, cfg, args.merge)
    if args.command == "enforce":
        return cmd_enforce(gh, cfg)
    return cmd_sweep(gh, cfg, args.merge)


if __name__ == "__main__":
    sys.exit(main())
