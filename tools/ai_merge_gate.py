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

Three PR classes can carry the label (everything else is rejected, the allow-list is closed):
  docs       only documentation (*.md, docs/** images)
  tests      only connector tests / connector E2E (src/test, seatunnel-connector-v2-e2e/connector-*-e2e)
  connector  a bug fix or small improvement inside exactly ONE connector blast-radius unit, plus
             its own tests/docs. A unit is a connector module, or one dialect of connector-jdbc.

Anything touching engine/api/core/common/config/formats/transforms/translation/dist, any pom.xml,
.github/**, tools/**, registration files, shared connector bases, checkpoint/state/serialization
classes, option removals/default changes, security-sensitive code, concurrency, classloading,
test weakening or file deletion/rename is rejected.

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
    (r"(^|/)pom\.xml$", "pom.xml / dependency / build change needs explicit owner consent and supply-chain review"),
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
}


class Config(object):
    """Runtime policy knobs; defaults fail closed for identities."""

    def __init__(self, env=None):
        env = os.environ if env is None else env
        self.base = env.get("AI_MERGE_BASE", "dev")
        self.label_actors = _csv(env.get("AI_MERGE_LABEL_ACTORS", ""))
        self.approvers = _csv(env.get("AI_MERGE_APPROVERS", "DanielLeens"))
        self.required_checks = _csv(env.get("AI_MERGE_REQUIRED_CHECKS", "Build"))
        self.trusted_associations = set(TRUSTED_ASSOCIATIONS)
        self.merge_enabled = env.get("AI_MERGE_ENABLED", "false").lower() == "true"


def _csv(value):
    return [x.strip() for x in value.split(",") if x.strip()]


# ------------------------------------------------------------------------------------------
# Pure classification (no network): this is what the unit tests exercise
# ------------------------------------------------------------------------------------------


def classify_path(path):
    """Return (category, module, unit, reason). category in doc|test|main|blocked."""
    if DOC_RE.search(path) and not path.startswith(".github/"):
        return "doc", None, None, None
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
        parts = path[len(CONNECTORS_ROOT):].split("/")
        if len(parts) < 2:
            return "blocked", None, None, "file at connectors-v2 root"
        idx = 0
        # Grouping directories (connector-cdc, connector-file, connector-http, ...) hold the real modules
        # as `connector-*` children; a plain module has `src` as its first child instead.
        if len(parts) > 2 and parts[1].startswith("connector-"):
            idx = 1
        module = parts[idx]
        sub = "/".join(parts[idx + 1:])
        if sub.startswith("src/test/"):
            return "test", module, None, None
        if not sub.startswith("src/main/"):
            return "blocked", None, None, "unexpected file in connector module"
        if module in SHARED_CONNECTOR_MODULES or SHARED_MODULE_SUFFIX.search(module):
            return "blocked", None, None, "shared connector base or cross-suite fixture (large blast radius)"
        if module == "connector-jdbc":
            match = JDBC_DIALECT_RE.search("/" + sub)
            if not match or match.group(1) in JDBC_SHARED_DIRS:
                return "blocked", None, None, "shared JDBC core (affects every dialect)"
            return "main", module, "connector-jdbc:" + match.group(1), None
        return "main", module, module, None

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


def evaluate(pr, files, cfg=None):
    """Pure eligibility evaluation. `pr` needs title/body/draft/base/author_association."""
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

    mains, tests, docs = [], [], []
    units, test_modules = set(), set()
    main_modules = set()
    total_lines = main_code_lines = 0
    new_option_files, added_test_annotation = [], False

    for f in files:
        path = f["filename"]
        status = f.get("status", "modified")
        patch = f.get("patch")
        lines = f.get("additions", 0) + f.get("deletions", 0)
        total_lines += lines
        category, module, unit, reason = classify_path(path)
        java = path.endswith(".java")
        added, removed = changed_lines(patch)

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
        elif category == "test":
            tests.append(path)
            test_modules.add(module)
            _check_test_file(path, status, added, removed, block, warn)
            if any(TEST_ANNOTATION_RE.search(x) for x in added):
                added_test_annotation = True
        elif category == "main":
            mains.append(path)
            units.add(unit)
            main_modules.add(module)
            code = sum(1 for x in added + removed if is_code_line(x, java))
            main_code_lines += code
            _check_main_file(path, status, added, removed, block, warn)
            if java and OPTION_FILE_RE.search(path) and any(OPTION_ADDED_RE.search(x) for x in added):
                new_option_files.append(path)

    if len(units) > 1:
        block("-", "multi-unit", "touches more than one connector blast-radius unit: %s" % ", ".join(sorted(units)))
    if mains and test_modules - main_modules - {None}:
        # Tests may only come from the unit under change; E2E for another connector is unrelated evidence.
        block("-", "foreign-tests", "tests of other modules mixed into a connector change: %s"
              % ", ".join(sorted(test_modules - main_modules)))
    if main_code_lines > 0 and not added_test_annotation:
        block("-", "no-regression-test", "production code changed but no test method was added or changed")
    if new_option_files and not docs:
        block(new_option_files[0], "option-without-docs", "new option added but no documentation changed")

    pr_class = "connector" if mains else ("tests" if tests else "docs")
    caps = CAPS[pr_class]
    if len(files) > caps["files"]:
        block("-", "size", "%d files exceed the %s cap of %d" % (len(files), pr_class, caps["files"]))
    if total_lines > caps["lines"]:
        block("-", "size", "%d changed lines exceed the %s cap of %d" % (total_lines, pr_class, caps["lines"]))
    if pr_class == "connector" and main_code_lines > caps["main_code_lines"]:
        block("-", "size", "%d production code lines exceed the cap of %d" % (main_code_lines, caps["main_code_lines"]))

    return {
        "eligible": not blockers,
        "class": pr_class,
        "units": sorted(units),
        "blockers": blockers,
        "warnings": warnings,
        "stats": {"files": len(files), "lines": total_lines, "main_code_lines": main_code_lines},
    }


def _check_main_file(path, status, added, removed, block, warn):
    if not path.endswith(".java"):
        return
    if CHECKPOINT_FILE_RE.search(path):
        block(path, "checkpoint-state", "checkpoint / split / commit-info / serializer class defines a persisted format")
    for text in added + removed:
        if SERIAL_RE.search(text):
            block(path, "serialization", "serialVersionUID touched")
            break
    if OPTION_FILE_RE.search(path):
        if status == "added" and path.endswith("Factory.java"):
            block(path, "new-factory", "new factory registration")
        for text in removed:
            if OPTION_REMOVED_RE.search(text):
                block(path, "option-contract", "option / default / factory definition removed or changed: %s" % text.strip())
                break
    for name, regex, detail in (
        ("concurrency", CONCURRENCY_RE, "threading / locking / async primitive touched"),
        ("security", SECURITY_RE, "security-sensitive identifier touched"),
        ("classloading", CLASSLOAD_RE, "reflection / classloading / process-level API touched"),
    ):
        for text in added + removed:
            if regex.search(text):
                block(path, name, "%s: %s" % (detail, text.strip()))
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
        "Head commit: `%s`. Class: `%s`. Units: %s. Files: %d, changed lines: %d, production code lines: %d."
        % (head_sha, result["class"], ", ".join(result["units"]) or "-", result["stats"]["files"],
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
    return pr, evaluate(pr_view(pr), files, cfg)


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
    for name in cfg.required_checks:
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
        result = evaluate(pr, files, cfg)
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
