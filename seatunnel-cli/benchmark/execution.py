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

"""Layered execution checks for generated configs.

Three verdict layers (each is a strict gate on top of the previous):
  L1 static  — benchmark.scoring (pyhocon + metadata + regex assertions)
  L2 dry-run — engine-side config validation without running the job.
  L3 execute — real job execution against the Docker data environment.
               BATCH: exit code 0 = pass. STREAMING: job stays alive for
               STREAM_ALIVE_SECONDS without erroring = pass (then killed).
               Optional per-task ``verify`` probes sink row counts.

Engine backends (auto-selected):
  1. Local dist — SEATUNNEL_HOME points at a built distribution;
     L2 uses ``seatunnel.sh --dry-run static`` (PR #10763).
  2. Docker     — no local dist needed. Runs ``apache/seatunnel:<tag>``
     (ST_BENCH_IMAGE, default apache/seatunnel:latest) attached to the
     benchmark compose network. L2 uses ``--check`` (the released-image
     equivalent of dry-run static). Task prompts reference ``localhost``
     endpoints; inside the container network those become the compose
     service hostnames, so configs are rewritten host→service before
     execution (deterministic textual mapping, applied identically for
     every model).

Credentials for the Docker environment are injected as ``-i KEY=VALUE``
variables so configs using ``${MYSQL_PASSWORD}``-style placeholders resolve
exactly like production usage.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

# Standard credentials of the benchmark Docker environment.
# Keep in sync with benchmark/docker/docker-compose.yml.
CREDENTIALS = {
    "MYSQL_USER": "root",
    "MYSQL_PASSWORD": "Test@123",
    "PG_USER": "bench",
    "PG_PASSWORD": "Test@123",
    "POSTGRES_USER": "bench",
    "POSTGRES_PASSWORD": "Test@123",
    "CLICKHOUSE_USER": "default",
    "CLICKHOUSE_PASSWORD": "Test@123",
    "ES_USER": "",
    "ES_PASSWORD": "",
    "DORIS_USER": "root",
    "DORIS_PASSWORD": "",
    "STARROCKS_USER": "root",
    "STARROCKS_PASSWORD": "",
    "AWS_ACCESS_KEY": "minioadmin",
    "AWS_SECRET_KEY": "minioadmin",
    "ACCESS_KEY": "minioadmin",
    "SECRET_KEY": "minioadmin",
    "PASSWORD": "Test@123",
    "DB_PASSWORD": "Test@123",
    "USER": "root",
    "DB_USER": "root",
    "KAFKA_PASSWORD": "",
    "AUTH_TOKEN": "",
}

DRY_RUN_TIMEOUT = 180
BATCH_RUN_TIMEOUT = 300
STREAM_ALIVE_SECONDS = 60

DOCKER_IMAGE = os.environ.get("ST_BENCH_IMAGE", "apache/seatunnel:latest")
DOCKER_NETWORK = os.environ.get("ST_BENCH_NETWORK", "docker_default")

# Task prompts use localhost:port endpoints (real-usage style). Inside the
# compose network the services live under their service hostnames. This is a
# deterministic infrastructure mapping applied identically to every model's
# config — not a correction of model output.
_HOST_REWRITES = [
    (re.compile(r"(?<![\w.])(localhost|127\.0\.0\.1):3306"), "mysql:3306"),
    (re.compile(r"(?<![\w.])(localhost|127\.0\.0\.1):5432"), "postgres:5432"),
    (re.compile(r"(?<![\w.])(localhost|127\.0\.0\.1):9092"), "kafka:19092"),
    (re.compile(r"(?<![\w.])(localhost|127\.0\.0\.1):8123"), "clickhouse:8123"),
    (re.compile(r"(?<![\w.])(localhost|127\.0\.0\.1):9200"), "elasticsearch:9200"),
    (re.compile(r"(?<![\w.])(localhost|127\.0\.0\.1):9000"), "minio:9000"),
    (re.compile(r"(?<![\w.])(localhost|127\.0\.0\.1):8030"), "doris:8030"),
    (re.compile(r"(?<![\w.])(localhost|127\.0\.0\.1):9030"), "doris:9030"),
    (re.compile(r"(?<![\w.])(localhost|127\.0\.0\.1):8031"), "starrocks:8030"),
    (re.compile(r"(?<![\w.])(localhost|127\.0\.0\.1):9031"), "starrocks:9030"),
]

# Patterns in engine output that mean a streaming job actually failed even
# though the process may still be running.
_FAILURE_PATTERNS = re.compile(
    r"Job .*(FAILED|FAILING)|SeaTunnelRuntimeException|"
    r"ErrorCode:\[|Exception in thread|JobExecutionException",
)

_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def find_seatunnel_sh() -> str | None:
    st_home = os.environ.get("SEATUNNEL_HOME", "").strip()
    if st_home:
        path = Path(st_home) / "bin" / "seatunnel.sh"
        if path.exists():
            return str(path)
    return None


def docker_engine_available() -> bool:
    """True if docker daemon works and the SeaTunnel image is present locally."""
    try:
        proc = subprocess.run(
            ["docker", "image", "inspect", DOCKER_IMAGE],
            capture_output=True, timeout=30,
        )
        return proc.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def engine_backend() -> str | None:
    """'local' (SEATUNNEL_HOME dist), 'docker', or None."""
    if find_seatunnel_sh():
        return "local"
    if docker_engine_available():
        return "docker"
    return None


def rewrite_hosts_for_docker(config: str) -> str:
    """Map localhost endpoints to compose service hostnames."""
    for pattern, replacement in _HOST_REWRITES:
        config = pattern.sub(replacement, config)
    return config


# Host-side port remaps for services whose compose host port differs from
# the in-network port (docker-compose.yml remaps them to avoid clashes
# with services commonly present on developer machines). Tasks/prompts use
# the canonical in-network ports; the LOCAL engine backend connects from
# the host, so those endpoints must be rewritten to the remapped ports.
_LOCAL_PORT_REWRITES = [
    (re.compile(r"localhost:5432\b"), "localhost:15432"),   # postgres
]


def rewrite_ports_for_local(config: str) -> str:
    """Map canonical service ports to the compose host-side remapped ports."""
    for pattern, replacement in _LOCAL_PORT_REWRITES:
        config = pattern.sub(replacement, config)
    return config


def _variable_args(config: str) -> list[str]:
    """Build -i KEY=VALUE args for every ${VAR} the config references."""
    args = []
    for var in sorted(set(_ENV_VAR_RE.findall(config))):
        value = CREDENTIALS.get(var, os.environ.get(var))
        if value is not None:
            args += ["-i", f"{var}={value}"]
    return args


def _write_conf(config: str) -> str:
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".conf", prefix="st_bench_", delete=False,
        dir="/tmp",
    ) as tmp:
        tmp.write(config)
        return tmp.name


def prepare_setup_files(task: dict) -> None:
    """Create local input files a task declares (e.g. CSV sources)."""
    for path_str, content in task.get("execution", {}).get("setup_files", {}).items():
        path = Path(path_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def _ensure_empty_jar() -> str:
    path = "/tmp/st_bench_empty.jar"
    if not os.path.exists(path):
        Path(path).touch()
    return path


def _docker_cmd(container_name: str, inner: list[str],
                mounts: list[str] | None = None) -> list[str]:
    """Build a docker run command on the benchmark network.

    /tmp is shared so generated .conf files, task setup_files, and file-sink
    outputs (/tmp/st_bench/...) are visible on both sides.

    The released image ships opengauss-jdbc-5.1.0.jar in /opt/seatunnel/lib,
    which hijacks the org.postgresql.Driver class and breaks every real
    PostgreSQL connection ("Protocol error. Session setup failed"). We shadow
    it with an empty file — an image defect workaround applied identically
    for every model.
    """
    cmd = ["docker", "run", "--rm", "--name", container_name,
           "--network", DOCKER_NETWORK, "-v", "/tmp:/tmp",
           "-v", f"{_ensure_empty_jar()}:/opt/seatunnel/lib/opengauss-jdbc-5.1.0.jar:ro"]
    for m in mounts or []:
        cmd += ["-v", m]
    cmd.append(DOCKER_IMAGE)
    cmd += inner
    return cmd


def run_dry_run(config: str) -> dict:
    """L2: engine config validation without running the job."""
    backend = engine_backend()
    if backend is None:
        return {"passed": None, "detail": "SKIPPED: no engine (SEATUNNEL_HOME/docker)",
                "seconds": 0.0}

    if backend == "docker":
        # Released images (2.3.x) predate PR #10763: their --check command is
        # an empty TODO stub that exits 0 for any config (verified against
        # apache/seatunnel:latest with an invalid connector). Running it would
        # report a fake 100% L2 pass rate, so we skip the layer honestly.
        # A dev-branch image (with --dry-run static) re-enables L2 here.
        return {"passed": None,
                "detail": "SKIPPED: released image --check is a no-op "
                          "(--dry-run static requires a dev-branch build)",
                "seconds": 0.0}

    conf_path = _write_conf(config)
    cmd = ["sh", find_seatunnel_sh(), "--dry-run", "static",
           "--config", conf_path] + _variable_args(config)

    start = time.monotonic()
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=DRY_RUN_TIMEOUT,
        )
        seconds = round(time.monotonic() - start, 2)
        if proc.returncode == 0:
            return {"passed": True, "detail": "PASS", "seconds": seconds}
        tail = (proc.stderr.strip() or proc.stdout.strip())[-1500:]
        return {"passed": False, "detail": tail, "seconds": seconds}
    except subprocess.TimeoutExpired:
        return {"passed": False, "detail": f"TIMEOUT after {DRY_RUN_TIMEOUT}s",
                "seconds": round(time.monotonic() - start, 2)}
    finally:
        try:
            os.unlink(conf_path)
        except OSError:
            pass


def run_execute(config: str, task: dict) -> dict:
    """L3: real local-mode execution. Returns {passed, detail, seconds}."""
    execution = task.get("execution", {})
    if execution.get("l3") == "skip":
        return {"passed": None,
                "detail": f"SKIPPED: {execution.get('l3_skip_reason', 'marked skip')}",
                "seconds": 0.0}

    backend = engine_backend()
    if backend is None:
        return {"passed": None, "detail": "SKIPPED: no engine (SEATUNNEL_HOME/docker)",
                "seconds": 0.0}

    services = execution.get("services", [])
    up, detail = check_services_up(services)
    if not up:
        return {"passed": None, "detail": f"SKIPPED: {detail}", "seconds": 0.0}

    prepare_setup_files(task)
    if backend == "docker":
        config = rewrite_hosts_for_docker(config)
    else:
        config = rewrite_ports_for_local(config)
    conf_path = _write_conf(config)

    container_name = None
    if backend == "local":
        cmd = ["sh", find_seatunnel_sh(), "--config", conf_path,
               "-m", "local"] + _variable_args(config)
    else:
        container_name = f"st-bench-run-{uuid.uuid4().hex[:8]}"
        inner = ["/opt/seatunnel/bin/seatunnel.sh", "--config", conf_path,
                 "-m", "local"] + _variable_args(config)
        cmd = _docker_cmd(container_name, inner)

    mode = execution.get("mode", "batch")
    start = time.monotonic()
    try:
        if mode == "streaming":
            result = _run_streaming(cmd, container_name)
        else:
            result = _run_batch(cmd)
        result["seconds"] = round(time.monotonic() - start, 2)

        # Optional sink-side verification (batch only, after success)
        if result["passed"] and execution.get("verify"):
            verify_result = _verify_sink(execution["verify"])
            if verify_result["passed"] is False:
                result["passed"] = False
                result["detail"] = f"job OK but verify failed: {verify_result['detail']}"
            elif verify_result["passed"]:
                result["detail"] += f"; verify: {verify_result['detail']}"
        return result
    finally:
        try:
            os.unlink(conf_path)
        except OSError:
            pass


def _run_batch(cmd: list[str]) -> dict:
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=BATCH_RUN_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return {"passed": False, "detail": f"TIMEOUT after {BATCH_RUN_TIMEOUT}s"}
    if proc.returncode == 0:
        return {"passed": True, "detail": "job FINISHED"}
    tail = (proc.stderr.strip() or proc.stdout.strip())[-1500:]
    return {"passed": False, "detail": tail}


def _kill_job(proc: subprocess.Popen, container_name: str | None) -> None:
    """Stop a streaming job: docker kill for container mode, SIGTERM otherwise."""
    if container_name:
        subprocess.run(["docker", "kill", container_name],
                       capture_output=True, timeout=30)
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
    elif proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=15)
        except Exception:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except Exception:
                pass


def _run_streaming(cmd: list[str], container_name: str | None = None) -> dict:
    """Start the job, watch output for STREAM_ALIVE_SECONDS.

    Pass = process still alive with no failure pattern in output.
    The job is killed afterwards either way.
    """
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    deadline = time.monotonic() + STREAM_ALIVE_SECONDS
    output_lines: list[str] = []
    failure = None

    def read_chunk() -> str:
        # Non-blocking read on the raw fd; text-mode .read() raises
        # BlockingIOError when the pipe is empty.
        try:
            data = os.read(proc.stdout.fileno(), 65536)
        except (BlockingIOError, OSError):
            return ""
        return data.decode("utf-8", errors="replace") if data else ""

    try:
        os.set_blocking(proc.stdout.fileno(), False)
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                # Streaming job exiting early is a failure
                output_lines.append(read_chunk())
                failure = f"exited early with code {proc.returncode}"
                break
            chunk = read_chunk()
            if chunk:
                output_lines.append(chunk)
                if _FAILURE_PATTERNS.search(chunk):
                    failure = "failure pattern in output"
                    break
            time.sleep(1.0)
    finally:
        _kill_job(proc, container_name)

    if failure:
        tail = "".join(output_lines)[-1500:]
        return {"passed": False, "detail": f"{failure}\n{tail}"}
    return {"passed": True,
            "detail": f"alive and healthy for {STREAM_ALIVE_SECONDS}s"}


def _verify_sink(verify: dict) -> dict:
    """Run a docker-exec probe and compare row count against expect_min_rows."""
    container = verify["container"]
    command = verify["command"]
    expect_min = int(verify.get("expect_min_rows", 1))
    try:
        proc = subprocess.run(
            ["docker", "exec", container, *command],
            capture_output=True, text=True, timeout=60,
        )
        if proc.returncode != 0:
            return {"passed": False, "detail": f"probe failed: {proc.stderr.strip()[:300]}"}
        m = re.search(r"\d+", proc.stdout)
        if not m:
            return {"passed": False, "detail": f"no count in probe output: {proc.stdout[:200]}"}
        count = int(m.group(0))
        if count >= expect_min:
            return {"passed": True, "detail": f"{count} rows in sink"}
        return {"passed": False, "detail": f"only {count} rows in sink (expected >= {expect_min})"}
    except FileNotFoundError:
        return {"passed": None, "detail": "SKIPPED: docker not available"}
    except subprocess.TimeoutExpired:
        return {"passed": False, "detail": "probe TIMEOUT"}


def check_services_up(services: list[str]) -> tuple[bool, str]:
    """Check the required docker compose services are running (by container name)."""
    if not services:
        return True, "no services required"
    name_map = {
        "mysql": "st-bench-mysql",
        "postgres": "st-bench-postgres",
        "kafka": "st-bench-kafka",
        "clickhouse": "st-bench-clickhouse",
        "elasticsearch": "st-bench-elasticsearch",
        "minio": "st-bench-minio",
        "doris": "st-bench-doris",
        "starrocks": "st-bench-starrocks",
    }
    try:
        proc = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=30,
        )
        running = set(proc.stdout.split())
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False, "docker not available"
    missing = [s for s in services if name_map.get(s, s) not in running]
    if missing:
        return False, f"services not running: {', '.join(missing)}"
    return True, "all services up"
