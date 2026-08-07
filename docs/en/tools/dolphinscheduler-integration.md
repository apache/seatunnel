# Integrating SeaTunnel with DolphinScheduler via Docker Compose

A common question when deploying SeaTunnel through Docker Compose is: which `SEATUNNEL_HOME` path should DolphinScheduler be configured with so it can correctly submit SeaTunnel jobs? This document explains the reasoning and provides a minimal, ready-to-use example.

## 1. What `SEATUNNEL_HOME` Means for Scheduler Integration

DolphinScheduler does not call SeaTunnel over a network API. Instead, it directly executes SeaTunnel's launch script on the same machine (or container) it runs on, for example:

```
${SEATUNNEL_HOME}/bin/seatunnel.sh --config <job_config_path>
```

This means `SEATUNNEL_HOME` must point to a path **that the DolphinScheduler process itself can actually reach**, and that path must contain a complete SeaTunnel installation (`bin/`, `config/`, `lib/`, `connectors/`, etc.). If the path is empty or missing, DolphinScheduler will fail at job execution time with a "script not found" or "command not found" error.

## 2. Host Paths vs. Container Paths

This is the most common source of confusion. There are two distinct "points of view":

- **Host path**: where the SeaTunnel installation actually lives on your machine or server's disk, e.g. `/home/user/seatunnel` or `./seatunnel`.
- **Container path**: what DolphinScheduler's container sees internally, e.g. `/opt/seatunnel`. Whether files exist at that path depends entirely on whether you mounted a host directory into it via `volumes`.

**Key rule**: `SEATUNNEL_HOME` always describes the path as seen **from the environment DolphinScheduler's process runs in**, not from your local machine. If DolphinScheduler runs inside a container, `SEATUNNEL_HOME` must be a container-internal path (e.g. `/opt/seatunnel`), and that container path must be mounted to the host directory where SeaTunnel is actually installed.

## 3. Deciding the Path When DolphinScheduler Runs on the Host

If DolphinScheduler is not containerized and runs directly as a process on a physical or virtual machine:

- `SEATUNNEL_HOME` should be set to the real host-side installation path, e.g. `/opt/module/seatunnel`.
- There is no volume-mounting concern here — DolphinScheduler's process and the SeaTunnel installation share the same filesystem, so the path is exactly what it appears to be.

## 4. Deciding the Path When DolphinScheduler Also Runs in Docker or Kubernetes

This is the case most people hit with Docker Compose. Two things are required:

1. Mount the host's SeaTunnel installation directory into a path inside the DolphinScheduler container via `volumes`.
2. Set `SEATUNNEL_HOME` to the **mounted container-side path**, not the host path.

The same logic applies in Kubernetes: "mounting" becomes `volumeMounts` combined with a `PersistentVolume` (or `hostPath`), but the principle is identical — `SEATUNNEL_HOME` should be set to the path as seen inside the Pod.

## 5. Minimal Working Example (Using the Common Container Path `/opt/seatunnel`)

```yaml
version: '3.8'

services:
  dolphinscheduler:
    image: apache/dolphinscheduler-standalone-server:3.2.1
    container_name: dolphinscheduler
    hostname: dolphinscheduler
    ports:
      - "12345:12345"
    environment:
      - SEATUNNEL_HOME=/opt/seatunnel
    volumes:
      # host ./seatunnel -> container /opt/seatunnel
      # the host directory must contain a full SeaTunnel installation beforehand
      - ./seatunnel:/opt/seatunnel:ro
      - ./dolphinscheduler/logs:/opt/dolphinscheduler/logs
    networks:
      - ds-network

networks:
  ds-network:
    driver: bridge
```

Prepare the host-side SeaTunnel installation directory (the official DolphinScheduler image does not bundle SeaTunnel itself — it must be downloaded and extracted manually):

```bash
mkdir -p seatunnel dolphinscheduler/logs
wget https://archive.apache.org/dist/seatunnel/2.3.4/apache-seatunnel-2.3.4-bin.tar.gz
tar -zxvf apache-seatunnel-2.3.4-bin.tar.gz -C seatunnel --strip-components=1
```

After this, `./seatunnel` should contain `bin/`, `config/`, `lib/`, and similar subdirectories, and the same content will be visible inside the container at `/opt/seatunnel` once it starts.

## 6. Required Volume Mounts and Network Assumptions

- **Required mount**: the host's SeaTunnel installation directory → the container path referenced by `SEATUNNEL_HOME`. A read-only mount (`:ro`) is sufficient, since DolphinScheduler only needs to execute scripts, not write to the installation directory.
- **Recommended mounts**: DolphinScheduler's log directory and any directory holding job configuration files, so logs and configs remain accessible outside the container for debugging and version control.
- **Network assumption**: if SeaTunnel jobs need to reach data sources (databases, message queues, etc.) that are also deployed via Docker Compose, they must be on the same custom network as DolphinScheduler (e.g. `ds-network` above); otherwise containers cannot resolve each other by service name and you'll need to fall back to host IPs or additional network configuration.
- If SeaTunnel jobs need to reach services on the host machine that are outside the Docker network, be aware of Docker's default network isolation — you may need `host.docker.internal` (on Docker Desktop) or explicit host-network configuration.
