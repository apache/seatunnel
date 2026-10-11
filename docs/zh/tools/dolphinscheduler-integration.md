# 使用 Docker Compose 将 SeaTunnel 与 DolphinScheduler 集成

当 SeaTunnel 通过 Docker Compose 部署时，一个常见的问题是：DolphinScheduler 应该配置哪个 `SEATUNNEL_HOME` 路径才能正确提交作业？本文档说明这个问题背后的原理，并给出一个可直接使用的最小示例。

## 1. `SEATUNNEL_HOME` 对调度器集成意味着什么

DolphinScheduler 并不是通过网络接口远程调用 SeaTunnel，而是在自己所在的机器（或容器）上，直接执行 SeaTunnel 提供的启动脚本，例如：

```
${SEATUNNEL_HOME}/bin/seatunnel.sh --config <job_config_path>
```

因此 `SEATUNNEL_HOME` 必须指向 **DolphinScheduler 进程实际能够访问到的一个路径**，这个路径下必须包含完整的 SeaTunnel 安装目录（`bin/`、`config/`、`lib/`、`connectors/` 等）。如果这个路径下是空的或者不存在，DolphinScheduler 执行任务时会直接报"找不到脚本"或"命令不存在"的错误。

## 2. 宿主机路径 与 容器内路径 的区别

这是最容易搞混的地方，需要分清两个"视角"：

- **宿主机路径（Host Path）**：SeaTunnel 安装包实际存放在你电脑或服务器磁盘上的位置，例如 `/home/user/seatunnel` 或 `./seatunnel`。
- **容器内路径（Container Path）**：DolphinScheduler 容器内部看到的路径，例如 `/opt/seatunnel`。这个路径是否存在文件，取决于你有没有通过 `volumes` 把宿主机目录挂载进去。

**关键点**：`SEATUNNEL_HOME` 这个环境变量，配置的永远是 **DolphinScheduler 进程自己所在环境看到的路径**，而不是你电脑上的路径。如果 DolphinScheduler 跑在容器里，`SEATUNNEL_HOME` 就必须写容器内路径（如 `/opt/seatunnel`），并确保这个容器路径通过 volume 挂载对应到了宿主机上真正装了 SeaTunnel 的目录。

## 3. DolphinScheduler 运行在宿主机上时，如何确定路径

如果你没有用容器运行 DolphinScheduler，而是直接在物理机 / 虚拟机上以进程方式运行它，那么：

- `SEATUNNEL_HOME` 直接填宿主机上 SeaTunnel 的真实安装路径，例如 `/opt/module/seatunnel`。
- 不涉及任何 volume 挂载问题，DolphinScheduler 进程和 SeaTunnel 安装包在同一套文件系统里，路径所见即所得。

## 4. DolphinScheduler 也运行在 Docker（或 Kubernetes）中时，如何确定路径

这是 Docker Compose 场景下最常遇到的情况。此时必须做两件事：

1. 把宿主机上的 SeaTunnel 安装目录，通过 `volumes` 挂载到 DolphinScheduler 容器内的某个路径。
2. 把 `SEATUNNEL_HOME` 设置为**挂载后的容器内路径**，而不是宿主机路径。

在 Kubernetes 环境下同理，只是"挂载"变成了 `volumeMounts` + `PersistentVolume`（或 `hostPath`），思路完全一致：Pod 内部看到的路径才是 `SEATUNNEL_HOME` 应该填的值。

## 5. 最小可用示例（使用常见容器路径 `/opt/seatunnel`）

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
      # 宿主机 ./seatunnel 目录 -> 容器内 /opt/seatunnel
      # 宿主机这个目录下需要预先放好完整的 SeaTunnel 安装包内容
      - ./seatunnel:/opt/seatunnel:ro
      - ./dolphinscheduler/logs:/opt/dolphinscheduler/logs
    networks:
      - ds-network

networks:
  ds-network:
    driver: bridge
```

准备宿主机上的 SeaTunnel 安装目录（Docker 官方镜像不自带 SeaTunnel 程序本体，需要手动下载解压）：

```bash
export version="3.0.0"
mkdir -p seatunnel dolphinscheduler/logs
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-${version}-bin.tar.gz"
tar -zxvf "apache-seatunnel-${version}-bin.tar.gz" -C seatunnel --strip-components=1
```
:::caution 警告
从 2.2.0-beta 版本开始，连接器插件默认不再随安装包一起打包。你必须在把目录以只读方式挂载并启动容器**之前**安装好这些插件，否则所有提交的作业都会因为缺少连接器而失败。
:::

```bash
sh seatunnel/bin/install-plugin.sh 3.0.0
```

完成后，`./seatunnel` 目录下应包含 `bin/`、`config/`、`lib/` 等子目录，容器启动后即可在 `/opt/seatunnel` 下看到同样的内容。

## 6. 所需的 Volume 挂载与网络假设

- **必须挂载**：宿主机的 SeaTunnel 安装目录 → 容器内 `SEATUNNEL_HOME` 指向的路径（只读挂载 `:ro` 即可，因为 DolphinScheduler 只需要执行脚本，不需要写入安装目录）。
- **建议挂载**：DolphinScheduler 的日志目录、以及存放作业配置文件（job config）的目录，方便在容器外查看任务配置和日志，也便于版本管理和排查问题。
- **网络假设**：如果 SeaTunnel 需要连接的数据源（数据库、消息队列等）也用 Docker Compose 部署，需确保它们和 DolphinScheduler 处于同一个自定义网络（如上例中的 `ds-network`），否则容器间无法通过服务名互相访问，必须改用宿主机 IP 或额外的网络配置。
- 如果 SeaTunnel 任务需要访问宿主机上的其他服务（不在 Docker 网络内），需要注意 Docker 默认的网络隔离，可能需要使用 `host.docker.internal`（Docker Desktop 环境）或显式配置宿主机网络访问。
