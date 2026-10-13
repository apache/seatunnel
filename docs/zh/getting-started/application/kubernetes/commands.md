---
sidebar_position: 4
title: 命令参考
---

# Application 命令

`bin/seatunnel-application.sh` 只提供三个操作：`submit`、`status` 和 `cancel`。它们都面向同一
个外部资源平台（`--target kubernetes`），并读取同一份部署文件，因此 `status` 和 `cancel`
连接的 Namespace、kubeconfig 与提交时完全一致。

## 命令格式

```bash
bin/seatunnel-application.sh submit --target kubernetes -c job.conf -a kubernetes.conf
bin/seatunnel-application.sh status --target kubernetes --id APPLICATION_ID -a kubernetes.conf [--wait]
bin/seatunnel-application.sh cancel --target kubernetes --id APPLICATION_ID -a kubernetes.conf
```

## 参数

| 参数 | 适用命令 | 说明 |
| --- | --- | --- |
| `submit` / `status` / `cancel` | 全部 | 操作类型，位于第一个位置参数 |
| `-t`、`--target` | 全部 | 资源平台，当前为 `kubernetes` |
| `-c`、`--config` | submit | 作业配置文件；当作业由平台提供（例如 `kubernetes.job-config-file`）时可省略 |
| `-a`、`--application-config` | 全部 | 可选的 HOCON 部署文件，例如 `kubernetes.conf` |
| `-i key=value` | 全部 | 覆盖单个非敏感部署选项，例如 `-i application.worker-count=4`，可重复使用 |
| `--id` | status、cancel | `submit` 打印的 application ID |
| `--wait` | submit、status | 轮询直到 application 到达终态 |
| `--job-id` | submit | 指定原生 Zeta 作业 ID，不指定时自动生成 |
| `--restore-job-id` | submit | 用该作业 ID 的最新 checkpoint 恢复本次 application |

## 命令能力对照

| 操作 | 平台动作 | 输出 | 是否等待 |
| --- | --- | --- | --- |
| `submit` | 创建 owner Job 及其自有的 Secret/ConfigMap/Service 并启动 Job | `Application ID`（即 Kubernetes Job 名）与 `Job ID` | 默认不等待，除非加 `--wait` |
| `status` | 读取 Job、Pod 和运行状态并映射为 `ApplicationStatus` | `Application ID` 与 `Status` | 默认不等待，除非加 `--wait` |
| `cancel` | 删除 owner Job，自有资源随之清理，引用资源不动 | `Application ID` 与 `Cancellation requested` | 不等待 |

## submit

`submit` 完成作业本地化、按部署链创建全部资源并启动 Job，然后打印两个 ID：

```text
Application ID: example-9f2c41a0b7d3
Job ID: 917381145928572929
```

application ID 由 `application.name` 生成，同时是 workload 名称、Kubernetes Job 名和所有自有
资源上的归属 label 值；它满足 DNS 命名要求，每次提交唯一。

命令在平台接受并启动 Job 后立即返回，镜像拉取、准入和调度慢都不会导致提交失败；加上
`--wait` 时会先等待 application 结束再打印终态。资源创建本身失败时，本次创建的资源会先
回滚再抛出错误。

## status

`status` 只读取当前状态，不修改 application：

```bash
bin/seatunnel-application.sh status --target kubernetes --id example-9f2c41a0b7d3 -a kubernetes.conf
```

```text
Application ID: example-9f2c41a0b7d3
Status: RUNNING
```

原生状态包括 `CREATED`、`DEPLOYING`、`RUNNING`、`SUCCEEDED`、`FAILED`、`CANCELED` 和
`UNKNOWN`。加 `--wait` 后每 500 毫秒轮询一次，直到出现终态（`SUCCEEDED`、`FAILED` 或
`CANCELED`）。出现可上报的失败时命令以非零码退出，因此可以直接作为调度器或 CI 的完成
判定：

```bash
if ! bin/seatunnel-application.sh status --target kubernetes --id "$APP_ID" -a kubernetes.conf --wait; then
  echo "application $APP_ID did not succeed"
fi
```

## cancel

`cancel` 删除 application 的 owner Job：

```bash
bin/seatunnel-application.sh cancel --target kubernetes --id example-9f2c41a0b7d3 -a kubernetes.conf
```

自有 Pod、Service、Secret 和 ConfigMap 通过 owner reference 一并回收；SeaTunnel 之外创建的
引用对象不会被修改或删除。取消后 application 状态为 `CANCELED`；已完成的 Job 会保留到
`kubernetes.finished-job-retention-seconds` 到期。

## 用 kubectl 排查

所有平台资源都带有 application ID 归属 label，可直接按 application 查询：

```bash
kubectl -n seatunnel-apps get jobs,pods,services -l seatunnel.apache.org/application-id=example-9f2c41a0b7d3
kubectl -n seatunnel-apps describe job example-9f2c41a0b7d3
kubectl -n seatunnel-apps logs job/example-9f2c41a0b7d3
```

:::tip
程序化等待结束用 `status --wait`，定位启动问题仍要看 Pod 事件和日志，命令本身不替代
`kubectl` 诊断。
:::

这些命令使用的部署选项见 [配置参考](configuration.md)。
