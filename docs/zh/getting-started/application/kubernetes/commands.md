---
sidebar_position: 4
title: 命令参考
---

# Kubernetes application 命令

`bin/seatunnel-application.sh` 在 Kubernetes 上提交、查看和取消 application。操作类型、参数、
等待语义与退出码见公共的[命令行](../cli.md)参考；本页说明这些命令在集群里具体做什么。

## 命令格式

```bash
bin/seatunnel-application.sh submit --target kubernetes -a kubernetes.conf [-c job.conf]
bin/seatunnel-application.sh status --target kubernetes --id APPLICATION_ID -a kubernetes.conf [--wait]
bin/seatunnel-application.sh cancel --target kubernetes --id APPLICATION_ID -a kubernetes.conf
```

## submit

`submit` 创建 owner `batch/v1` Job、application 自有的 Secret/ConfigMap/Service，并启动 Job：

```text
Application ID: example-9f2c41a0b7d3
Job ID: 917381145928572929
```

application ID 由 `application.name` 生成，同时是 Kubernetes Job 名和所有自有资源上的归属
label 值。当部署文件通过 `kubernetes.job-config-file` 选择挂载的作业文件时，`-c` 可以省略。

命令在 API 接受并启动 Job 后立即返回，镜像拉取、准入和调度慢都不会让提交失败；资源创建本身
失败时，本次创建的资源会先回滚再抛出错误。

## status

`status` 把 Job 和 Pod 状态映射为平台无关状态：

```text
Application ID: example-9f2c41a0b7d3
Status: RUNNING
```

不带 `--wait` 的 `status` 打印状态快照，查询成功时返回 `0`，包括 `FAILED` 和 `UNKNOWN`；
查询异常以非零码退出。加 `--wait` 会持续轮询直到终态或 `UNKNOWN`。
`status --wait` 和 `submit --wait` 在 `FAILED` 时退出码为 `1`，在 `UNKNOWN` 时为 `2`
（表示无法确定成功或失败），在 `SUCCEEDED` 或 `CANCELED` 时为 `0`。
共享退出码规则见[状态与等待](../cli.md#状态与等待)。
`--wait-timeout` 可以限制轮询时长，超时后带最后状态报错退出；
默认 `0` 表示一直等待。`--wait-interval` 设置轮询间隔（默认 500 ms）。

## cancel

`cancel` 删除 application 的 owner Job，自有 Pod、Service、Secret 和 ConfigMap 通过 owner
reference 一并回收；SeaTunnel 之外创建的引用对象不会被修改或删除。删除请求被接受后命令输出
`Cancellation requested`。由于 owner Job 已被删除，之后再执行 `status` 会返回 `UNKNOWN`；
可以用 `kubectl` 确认 Pod 已经消失。如果删除前 API 调用失败，请在 API 恢复后重试 `cancel`。

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
