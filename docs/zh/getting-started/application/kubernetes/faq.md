---
sidebar_position: 7
title: FAQ
---

# Kubernetes FAQ

Application Mode 运维、生命周期、故障与安全方面的常见问题。命令见[命令参考](commands.md)，全量
参数表见[配置参数说明](configuration.md)。

## 运维

### 如何查看或取消 application？

```bash
bin/seatunnel-application.sh status --target kubernetes \
  --id ACTUAL_APPLICATION_ID --application-config kubernetes.conf

bin/seatunnel-application.sh cancel --target kubernetes \
  --id ACTUAL_APPLICATION_ID --application-config kubernetes.conf
```

`cancel` 删除 Job，并清理带 application 标签的 Pod、Service 以及由本应用创建的 Secret 和
ConfigMap。取消不创建 savepoint，引用的 Secret/ConfigMap 不会被修改。资源删除后再次查询返回
`UNKNOWN`；Job TTL 到期后也会返回 `UNKNOWN`。

### 如何查看资源和日志？

```bash
kubectl -n seatunnel-apps get jobs,pods,services,secrets \
  -l seatunnel.apache.org/application-id=ACTUAL_APPLICATION_ID

kubectl -n seatunnel-apps logs job/ACTUAL_APPLICATION_ID
kubectl -n seatunnel-apps describe job ACTUAL_APPLICATION_ID
```

Worker 问题需要逐个查看对应 Pod 日志和事件。镜像拉取、ResourceQuota、LimitRange、准入策略和节点容量问题通常出现在 Pod events 中。

## 生命周期

### 完成后的资源保留多久？

Master 退出码形成 Job 的 `Complete` 或 `Failed` condition。结束后的 Master Pod、Service 以及由
本应用创建的 Secret 和 ConfigMap 保留到 `kubernetes.finished-job-retention-seconds` 指定的
TTL，便于排障。

如果 Master 在清理前被强制终止，Worker 会在失去 Master 后退出，停止的 Pod 对象可能保留到 Job TTL。API 故障阻止清理时，在 API 恢复后重新执行 `cancel`。

### Master Pod 始终起不来会怎样？

Job 会一直处于 `Active`，`status --wait` 会持续轮询；可以用 `--wait-timeout` 限制等待
时长。无人值守的批作业建议设置 `kubernetes.active-deadline-seconds`，让 Kubernetes 自动把 Job
置为失败，`--wait` 随之返回 `FAILED`。根因看 Pod events：镜像拉取、准入策略、ResourceQuota 或
节点资源不足。

## 常见问题

| 现象 | 检查项 |
| --- | --- |
| Master Pod 未运行 | image pull、准入策略、ResourceQuota、节点资源和 kubeconfig 权限 |
| Worker 未注册 | Service、NetworkPolicy、Master 端口、ServiceAccount 权限和 Worker events |
| Pod 内缺文件 | 引用的 ConfigMap/Secret 名称、key 与 `mount` 全路径必须一致；引用的 key 会在提交前校验。 |
| 找不到 Connector | 镜像 `connectors/` 是否包含版本匹配的插件和驱动 |
| Job 失败但原因不清晰 | Master 日志、所有 Worker 日志和 `kubectl describe job/pod` |
| checkpoint 恢复失败 | 历史 Zeta job ID、PVC/远程存储可访问性、保留策略和配置一致性 |

## 安全建议

- 使用独立 Namespace 和最小权限 ServiceAccount。
- 只有 Master 挂载 API token，Worker 不需要凭据。
- 私有镜像凭据通过 ServiceAccount 或集群配置管理。
- 限制 application Secret 的读取权限；集群安全策略要求时，为 Kubernetes Secret 启用静态加密。
- 不在日志中暴露 Connector 凭据，优先使用 Connector、文件系统或集群支持的凭据机制。
- 将 `kubernetes.kubeconfig` 写入权限受限的部署配置文件，或使用 Kubernetes SDK 默认配置。不要通过 `-i` 传递，因为提交主机上的其他用户可能看到命令行参数。
- 使用 NetworkPolicy 时仅放行需要的 Hazelcast 与外部数据系统流量。
