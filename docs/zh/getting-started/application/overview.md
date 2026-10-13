---
sidebar_position: 1
title: 概述
---

# Application Mode 简介

Application Mode 为每个作业运行一套独立的 Zeta 集群：资源平台启动专用的 Master 和固定数量的
Worker，Master 只运行一个作业，作业结束后平台回收全部进程。Worker 注册、slot 调度、任务执行和
checkpoint 仍由 Zeta 自己负责，平台只提供并回收计算资源。

Application Mode 不是 standalone 或共享集群部署的替代品，而是同一个 Zeta 引擎的一种部署模式：
一个作业对应一个自带 Master 和 Worker 的自包含 application。

## 解决了什么问题

- 不再手写启动、发现、提交、失败传播和清理脚本：一条命令提交 application，启动、观测和回收都交给平台；
- 按作业隔离：每个作业一套专用 Master 和固定 Worker，作业之间互不影响，application 状态直接映射平台状态；
- 平台原生运维：status/cancel 直接访问平台 API，凭据放在平台管理的 Secret 中而不是作业文件里；
- checkpoint 跨 application 存活：后续 application 可以按原生 job ID 恢复上一个作业。

## 适用场景

- 已经有资源平台，不想再维护长期运行的共享 SeaTunnel 集群；
- 每个作业都希望拥有独立集群和固定资源占用；
- 希望用平台原生对象查看和管理作业生命周期。

## 不适用场景

- 大量小作业希望共用一个长期集群（用 standalone 或集群部署）；
- 作业运行期间需要动态扩缩 Worker 或自动补拉 Worker；Application Mode 使用固定 Worker，失败即结束；
- 发行版中没有对应资源平台的 provider。

## 集群形态

- 一个 application 运行一个 Zeta Master 和固定数量的 Worker，作业运行期间不增删成员；
- Worker 在作业启动前把 slot 注册到 Master；
- Master 把作业结果映射为平台终态，并在退出前释放全部 Worker；
- checkpoint 存放在引擎侧的持久化存储中，可跨 application 生命周期，由新的 application 按 job ID 恢复。

## 命令行

```bash
bin/seatunnel-application.sh submit -t <target> -c job.conf -a application.config
bin/seatunnel-application.sh status -t <target> --id APPLICATION_ID -a application.config --wait
bin/seatunnel-application.sh cancel -t <target> --id APPLICATION_ID -a application.config
```

| 参数 | 含义 |
| --- | --- |
| `-t`, `--target` | 运行 application 的资源平台 |
| `-c`, `--config` | SeaTunnel 作业配置文件，submit 必填 |
| `-a`, `--application-config` | 可选部署文件，包含公共选项和平台选项 |
| `--wait` | submit/status 等待终态；不传则立即返回 |
| `-ikey=value` | 非敏感部署覆盖项，例如 worker 资源或平台选项 |

`submit` 在平台接受 application 后立即返回，不等待 Master，所以镜像拉取慢或平台调度慢不会让提交失败。
`status`、`cancel` 只访问平台 API，Master 退出后仍可使用。

## 前提条件

- 发行版中包含所选用资源平台的 provider；
- 平台能够启动、观测并在 application 结束时回收专用进程；
- 凭据不要写进作业文件：通过平台管理的 Secret 注入，并在 application 中用 `${VAR}` 占位符解析。

平台相关的环境准备、权限和配置说明见对应平台指南。

## 术语表

| 术语 | 定义 |
| --- | --- |
| Application | 一个作业加上专属的 Master 和固定 Worker，用平台原生 ID 标识 |
| Master | 唯一运行作业、协调 Worker 并发布结果的 Zeta 成员 |
| Worker | 固定数量的 Zeta 成员，注册 slot 并执行任务 |
| 资源管理器 | Engine 中通过平台 driver 管理 Worker 生命周期的组件 |
| 资源管理器 driver | 平台 provider 的实现：启动、观测、释放 Worker 进程 |
| Application 规格 | 不可变、平台无关的 application 字段，随 application 带到 Master |
| 本地化配置 | 把规格交付给 Master 的平台机制，例如挂载文件或注入环境变量 |
| Checkpoint | 引擎持有的持久化状态，独立于 application 生命周期 |

## 推荐阅读顺序

| 阶段 | 文档 | 说明 |
| --- | --- | --- |
| 理解 | 本页 | Application Mode 是什么、什么时候用 |
| 上手 | 平台指南 | 环境准备、首次提交、状态查询 |
| 配置 | 平台指南 | 平台选项、凭据和运行时配置 |
| 运维 | 平台指南 | 取消、失败诊断和保留策略 |
