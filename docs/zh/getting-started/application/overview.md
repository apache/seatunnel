---
sidebar_position: 1
title: 概述
---

# Application Mode 简介

Application Mode 一次提交部署一套自包含的 application：资源平台启动专用的 Zeta Master 和固定数量
的 Worker，Master 只运行一个作业，作业结束后平台回收全部进程。

Worker 注册、slot 调度、任务执行和 checkpoint 仍由 Zeta 自己负责，资源平台只提供并回收计算资源。
正是这层边界，让同一个作业文件无需改动就能跑在不同的资源平台上。

## 解决了什么问题

### 一次性批处理任务

作业只跑一次，结果落盘后立即释放全部资源：不需要维护长期驻留的集群，也没有残留进程需要手工清理。
application 状态直接映射为平台对象，用 `status` 和 `cancel` 就能观测和停止。

### 大规模批处理团队

同时运行大量批处理作业的团队可以获得按作业隔离的能力：每个作业独享 Master 和固定数量的 Worker，
有自己的 CPU、内存、slot 占用，以及 namespace 和 label。作业之间不会互相借占 slot 或干扰状态，
配额和策略也按 application 生效。checkpoint 跨 application 存活，新的 application 可以按原生
job ID 恢复此前的作业。

## 适用场景

- 已经有资源平台，不想再维护长期运行的共享 SeaTunnel 集群；
- 每个作业都希望拥有独立集群和固定资源占用；
- 希望用平台对象查看和管理作业生命周期。

## 不适用场景

- 大量小作业希望占用一个长期集群（用独立或集群部署）；
- 作业运行期间需要动态扩缩 Worker 或自动补拉 Worker；应用模式使用固定 Worker，失败即结束；
- 发行版中没有对应资源平台的提供商。

## 分布形态

- 一个应用程序运行一个 Zeta Master 和固定数量的 Worker，作业运行期间不增减成员；
- Worker 在作业启动前把 slot 注册到 Master；
- Master 将作业结果映射为平台终态，并在退出前释放全部 Worker；
- checkpoint 存放在引擎侧的持久化存储中，可跨应用程序生命周期，由新的应用程序按作业 ID 恢复。

## 命令行

```bash
bin/seatunnel-application.sh submit -t <target> -c job.conf -a application.config
bin/seatunnel-application.sh status -t <target> --id APPLICATION_ID -a application.config --wait
bin/seatunnel-application.sh cancel -t <target> --id APPLICATION_ID -a application.config
```

| 参数 | 含义 |
| --- | --- |
| `-t`, `--target` | 运行 application 的资源平台 |
| `-c`, `--config` | SeaTunnel 作业配置文件；作业由平台提供（如挂载的 `job.conf`）时可省略 |
| `-a`, `--application-config` | 可选部署文件，包含公共选项和平台选项 |
| `--wait` | submit/status 等待终态；不传则立即返回 |
| `-ikey=value` | 非敏感部署覆盖项，例如 worker 资源或平台选项 |

`submit` 在平台接受 application 后立即返回，不等待 Master，所以镜像拉取慢或平台调度慢不会让提交失败。
`status`、`cancel` 只访问平台 API，Master 退出后仍可使用。

完整的命令参考（含退出码与取消行为）见对应平台指南。

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
