---
sidebar_position: 2
title: 命令行
---

# Application 命令行

`bin/seatunnel-application.sh` 是 Application Mode 的唯一入口，只提供三个操作。三者都面向同一个
外部资源平台并读取同一份部署文件，因此 `status`、`cancel` 看到的正是 `submit` 创建的
application。

## 命令格式

```bash
bin/seatunnel-application.sh submit -t <target> -a application.config [-c job.conf]
bin/seatunnel-application.sh status -t <target> --id APPLICATION_ID -a application.config [--wait] \
  [--wait-timeout <ms>] [--wait-interval <ms>]
bin/seatunnel-application.sh cancel -t <target> --id APPLICATION_ID -a application.config
```

内置帮助可以列出全部参数：

```bash
bin/seatunnel-application.sh -h
```

```text
Usage: seatunnel-application.sh [options] submit | status | cancel
  Options:
    -a, --application-config Optional HOCON deployment file, e.g.
                             application.config (not the job file)
    -c, --config             SeaTunnel job configuration file (required for
                             submit unless the provider supplies it, for
                             example kubernetes.job-config-file)
    -h, --help               Show the usage message
    --id                     Platform application ID (required for
                             status/cancel)
    --job-id                 Optional native Zeta job ID for submit
    --restore-job-id         Historical Zeta job ID whose latest checkpoint
                             restores this new application
  * -t, --target             External resource platform: kubernetes
    --wait                   Wait for completion (submit/status only; default:
                             return immediately) (default: false)
    --wait-timeout           Maximum time in milliseconds that --wait polls for
                             a terminal status before failing with the last
                             observed status; 0 waits indefinitely (default: 0)
    --wait-interval          Status poll interval in milliseconds while --wait
                             blocks (default: 500)
    -D                       Override a non-sensitive deployment option:
                             -Dkey=value (syntax: -Dkey=value) (default: {})
```

## 参数

| 参数 | 适用操作 | 说明 |
| --- | --- | --- |
| `submit` / `status` / `cancel` | 全部 | 操作类型，位于第一个位置参数 |
| `-t`、`--target` | 全部 | 运行 application 的资源平台 |
| `-c`、`--config` | submit | SeaTunnel 作业配置文件；作业由平台提供（如挂载的 `job.conf`）时可省略 |
| `-a`、`--application-config` | 全部 | 可选部署文件，包含公共选项和平台选项 |
| `-Dkey=value` | 全部 | 覆盖单个非敏感的 `application.*` / `kubernetes.*` 部署选项（例如 `-Dapplication.worker-count=4`），可重复；作业 `${...}` 变量在 Pod 内解析，不在提交机替换 |
| `--id` | status、cancel | `submit` 打印的平台 application ID |
| `--wait` | submit、status | 轮询直到终态 |
| `--wait-timeout <ms>` | submit、status | `--wait` 等待终态的最长时间（毫秒），超时后带最后状态报错；默认 `0` 表示一直等待。需配合 `--wait` |
| `--wait-interval <ms>` | submit、status | `--wait` 的状态轮询间隔（毫秒），默认 `500`。需配合 `--wait` |
| `--job-id` | submit | 指定原生 Zeta 作业 ID，不指定时自动生成 |
| `--restore-job-id` | submit | 用该作业 ID 的最新 checkpoint 恢复本次 application |

## 操作行为

| 操作 | 做什么 | 结果 |
| --- | --- | --- |
| `submit` | 完成作业本地化，在平台上创建 application 并启动 | 打印 application ID 与原生作业 ID |
| `status` | 读取平台状态并映射为 `ApplicationStatus` | 打印 application ID 与状态；application 失败时以非零码退出 |
| `cancel` | 删除 application 及其自有资源 | 打印 application ID 与取消结果 |

## submit 立即返回

`submit` 在平台接受并启动 application 后立即返回，镜像拉取、准入和调度慢都不会让命令失败；
需要终态时加 `--wait` 在同一命令内等待：

```bash
bin/seatunnel-application.sh submit -t <target> -a application.config --wait
```

作业可以来自 `-c`，也可以来自平台挂载到 application 内的文件（例如
`kubernetes.job-config-file`）；两者都没有的提交会在创建第一个资源之前被拒绝。写成 `${VAR}`
的值在运行中的 application 内解析，不在提交机解析，因此可以通过环境变量注入平台管理的
凭据。

## 状态与等待

`status` 默认只读一次快照；加 `--wait` 后持续轮询直到终态。`--wait-timeout` 可以限制轮询时长，
超时后带最后状态报错退出；默认 `0` 表示一直等待，与 attached 客户端语义一致。`--wait-interval`
控制轮询间隔。平台无关的状态定义如下：

| 状态 | 含义 |
| --- | --- |
| `CREATED` | 平台已接受提交，尚未启动控制面 |
| `DEPLOYING` | 平台正在准备控制面 |
| `RUNNING` | application 正在运行，Worker 或作业可能仍在启动 |
| `SUCCEEDED` | 作业成功结束 |
| `FAILED` | 启动、执行或可上报的清理失败 |
| `CANCELED` | 平台报告已取消 |
| `UNKNOWN` | 已知 application 的平台状态暂时无法映射 |

`SUCCEEDED`、`FAILED`、`CANCELED` 为终态，`--wait` 在终态停止。`FAILED`、`CANCELED` 和
`UNKNOWN` 会让 `status` 以非零码退出，因此可以直接作为调度器或 CI 的完成判定：

```bash
if ! bin/seatunnel-application.sh status -t <target> --id "$APP_ID" -a application.config --wait; then
  echo "application $APP_ID did not succeed"
fi
```

## cancel

`cancel` 请求平台移除 application：自有资源随之删除，SeaTunnel 之外管理的对象不受影响。已取消
或已完成的 application 能保留多久，取决于平台自身的保留策略。

各操作在具体平台上的行为见对应平台指南。
