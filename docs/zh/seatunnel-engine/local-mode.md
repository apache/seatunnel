---

sidebar_position: 2
-------------------

# 以本地模式运行作业

仅用于测试。

最推荐在生产环境中使用SeaTunnel Engine的方式为[集群模式](cluster-mode.md).

## 本地模式部署SeaTunnel Engine

[部署SeaTunnel Engine本地模式参考](../../zh/start-v2/locally/deployment.md)

## 修改SeaTunnel Engine配置

将$SEATUNNEL_HOME/config/hazelcast.yaml中的自动增量更新为true

## 提交作业

```shell
$SEATUNNEL_HOME/bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template -e local
```

