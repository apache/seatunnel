---

sidebar_position: 3
-------------------

# 以集群模式运行作业

这是最推荐的在生产环境中使用SeaTunnel Engine的方法。此模式支持SeaTunnel Engine的全部功能，集群模式将具有更好的性能和稳定性。

在集群模式下，首先需要部署SeaTunnel Engine集群，然后客户端将作业提交给SeaTunnel Engine集群运行。

## 部署SeaTunnel Engine集群

部署SeaTunnel Engine集群参考[SeaTunnel Engine集群部署](deployment.md)

## 提交作业

```shell
$SEATUNNEL_HOME/bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template
```

