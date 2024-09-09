---
sidebar_position: 3
---

# SeaTunnel Engine(Zeta) 安装部署

SeaTunnel Engine(Zeta) 支持三种不同的部署模式：本地模式、混合集群模式和分离集群模式。

每种部署模式都有不同的使用场景和优缺点。在选择部署模式时，您应该根据您的需求和环境来选择。

Local模式：只用于测试，每个任务都会启动一个独立的进程，任务运行完成后进程会退出。

混合集群模式：SeaTunnel Engine 的Master服务和Worker服务混合在同一个进程中，所有节点都可以运行作业并参与选举成为master，即master节点也在同时运行同步任务。在该模式下，Imap(保存任务的状态信息用于为任务的容错提供支持)数据会分布在所有节点中。

分离集群模式：SeaTunnel Engine 的Master服务和Worker服务分离，每个服务单独一个进程。Master节点只负责作业调度，rest api，任务提交等，Imap数据只存储在Master节点中。Worker节点只负责任务的执行，不参与选举成为master，也不存储Imap数据。

使用建议：建议使用[分离集群模式](separated-cluster-deployment.md)。在混合集群模式下，Master节点要同步运行任务，当任务规模较大时，会影响Master节点的稳定性，一但Master节点宕机或心跳超时，会导致Master节点切换，Master节点切换会导致所有正在运行的任务进行容错，会进一步增长集群的负载。因此，我们更建议使用分离模式。

[Local模式部署](local-mode-deployment.md)

[混合集群模式部署](hybrid-cluster-deployment.md)

[分离集群模式部署](separated-cluster-deployment.md)
