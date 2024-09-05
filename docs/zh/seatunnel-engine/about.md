---
sidebar_position: 1
---

# SeaTunnel Engine 简介

SeaTunnel Engine 是一个由社区开发的用于数据同步场景的引擎，作为 SeaTunnel 的默认引擎，它支持高吞吐量、低延迟和强一致性的数据同步作业操作，更快、更稳定、更节省资源且易于使用。

SeaTunnel Engine 的整体设计遵循以下路径：

- 更快，SeaTunnel Engine 的执行计划优化器旨在减少数据网络传输，从而减少由于数据序列化和反序列化造成的整体同步性能损失，使用户能够更快地完成数据同步操作。同时，支持速度限制，以合理速度同步数据。
- 更稳定，SeaTunnel Engine 使用 Pipeline 作为数据同步任务的最小粒度的检查点和容错。任务的失败只会影响其上游和下游任务，避免了任务失败导致整个作业失败或回滚的情况。同时，SeaTunnel Engine 还支持数据缓存，用于源数据有存储时间限制的场景。当启用缓存时，从源读取的数据将自动缓存，然后由下游任务读取并写入目标。在这种情况下，即使由于目标失败而无法写入数据，也不会影响源的常规读取，防止源数据过期被删除。
- 节省空间，SeaTunnel Engine 内部使用动态线程共享技术。在实时同步场景中，对于每个表数据量很大但每个表数据量很小的表，SeaTunnel Engine 将在共享线程中运行这些同步任务，以减少不必要的线程创建并节省系统空间。在读取和写入数据方面，SeaTunnel Engine 的设计目标是最小化 JDBC 连接的数量；在 CDC 场景中，SeaTunnel Engine 将重用日志读取和解析资源。
- 简单易用，SeaTunnel Engine 减少了对第三方服务的依赖，并且可以独立于如 Zookeeper 和 HDFS 等大数据组件实现集群管理、快照存储和集群 HA 功能。这对于目前缺乏大数据平台的用户，或者不愿意依赖大数据平台进行数据同步的用户来说非常有用。

未来，SeaTunnel Engine 将进一步优化其功能，以支持离线批同步的全量同步和增量同步、实时同步和 CDC。

### 集群管理

- 支持独立运行；
- 支持集群运行；
- 支持自治集群（去中心化），使用户无需为 SeaTunnel Engine 集群指定主节点，因为它可以在运行过程中自行选择主节点，并且在主节点失败时自动选择新的主节点；
- 自治集群节点发现和具有相同 cluster_name 的节点将自动形成集群。

### 核心功能

- 支持在本地模式下运行作业，作业完成后集群自动销毁；
- 支持在集群模式下运行作业（单机或集群），通过 SeaTunnel 客户端将作业提交给 SeaTunnel Engine 服务，作业完成后服务继续运行并等待下一个作业提交；
- 支持离线批同步；
- 支持实时同步；
- 批流一体，所有 SeaTunnel V2 Connector 均可在 SeaTunnel Engine 中运行；
- 支持分布式快照算法，并支持与 SeaTunnel V2 Connector 的两阶段提交，确保数据只执行一次。
- 支持在 Pipeline 级别调用作业，以确保即使在资源有限的情况下也能启动；
- 支持在 Pipeline 级别对作业进行容错。任务失败只影响其所在 Pipeline，只需要回滚 Pipeline 下的任务；
- 支持动态线程共享，以实时同步大量小数据集。

### 快速开始

https://seatunnel.apache.org/docs/start-v2/locally/quick-start-seatunnel-engine

### 下载安装

[下载安装](download-seatunnel.md)
