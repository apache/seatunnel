---
sidebar_position: 2
---

# Deployment SeaTunnel Engine

## 1. Download

SeaTunnel Engine is the default engine of SeaTunnel. The installation package of SeaTunnel already contains all the contents of SeaTunnel Engine.

## 2. Config Cluster

### 2.1 Config SEATUNNEL_HOME

You can config `SEATUNNEL_HOME` by add `/etc/profile.d/seatunnel.sh` file. The content of `/etc/profile.d/seatunnel.sh` are

```
export SEATUNNEL_HOME=${seatunnel install path}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

### 2.2 Config SeaTunnel Engine JVM options

SeaTunnel Engine supported two ways to set jvm options.

1. Add JVM Options to `$SEATUNNEL_HOME/bin/seatunnel-cluster.sh`.

    Modify the `$SEATUNNEL_HOME/bin/seatunnel-cluster.sh` file and add `JAVA_OPTS="-Xms2G -Xmx2G"` in the first line.
2. Add JVM Options when start SeaTunnel Engine. For example `seatunnel-cluster.sh -DJvmOption="-Xms2G -Xmx2G"`

### 2.3 Config SeaTunnel Engine 

SeaTunnel Engine provides many functions, which need to be configured in seatunnel.yaml. 

#### Backup count

SeaTunnel Engine implement cluster management based on [Hazelcast IMDG](https://docs.hazelcast.com/imdg/4.1/). The state data of cluster(Job Running State, Resource State) are storage is [Hazelcast IMap](https://docs.hazelcast.com/imdg/4.1/data-structures/map).
The data saved in Hazelcast IMap will be distributed and stored in all nodes of the cluster. Hazelcast will partition the data stored in Imap. Each partition can specify the number of backups.
Therefore, SeaTunnel Engine can achieve cluster HA without using other services(for example zookeeper).

The `backup count` is to define the number of synchronous backups. For example, if it is set to 1, backup of a partition will be placed on one other member. If it is 2, it will be placed on two other members.

We suggest the value of `backup-count` is the `min(1, max(5, N/2))`. `N` is the number of the cluster node.

```
seatunnel:
    engine:
        backup-count: 1
        # other config

```

#### Slot service

The number of Slots determines the number of TaskGroups the cluster node can run in parallel. SeaTunnel Engine is a data synchronization engine and most jobs are IO intensive.

Dynamic Slot is suggest.

```
seatunnel:
    engine:
        slot-service:
            dynamic-slot: true
        # other config
```

#### Checkpoint Manager

Like Flink, SeaTunnel Engine support Chandy–Lamport algorithm. Therefore, SeaTunnel Engine can realize data synchronization without data loss and duplication.


