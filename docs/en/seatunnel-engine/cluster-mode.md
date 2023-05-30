---

sidebar_position: 3
-------------------

# Run Job With Cluster Mode

This is the most recommended way to use SeaTunnel Engine in the production environment. Full functionality of SeaTunnel Engine is supported in this mode and the cluster mode will have better performance and stability.

In the cluster mode, the SeaTunnel Engine cluster needs to be deployed first, and the client will submit the job to the SeaTunnel Engine cluster for running.

## Deploy SeaTunnel Engine Cluster

Deploy a SeaTunnel Engine Cluster reference [SeaTunnel Engine Cluster Deploy](deployment.md)

## Submit Job

```shell
$SEATUNNEL_HOME/bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template
```

