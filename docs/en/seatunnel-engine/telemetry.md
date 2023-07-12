---

sidebar_position: 8
-------------------

# Telemetry

Integrating `Open Telemetry` through `Prometheus-exports` can better seamlessly connect to related monitoring platforms such as Prometheus and Grafana, improving the ability to monitor and alarm of the Seatunnel cluster.

You can configure the port exposed by the telemetry server in the `seatunnel.yaml` file.

The following is an example declarative configuration.

```yaml
seatunnel:
  engine:
    telemetry: 
      http-port: 1024
```

***********************************************
 Job info detail
***********************************************
createdJobCount           :                   0
scheduledJobCount         :                   0
runningJobCount           :                   0
failingJobCount           :                   0
failedJobCount            :                   0
cancellingJobCount        :                   0
canceledJobCount          :                   0
finishedJobCount          :                   0
restartingJobCount        :                   0
suspendedJobCount         :                   0
reconcilingJobCount       :                   0

***********************************************
     CoordinatorService Thread Pool Status
***********************************************
activeCount               :                   0
corePoolSize              :                   0
maximumPoolSize           :          2147483647
poolSize                  :                   0
completedTaskCount        :                   0
taskCount                 :                   0
***********************************************
