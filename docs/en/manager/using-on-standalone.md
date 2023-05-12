---

sidebar_position: 2
-------------------

# Using On Local With Standalone

# savepoint and restore with savepoint

savepoint is created using the checkpoint. a global mirror of job execution status, which can be used for job or seatunnel stop and recovery, upgrade, etc.

## use savepoint

To use savepoint, you need to ensure that the connector used by the job supports checkpoint, otherwise data may be lost or duplicated.

1. Make sure the job is running

2. Use the following command to trigger savepoint:  
   ```./bin/seatunnel.sh -s {jobId}```

After successful execution, the checkpoint data will be saved and the task will end.

## use restore with savepoint

Resume from savepoint using jobId  
```./bin/seatunnel.sh -c {jobConfig} -r {jobId}```


# Submit Job


# Check Job List

# Pause Job （savepoint）

# Renew Job （from the nearest checkpoint）

# Obtain Job Monitoring Information


## What's More

For now, you are already take a quick look about SeaTunnel, you could see [connector](../../connector-v2/source/FakeSource.md) to find all
source and sink SeaTunnel supported. Or see [SeaTunnel Engine](../../seatunnel-engine/about.md) if you want to know more about SeaTunnel Engine.

SeaTunnel also supports running jobs in Spark/Flink. You can see [Quick Start With Spark](quick-start-spark.md) or [Quick Start With Flink](quick-start-flink.md).
