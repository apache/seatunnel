---

sidebar_position: 5
-------------------

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
