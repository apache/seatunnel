---
sidebar_position: 8
---

# Savepoint And Restore With Savepoint

Savepoint is created for using the checkpoint. A global mirror of job execution status can be used for job or seatunnel stop and recovery, upgrade, etc.

## Use Savepoint

To use savepoint, you need to ensure that the connector used by the job supports checkpoint, otherwise data may be lost or duplicated.

1. Make sure the job is running

2. Use the following command to trigger savepoint:  
   ```./bin/seatunnel.sh -s {jobId}```

After successful execution, the checkpoint data will be saved and the task will end.

## Use Restore With Savepoint

Resume from savepoint using jobId  
```./bin/seatunnel.sh -c {jobConfig} -r {jobId}```
