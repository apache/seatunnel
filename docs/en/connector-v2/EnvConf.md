# EnvConf

This document describes env configuration information,env unifies the environment variables of all engines.

## job.name

This parameter configures the task name.

## jars

Third-party packages can be loaded via `jars`, like `jars="file://local/jar1.jar;file://local/jar2.jar"`

## job.mode

You can configure whether the task is in batch mode or stream mode through `job.mode`, like `job.mode = "BATCH"` or `job.mode = "STREAMING"` 

## checkpoint.interval

Gets the interval in which checkpoints are periodically scheduled.

## parallelism

This parameter configures the parallelism of source and sink.
