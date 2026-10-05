# BOS HDFS SDK (runtime dependency)

`bos-hdfs-sdk` is **not published to Maven Central**. SeaTunnel does not declare it as a Maven
dependency; users must install the jar at **runtime** only.

## Download

Download `bos-hdfs-sdk-1.0.4-community.jar` from:

https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.4-community.jar.zip

## Install

Copy the jar into `${SEATUNNEL_HOME}/lib` before running BosFile jobs (same as documented in
`docs/en/connectors/source/BosFile.md`).

> Use **1.0.4+**. SDK 1.0.3 always calls `headBucket` during FileSystem init and ignores
> `fs.bos.bucket.hierarchy=false`, which fails when the AK/SK lacks `HeadBucket` permission.
