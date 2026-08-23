# BOS HDFS SDK

Download `bos-hdfs-sdk-1.0.4-community.jar` from:

https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.4-community.jar.zip

Install into the local Maven repository before building:

```bash
mvn install:install-file \
  -Dfile=bos-hdfs-sdk-1.0.4-community.jar \
  -DpomFile=bos-hdfs-sdk-pom.xml
```

> Use **1.0.4+**. SDK 1.0.3 always calls `headBucket` during FileSystem init and ignores
> `fs.bos.bucket.hierarchy=false`, which fails when the AK/SK lacks `HeadBucket` permission.
