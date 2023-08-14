---

sidebar_position: 7
-------------------

# Checkpoint Storage

## Introduction

Checkpoint is a fault-tolerant recovery mechanism. This mechanism ensures that when the program is running, it can recover itself even if it suddenly encounters an exception.

### Checkpoint Storage

Checkpoint Storage is a storage mechanism for storing checkpoint data.

SeaTunnel Engine supports the following checkpoint storage types:

- HDFS (OSS,S3,HDFS,LocalFile)
- LocalFile (native), (it's deprecated: use Hdfs(LocalFile) instead.

We used the microkernel design pattern to separate the checkpoint storage module from the engine. This allows users to implement their own checkpoint storage modules.

`checkpoint-storage-api` is the checkpoint storage module API, which defines the interface of the checkpoint storage module.

if you want to implement your own checkpoint storage module, you need to implement the `CheckpointStorage` and provide the corresponding `CheckpointStorageFactory` implementation.

### Checkpoint Storage Configuration

The configuration of the `seatunnel-server` module is in the `seatunnel.yaml` file.

```yaml

seatunnel:
    engine:
        checkpoint:
            storage:
                type: hdfs #plugin name of checkpoint storage, we support hdfs(S3, local, hdfs), localfile (native local file) is the default, but this plugin is de
              # plugin configuration
                plugin-config: 
                  namespace: #checkpoint storage parent path, the default value is /seatunnel/checkpoint/
                  K1: V1 # plugin other configuration
                  K2: V2 # plugin other configuration   
```

Notice: namespace must end with "/".

#### OSS

Aliyun oss base on hdfs-file, so you can refer [hadoop oss docs](https://hadoop.apache.org/docs/stable/hadoop-aliyun/tools/hadoop-aliyun/index.html) to config oss.

Except when interacting with oss buckets, the oss client needs the credentials needed to interact with buckets.
The client supports multiple authentication mechanisms and can be configured as to which mechanisms to use, and their order of use. Custom implementations of org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider may also be used.
if you used AliyunCredentialsProvider (can be obtained from the Aliyun Access Key Management), these consist of an access key, a secret key.
you can config like this:

```yaml
seatunnel:
  engine:
    checkpoint:
      interval: 6000
      timeout: 7000
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: oss
          oss.bucket: your-bucket
          fs.oss.accessKeyId: your-access-key
          fs.oss.accessKeySecret: your-secret-key
          fs.oss.endpoint: endpoint address
          fs.oss.credentials.provider: org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider
```

For additional reading on the Hadoop Credential Provider API see: [Credential Provider API](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

Aliyun oss Credential Provider implements see: [Auth Credential Providers](https://github.com/aliyun/aliyun-oss-java-sdk/tree/master/src/main/java/com/aliyun/oss/common/auth)

#### S3

S3 base on hdfs-file, so you can refer [hadoop s3 docs](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html) to config s3.

Except when interacting with public S3 buckets, the S3A client needs the credentials needed to interact with buckets.
The client supports multiple authentication mechanisms and can be configured as to which mechanisms to use, and their order of use. Custom implementations of com.amazonaws.auth.AWSCredentialsProvider may also be used.
if you used SimpleAWSCredentialsProvider (can be obtained from the Amazon Security Token Service), these consist of an access key, a secret key.
you can config like this:

```yaml
``` yaml

seatunnel:
    engine:
        checkpoint:
            interval: 6000
            timeout: 7000
            storage:
                type: hdfs
                max-retained: 3
                plugin-config:
                    storage.type: s3
                    s3.bucket: your-bucket
                    fs.s3a.access.key: your-access-key
                    fs.s3a.secret.key: your-secret-key
                    fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
                    

```

if you used `InstanceProfileCredentialsProvider`, this supports use of instance profile credentials if running in an EC2 VM, you could check [iam-roles-for-amazon-ec2](https://docs.aws.amazon.com/zh_cn/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html).
you can config like this:

```yaml

seatunnel:
  engine:
    checkpoint:
      interval: 6000
      timeout: 7000
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: s3
          s3.bucket: your-bucket
          fs.s3a.endpoint: your-endpoint
          fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.InstanceProfileCredentialsProvider
```

For additional reading on the Hadoop Credential Provider API see: [Credential Provider API](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

#### HDFS

if you used HDFS, you can config like this:

```yaml
seatunnel:
  engine:
    checkpoint:
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: hdfs
          fs.defaultFS: hdfs://localhost:9000
          // if you used kerberos, you can config like this:
          kerberosPrincipal: your-kerberos-principal
          kerberosKeytab: your-kerberos-keytab  
```

if HDFS is in HA mode , you can config like this:

```yaml
seatunnel:
  engine:
    checkpoint:
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: hdfs
          fs.defaultFS: hdfs://usdp-bing
          seatunnel.hadoop.dfs.nameservices: usdp-bing
          seatunnel.hadoop.dfs.ha.namenodes.usdp-bing: nn1,nn2
          seatunnel.hadoop.dfs.namenode.rpc-address.usdp-bing.nn1: usdp-bing-nn1:8020
          seatunnel.hadoop.dfs.namenode.rpc-address.usdp-bing.nn2: usdp-bing-nn2:8020
          seatunnel.hadoop.dfs.client.failover.proxy.provider.usdp-bing: org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"

```

if HDFS has  some other configs in `hdfs-site.xml` or `core-site.xml` , just set HDFS config by using  `seatunnel.hadoop.`  prefix.

#### LocalFile

```yaml
seatunnel:
  engine:
    checkpoint:
      interval: 6000
      timeout: 7000
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: hdfs
          fs.defaultFS: file:/// # Ensure that the directory has written permission 

```

