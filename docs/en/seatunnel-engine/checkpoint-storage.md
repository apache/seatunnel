---
sidebar_position: 7
---

# Checkpoint Storage

## Introduction

Checkpoint is a fault-tolerant recovery mechanism. This mechanism ensures that when the program is running, it can recover itself even if it suddenly encounters an exception.

### Checkpoint Storage

Checkpoint Storage is a storage mechanism for storing checkpoint data.

SeaTunnel Engine supports the following checkpoint storage types:

- HDFS (OSS,COS,S3,HDFS,LocalFile)
- LocalFile (native), (it's deprecated: use Hdfs(LocalFile) instead.

We use the microkernel design pattern to separate the checkpoint storage module from the engine. This allows users to implement their own checkpoint storage modules.

`checkpoint-storage-api` is the checkpoint storage module API, which defines the interface of the checkpoint storage module.

If you want to implement your own checkpoint storage module, you need to implement the `CheckpointStorage` and provide the corresponding `CheckpointStorageFactory` implementation.

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

Aliyun OSS based hdfs-file you can refer [Hadoop OSS Docs](https://hadoop.apache.org/docs/stable/hadoop-aliyun/tools/hadoop-aliyun/index.html) to config oss.

Except when interacting with oss buckets, the oss client needs the credentials needed to interact with buckets.
The client supports multiple authentication mechanisms and can be configured as to which mechanisms to use, and their order of use. Custom implementations of org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider may also be used.
If you used AliyunCredentialsProvider (can be obtained from the Aliyun Access Key Management), these consist of an access key, a secret key.
You can config like this:

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
```

For additional reading on the Hadoop Credential Provider API, you can see: [Credential Provider API](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

For Aliyun OSS Credential Provider implements, you can see: [Auth Credential Providers](https://github.com/aliyun/aliyun-oss-java-sdk/tree/master/src/main/java/com/aliyun/oss/common/auth)

#### COS

Tencent COS based hdfs-file you can refer [Hadoop COS Docs](https://hadoop.apache.org/docs/stable/hadoop-cos/cloud-storage/) to config COS.

Except when interacting with cos buckets, the cos client needs the credentials needed to interact with buckets.
The client supports multiple authentication mechanisms and can be configured as to which mechanisms to use, and their order of use. Custom implementations of com.qcloud.cos.auth.COSCredentialsProvider may also be used.
If you used SimpleCredentialsProvider (can be obtained from the Tencent Cloud API Key Management), these consist of an access key, a secret key.
You can config like this:

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
          storage.type: cos
          cos.bucket: cosn://your-bucket
          fs.cosn.credentials.provider: org.apache.hadoop.fs.cosn.auth.SimpleCredentialsProvider
          fs.cosn.userinfo.secretId: your-secretId
          fs.cosn.userinfo.secretKey: your-secretKey
          fs.cosn.bucket.region: your-region
```

For additional reading on the Hadoop Credential Provider API, you can see: [Credential Provider API](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

For additional COS configuration, you can see: [Tencent Hadoop-COS Docs](https://doc.fincloud.tencent.cn/tcloud/Storage/COS/846365/hadoop)

Please add the following jar to the lib directory:
- [hadoop-cos-3.4.1.jar](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-cos/3.4.1)
- [cos_api-bundle-5.6.69.jar](https://mvnrepository.com/artifact/com.qcloud/cos_api-bundle/5.6.69)
- [hadoop-shaded-guava-1.1.1.jar](https://mvnrepository.com/artifact/org.apache.hadoop.thirdparty/hadoop-shaded-guava/1.1.1)

#### S3

S3 based hdfs-file you can refer [hadoop s3 docs](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html) to config s3.

Except when interacting with public S3 buckets, the S3A client needs the credentials needed to interact with buckets.
The client supports multiple authentication mechanisms and can be configured as to which mechanisms to use, and their order of use. Custom implementations of com.amazonaws.auth.AWSCredentialsProvider may also be used.
If you used SimpleAWSCredentialsProvider (can be obtained from the Amazon Security Token Service), these consist of an access key, a secret key.
You can config like this:

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
                    fs.s3a.access.key: your-access-key
                    fs.s3a.secret.key: your-secret-key
                    fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
                    

```

If you used `InstanceProfileCredentialsProvider`, which supports use of instance profile credentials if running in an EC2 VM, you can check [iam-roles-for-amazon-ec2](https://docs.aws.amazon.com/zh_cn/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html).
You can config like this:

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

If you want to use Minio that supports the S3 protocol as checkpoint storage, you should configure it this way:

```yaml

seatunnel:
  engine:
    checkpoint:
      interval: 10000
      timeout: 60000
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: s3
          fs.s3a.access.key: xxxxxxxxx # Access Key  of MinIO
          fs.s3a.secret.key: xxxxxxxxxxxxxxxxxxxxx # Secret Key of MinIO
          fs.s3a.endpoint: http://127.0.0.1:9000 # Minio HTTP service access address
          s3.bucket: s3a://test # test is the bucket name which  storage the checkpoint file
          fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
       # important: The user of this key needs to have write permission for the bucket, otherwise an exception of 403 will be returned
```

For additional reading on the Hadoop Credential Provider API, you can see: [Credential Provider API](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

#### HDFS

if you use HDFS, you can config like this:

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
          kerberosKeytabFilePath: your-kerberos-keytab
          // if you need hdfs-site config, you can config like this:
          hdfs_site_path: /path/to/your/hdfs_site_path
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
          seatunnel.hadoop.dfs.client.failover.proxy.provider.usdp-bing: org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider

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

### Enable cache

When storage:type is hdfs, cache is disabled by default. If you want to enable it, set `disable.cache: false`

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
          disable.cache: false
          fs.defaultFS: hdfs:///

```

or

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
          disable.cache: false
          fs.defaultFS: file:///
```

