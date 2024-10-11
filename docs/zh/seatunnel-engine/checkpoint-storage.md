---
sidebar_position: 7
---

# 检查点存储

## 简介

检查点是一种容错恢复机制。这种机制确保程序在运行时，即使突然遇到异常，也能自行恢复。

### 检查点存储

SeaTunnel Engine支持以下检查点存储类型:

- HDFS (OSS,S3,HDFS,LocalFile)
- LocalFile (本地)，(已弃用: 使用HDFS(LocalFile)替代).

我们使用微内核设计模式将检查点存储模块从引擎中分离出来。这允许用户实现他们自己的检查点存储模块。

`checkpoint-storage-api`是检查点   存储模块API，它定义了检查点存储模块的接口。

如果你想实现你自己的检查点存储模块，你需要实现`CheckpointStorage`并提供相应的`CheckpointStorageFactory`实现。

### 检查点存储配置

`seatunnel-server`模块的配置在`seatunnel.yaml`文件中。

```yaml

seatunnel:
    engine:
        checkpoint:
            storage:
                type: hdfs #检查点存储的插件名称，支持hdfs(S3, local, hdfs), 默认为localfile (本地文件), 但这种方式已弃用
              # 插件配置
                plugin-config: 
                  namespace: #检查点存储父路径，默认值为/seatunnel/checkpoint/
                  K1: V1 # 插件其它配置
                  K2: V2 # 插件其它配置  
```

注意: namespace必须以"/"结尾。

#### OSS

阿里云OSS是基于hdfs-file，所以你可以参考[Hadoop OSS文档](https://hadoop.apache.org/docs/stable/hadoop-aliyun/tools/hadoop-aliyun/index.html)来配置oss.

OSS buckets交互外，oss客户端需要与buckets交互所需的凭据。
客户端支持多种身份验证机制，并且可以配置使用哪种机制及其使用顺序。也可以使用of org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider的自定义实现。
如果您使用AliyunCredentialsProvider(可以从阿里云访问密钥管理中获得)，它们包括一个access key和一个secret key。
你可以这样配置:

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

有关Hadoop Credential Provider API的更多信息，请参见: [Credential Provider API](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

阿里云OSS凭证提供程序实现见: [验证凭证提供](https://github.com/aliyun/aliyun-oss-java-sdk/tree/master/src/main/java/com/aliyun/oss/common/auth)

#### S3

S3基于hdfs-file，所以你可以参考[Hadoop s3文档](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)来配置s3。

除了与公共S3 buckets交互之外，S3A客户端需要与buckets交互所需的凭据。
客户端支持多种身份验证机制，并且可以配置使用哪种机制及其使用顺序。也可以使用com.amazonaws.auth.AWSCredentialsProvider的自定义实现。
如果您使用SimpleAWSCredentialsProvider(可以从Amazon Security Token服务中获得)，它们包括一个access key和一个secret key。
您可以这样配置:

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

如果您使用`InstanceProfileCredentialsProvider`，它支持在EC2 VM中运行时使用实例配置文件凭据，您可以检查[iam-roles-for-amazon-ec2](https://docs.aws.amazon.com/zh_cn/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html).
您可以这样配置:

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

有关Hadoop Credential Provider API的更多信息，请参见: [Credential Provider API](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

#### HDFS

如果您使用HDFS，您可以这样配置:

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
          // 如果您使用kerberos，您可以这样配置:
          kerberosPrincipal: your-kerberos-principal
          kerberosKeytabFilePath: your-kerberos-keytab
```

如果HDFS是HA模式，您可以这样配置:

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

如果HDFS在`hdfs-site.xml`或`core-site.xml`中有其他配置，只需使用`seatunnel.hadoop.`前缀设置HDFS配置即可。

#### 本地文件

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
          fs.defaultFS: file:/// # 请确保该目录具有写权限

```

### 开启高速缓存

当storage:type为hdfs时，默认关闭cache。如果您想启用它，请设置为`disable.cache: false`。

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
          fs.defaultFS: hdfs:/// # Ensure that the directory has written permission
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

