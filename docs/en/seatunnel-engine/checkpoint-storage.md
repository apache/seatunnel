# Checkpoint Storage
## Introduction
Checkpoint is a fault-tolerant recovery mechanism. This mechanism ensures that when the program is running, it can recover itself even if it suddenly encounters an exception.

### Checkpoint Storage
Checkpoint Storage is a storage mechanism for storing checkpoint data. 

SeaTunnel Engine supports the following checkpoint storage types:

- HDFS (S3,HDFS,LocalFile)
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
                  storageNameSpace: #checkpoint storage parent path, the default value is /seatunnel/checkpoint
                  K1: V1 # plugin other configuration
                  K2: V2 # plugin other configuration   
```
#### S3
S3 base on hdfs-file, so you can refer [hadoop docs](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html) to config s3.

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
            max-concurrent: 5
            tolerable-failure: 2
            storage:
                type: hdfs
                max-retained: 3
                plugin-config:
                    storage-type: s3
                    s3.bucket: your-bucket
                    fs.s3a.access-key: your-access-key
                    fs.s3a.secret-key: your-secret-key
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
      max-concurrent: 5
      tolerable-failure: 2
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage-type: s3
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
          storage-type: hdfs
            fs.defaultFS: hdfs://localhost:9000
            // if you used kerberos, you can config like this:
            kerberosPrincipal: your-kerberos-principal
            kerberosKeytab: your-kerberos-keytab  
```


#### LocalFile
```yaml
seatunnel:
  engine:
    checkpoint:
      interval: 6000
      timeout: 7000
      max-concurrent: 5
      tolerable-failure: 2
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage-type: hdfs
            fs.defaultFS: /tmp/ # Ensure that the directory has written permission 

```
