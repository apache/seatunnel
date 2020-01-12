# 部署与运行

> Waterdrop For Flink 依赖Java运行环境和Flink，详细的Waterdrop 安装步骤参考[安装Waterdrop](/zh-cn/v2/flink/installation)

下面重点说明不同平台的运行方式:

### 在Flink Standalone集群上运行Waterdrop

```
.bin/start-waterdrop-flink.sh --config config-path

```
### 在Yarn集群上运行Waterdrop
```
.bin/start-waterdrop-flink.sh -m yarn-cluster --config config-path
```


---

可参考: [Flink Yarn Setup](https://ci.apache.org/projects/flink/flink-docs-release-1.9/zh/ops/deployment/yarn_setup.html)


