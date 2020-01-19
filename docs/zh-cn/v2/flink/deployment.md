# 部署与运行

> Waterdrop For Flink 依赖Java运行环境和Flink，详细的Waterdrop 安装步骤参考[安装Waterdrop](/zh-cn/v2/flink/installation)

下面重点说明不同平台的运行方式:

> 首先编辑解压后waterdrop目录下的config/waterdrop-env.sh, 指定必须环境配置FLINK_HOME

### 在Flink Standalone集群上运行Waterdrop

```
.bin/start-waterdrop-flink.sh --config config-path
# -p 2 指定flink job的并行度为2,还可以指定更多的参数，使用 flink run -h查看
.bin/start-waterdrop-flink.sh -p 2 --config config-path
```
### 在Yarn集群上运行Waterdrop
```
.bin/start-waterdrop-flink.sh -m yarn-cluster --config config-path
# -ynm waterdrop 指定在yarn webUI显示的名称为waterdrop,还可以指定更多的参数，使用 flink run -h查看
.bin/start-waterdrop-flink.sh -m yarn-cluster -ynm waterdrop --config config-path
```


---

可参考: [Flink Yarn Setup](https://ci.apache.org/projects/flink/flink-docs-release-1.9/zh/ops/deployment/yarn_setup.html)


