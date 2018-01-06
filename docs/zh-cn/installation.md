# 安装

### 准备好JDK1.8

Waterdrop 依赖JDK1.8运行环境。

### 准备好Spark
 
Waterdrop 依赖Spark，安装Waterdrop前，需要先准备好Spark。
请先[下载Spark](http://spark.apache.org/downloads.html), Spark版本请选择 >= 2.x.x。下载解压后，不需要做任何配置即可提交Spark deploy-mode = local模式的任务。
如果你期望任务运行在Standalone集群或者Yarn、Mesos集群上，请参考Spark官网配置文档。

### 安装Waterdrop

下载[Waterdrop安装包](https://github.com/InterestingLab/waterdrop/releases) 并解压:

```
wget https://github.com/InterestingLab/waterdrop/releases/download/v1.0.0/waterdrop-1.0.0.zip -O waterdrop-1.0.0.zip
unzip waterdrop-1.0.0.zip
ln -s waterdrop-1.0.0 waterdrop
```

没有任何复杂的安装配置步骤，Waterdrop的使用方法请参考[Quick Start](/zh-cn/quick-start.md), 配置请参考[Configuration](/zh-cn/configuration/base)。

如果想把Waterdrop部署在Spark Standalone/Yarn/Mesos集群上运行，请参考[Waterdrop部署](/zh-cn/deployment)

