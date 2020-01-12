# 下载、安装

## 下载

### 社区版本（Community）

https://github.com/InterestingLab/waterdrop/releases

### 商业版本（Enterprise）

https://pan.baidu.com/s/15y4OcpXIumahiaYM0x6oiw

商业合作、咨询、购买以及产品支持，请联系：
```
公司：北京奥龙信科技有限公司
产品：Waterdrop
商务合作联系人：高先生
联系电话：18801136525
邮箱：garygaowork@gmail.com
```
## 环境准备

### 准备好JDK1.8

Waterdrop 依赖JDK1.8运行环境。

### 准备好Flink
 
请先[下载Flink](https://flink.apache.org/downloads.html), Flink版本请选择 >= 1.9.0。下载完成进行[安装](https://ci.apache.org/projects/flink/flink-docs-release-1.9/zh/ops/deployment/cluster_setup.html)

### 安装Waterdrop

下载Waterdrop安装包并解压， 这里以社区版为例:

```
wget https://github.com/InterestingLab/waterdrop/releases/download/v<version>/waterdrop-<version>.zip -O waterdrop-<version>.zip
unzip waterdrop-<version>.zip
ln -s waterdrop-<version> waterdrop
```

没有任何复杂的安装配置步骤，Waterdrop的使用方法请参考[Quick Start](/zh-cn/v2/flink/quick-start.md), 配置请参考[Configuration](/zh-cn/v2/flink/configuration/)。

如果想把Waterdrop部署在Spark Standalone/Yarn集群上运行，请参考[Waterdrop部署](/zh-cn/v2/flink/deployment)

