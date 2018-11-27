# Installation

## Download

### Community

https://github.com/InterestingLab/waterdrop/releases

### Enterprise

https://pan.baidu.com/s/15y4OcpXIumahiaYM0x6oiw


For business cooperation, consulting, purchasing and product supporting, please contact:
```
Contact Person: Mr. Gao
Mail: garygaowork@gmail.com
```
## Prepare environment

### JDK1.8

Waterdrop relies on the JDK1.8.

### Spark

Waterdrop relies on Spark. Before installing Waterdrop, you need prepare Spark.

Please choose Spark which version >= 2.2.0, you can download Spark on [Download](http://spark.apache.org/downloads.html). After downloading and extracting, you can submit a Waterdrop application with locally mode.

If you want to run an application on Standalone cluster or Yarn or Mesos cluster, please refer to [Spark official website](https://spark.apache.org/docs/latest/cluster-overview.html).

### Installing Waterdrop

Download the Waterdrop package and unzip it. Here is the example of community version:

```
wget https://github.com/InterestingLab/waterdrop/releases/download/v<version>/waterdrop-<version>.zip -O waterdrop-<version>.zip
unzip waterdrop-<version>.zip
ln -s waterdrop-<version> waterdrop
```

There is not any complicated installation steps, the usage of Waterdrop refer to [Quick Start](/en/quick-start.md) and the configuration of Waterdrop refer to [Configuration](/en/configuration/base)

If you want to run an Waterdrop application on Standalone cluster or Yarn or Mesos cluster, please refer to [Waterdrop Deployment](/en/deployment.md).

