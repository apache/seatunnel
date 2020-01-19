## Waterdrop v2.x 与 v1.x 的区别是什么？

#### v1.x VS v2.x

| - | v1.x | v2.x |
|---|---|---|
| 支持Spark | Yes | Yes |
| 开发Spark插件 | Yes | Yes |
| 支持Flink | No | Yes |
| 开发Flink插件 | No | Yes |
| 支持的Waterdrop运行模式 | local, Spark/Flink Standalone Cluster, on Yarn, on k8s | local, Spark/Flink Standalone Cluster, on Yarn, on k8s |
| 支持SQL计算 | Yes | Yes |
| 配置文件动态变量替换 | Yes | Yes |
| 项目代码编译方式 | sbt(下载依赖很困难，我们正式放弃sbt) | maven |
| 主要编程语言 | scala | java |


备注：
1. Waterdrop v1.x 与 v2.x 还有一个很大的区别，就是配置文件中，input改名为source, filter改名为transform, output改名为sink，如下：

```
# v1.x 的配置文件:
input {}
filter {}
output {}
```

```
# v2.x 的配置文件:
source {} # input -> source
transform {} # filter -> transform
sink {} # output -> sink
```


#### 为什么InterestingLab团队要研发Waterdrop v2.x ?

在2017年的夏天，InterestingLab 团队为了大幅提升海量、分布式数据计算程序的开发效率和运行稳定性，开源了支持Spark流式和离线批计算的Waterdrop v1.x。
直到2019年的冬天，这两年的时间里，Waterdrop逐渐被国内多个一二线互联网公司以及众多的规模较小的创业公司应用到生产环境，持续为其产生价值和收益。
在Github上，目前此项目的Star + Fork 数也超过了[1000+](https://github.com/InterestingLab/waterdrop/)，它的能力和价值得到了充分的认可在。

InterestingLab 坚信，只有真正为用户产生价值的开源项目，才是好的开源项目，这与那些为了彰显自身技术实力，疯狂堆砌功能和高端技术的开源项目不同，它们很少考虑用户真正需要的是什么。
然后，时代是在进步的，InterestingLab也有深深的危机感，无法停留在当前的成绩上不前进。

在2019年的夏天，InterestingLab 做出了一个重要的决策 —— 在Waterdrop上尽快支持Flink，让Flink的用户也能够用上Waterdrop，感受到它带来的实实在在的便利。
终于，在2020年的春节前夕，InterestingLab 正式对外开放了Waterdrop v2.x，一个同时支持Spark(Spark >= 2.2)和Flink(Flink >=1.9)的版本，希望它能帮助到国内庞大的Flink社区用户。

在此特此感谢，Facebook Presto项目，Presto项目是一个非常优秀的开源OLAP查询引擎，提供了丰富的插件化能力。
Waterdrop项目正式学习了它的插件化体系架构之后，在Spark和Flink上研发出的一套插件化体系架构，为Spark和Flink计算程序的插件化开发插上了翅膀。
