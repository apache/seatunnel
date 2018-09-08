# 深入 Waterdrop

## Waterdrop 努力改善多处痛点

除了大大简化分布式数据处理难度外，Waterdrop尽所能为您解决可能遇到的问题：

* 数据丢失与重复

如 Waterdrop 的 Kafka Input 是通过 Kafka Direct API 实现的，同时通过checkpoint机制或者支持幂等写入的Output的支持，实现了exactly once操作。此外Waterdrop的项目代码经过了详尽测试，尽可能减少了因数据处理异常导致的数据意外丢弃。

* 任务堆积与延迟

在线上环境，存在大量的Spark任务或者包含较多task的单个stage的Spark运行环境中，我们多次遇到单个task处理时间较长，拖慢了整个batch的情况。Waterdrop默认开启了Spark推测执行的功能，推测执行功能会找到慢task并启动新的task，并以先完成的task作为结算结果。

* 吞吐量低

Waterdrop 的代码实现中，直接利用了多项在实践中被证明有利于提升处理性能的Spark的高级特性，如：

（1）在核心流程代码中，使用Dataset，Spark SQL  编程API，有效利用了Spark 的catalyst优化器。

（2）支持插件实现中使用broadcast variable，对于IP库解析，写数据库链接维护这样的应用场景，能起到优化作用。

（3）在插件的实现代码中，性能始终是我们优先考虑的因素。

* 应用到生产环境周期长

使用 Waterdrop 可以做到开箱即用，在安装、部署、启动上做了多处简化；插件体系容易配置和部署，开发者能够很快在 Waterdrop 中集成特定业务逻辑。

* 缺少应用运行状态监控

（1）Waterdrop 自带监控工具 `Guardian`，是 Waterdrop 的子项目，可监控 Waterdrop 是否存活，并能够根据配置自动拉起 Waterdrop 实例；可监控其运行时streaming batch是否存在堆积和延迟，并发送报警。

（2）下一个release版本中将加入数据处理各阶段耗时统计，方便做性能优化。

