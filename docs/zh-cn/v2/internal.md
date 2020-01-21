## 深入Waterdrop

#### 基本原理

本质上，Waterdrop不是对Spark和Flink内部的修改，而是在Spark和Flink的基础上，做了一个平台化和产品化的包装，使广大开发者使用Spark和Flink的时候更加简单和易用，主要有两个亮点：

* 完全可以做到开箱即用

* 开发者可以开发自己的插件，plugin in 到 Waterdrop上跑，而不需要写一个完整的Spark或者Flink程序

* 当然，Waterdrop从v2.0开始，同时支持Spark和Flink。

如果想了解Waterdrop的实现原理，建议熟练掌握一个最重要的设计模式：控制反转(或者叫依赖注入)，这是Waterdrop实现的基本思想。控制反转(或者叫依赖注入)是什么？我们用两句话来总结：

* 上层不依赖底层，两者依赖抽象。

* 流程代码与业务逻辑应该分离。

如果想进一步了解Waterdrop的设计与实现原理，请查看视频：https://time.geekbang.org/dailylesson/detail/100028486

#### Waterdrop的插件化体系是如何构建出来的？

可能有些同学听说过Google Guice，它是一个非常优秀的依赖注入框架，很多开源项目如Elasticsearch就是利用的Guice实现的各个模块依赖的注入，当然包括Elasticsearch 插件。

其实对于插件体系架构来说，java本身就带了一个可以用来实现插件化的功能，即[Java SPI](https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html)，开源项目Presto中也大量使用了它来加载插件。

Waterdrop的项目负责人是Presto源码的重度粉丝，所以Waterdrop参考了Presto的插件管理方式，也使用了Java SPI来实现了插件化的管理。
