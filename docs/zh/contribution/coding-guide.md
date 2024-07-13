# 编码指南

本指南整体介绍了当前 Apache SeaTunnel 的模块和提交一个高质量 pull request 的最佳实践。

## 模块概述

| 模块名                                    | 介绍                                                                 |
|----------------------------------------|--------------------------------------------------------------------|
| seatunnel-api                          | SeaTunnel connector V2 API 模块                                      |
| seatunnel-common                       | SeaTunnel 通用模块                                                     |
| seatunnel-connectors-v2                | SeaTunnel connector V2 模块, connector V2 处于社区重点开发中                  |
| seatunnel-core/seatunnel-spark-starter | SeaTunnel connector V2 的 Spark 引擎核心启动模块                            |
| seatunnel-core/seatunnel-flink-starter | SeaTunnel connector V2 的 Flink 引擎核心启动模块                            |
| seatunnel-core/seatunnel-starter       | SeaTunnel connector V2 的 SeaTunnel 引擎核心启动模块                        |
| seatunnel-e2e                          | SeaTunnel 端到端测试模块                                                  |
| seatunnel-examples                     | SeaTunnel 本地案例模块， 开发者可以用来单元测试和集成测试                                 |
| seatunnel-engine                       | SeaTunnel 引擎模块, seatunnel-engine 是 SeaTunnel 社区新开发的计算引擎，用来实现数据同步   |
| seatunnel-formats                      | SeaTunnel 格式化模块，用来提供格式化数据的能力                                       |
| seatunnel-plugin-discovery             | SeaTunnel 插件发现模块，用来加载类路径中的SPI插件                                    |
| seatunnel-transforms-v2                | SeaTunnel transform V2 模块, transform V2 处于社区重点开发中                  |
| seatunnel-translation                  | SeaTunnel translation 模块, 用来适配Connector V2 和其他计算引擎， 例如Spark、Flink等 |

## 如何提交一个高质量的Pull Request

1. 创建实体类的时候使用 `lombok` 插件的注解(`@Data` `@Getter` `@Setter` `@NonNull` 等)来减少代码量。在编码过程中优先使用 lombok 插件是一个很好的习惯。

2. 如果你需要在类中使用 log4j 打印日志， 优先使用 `lombok` 中的 `@Slf4j` 注解。

3. SeaTunnel 使用 Github issue 来跟踪代码问题，包括 bugs 和 改进， 并且使用 Github pull request 来管理代码的审查和合并。所以创建一个清晰的 issue 或者 pull request 能让社区更好的理解开发者的意图，最佳实践如下：

   > [目的] [模块名称] [子模块名称] 描述

   1. Pull request 目的包含: `Hotfix`, `Feature`, `Improve`, `Docs`, `WIP`。 请注意如果选择 `WIP`, 你需要使用 github 的 draft pull request。
   2. Issue 目的包含: `Feature`, `Bug`, `Docs`, `Discuss`。
   3. 模块名称: 当前 pull request 或 issue 所涉及的模块名称, 例如: `Core`, `Connector-V2`, `Connector-V1`等。
   4. 子模块名称: 当前 pull request 或 issue 所涉及的子模块名称, 例如:`File` `Redis` `Hbase`等。
   5. 描述: 高度概括下当前 pull request 和 issue 要做的事情，尽量见名知意。

   提示:**更多内容, 可以参考 [Issue Guide](https://seatunnel.apache.org/community/contribution_guide/contribute#issue) 和 [Pull Request Guide](https://seatunnel.apache.org/community/contribution_guide/contribute#pull-request)**

4. 代码片段不要重复。 如果一段代码被使用多次，定义多次不是好的选择，最佳实践是把它公共独立出来让其他模块使用。

5. 当抛出一个异常时， 需要一起带上提示信息并且使异常的范围尽可能地小。抛出过于广泛的异常会让错误处理变得复杂并且容易包含安全问题。例如，如果你的 connector 在读数据的时候遇到 `IOException`， 合理的做法如下：

   ```java
   try {
       // read logic
   } catch (IOException e) {
       throw SeaTunnelORCFormatException("This orc file is corrupted, please check it", e);
   }
   ```

6. Apache 项目的 license 要求很严格， 每个 Apache 项目文件都应该包含一个 license 声明。 在提交 pull request 之前请检查每个新文件都包含 `Apache License Header`。

   ```java
   /*
    * Licensed to the Apache Software Foundation (ASF) under one or more
    * contributor license agreements.  See the NOTICE file distributed with
    * this work for additional information regarding copyright ownership.
    * The ASF licenses this file to You under the Apache License, Version 2.0
    * (the "License"); you may not use this file except in compliance with
    * the License.  You may obtain a copy of the License at
    *
    *    http://www.apache.org/licenses/LICENSE-2.0
    *
    * Unless required by applicable law or agreed to in writing, software
    * distributed under the License is distributed on an "AS IS" BASIS,
    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    * See the License for the specific language governing permissions and
    * limitations under the License.
    */
   ```

7. Apache SeaTunnel 使用 `Spotless` 管理代码风格和格式检查。你可以使用下面的命令来自动修复代码风格问题和格式。

   ```shell
   ./mvnw spotless:apply
   ```

8. 提交 pull request 之前，确保修改后项目编译正常，使用下面命令打包整个项目：

   ```shell
   # 多线程编译
   ./mvnw -T 1C clean package
   ```

   ```shell
   # 单线程编译
   ./mvnw clean package
   ```

9. 提交 pull request 之前，在本地用完整的单元测试和集成测试来检查你的功能性是否正确，最佳实践是用 `seatunnel-examples` 模块的例子去检查多引擎是否正确运行并且结果正确。

10. 如果提交的 pull request 是一个新的特性， 请记得更新文档。

11. 提交 connector 相关的 pull request, 可以通过写 e2e 测试保证鲁棒性，e2e 测试需要包含所有的数据类型，并且初始化尽可能小的 docker 镜像，sink 和 source 的测试用例可以写在一起减少资源的损耗。 可以参考这个不错的例子： [MongodbIT.java](https://github.com/apache/seatunnel/blob/dev/seatunnel-e2e/seatunnel-connector-v2-e2e/connector-mongodb-e2e/src/test/java/org/apache/seatunnel/e2e/connector/v2/mongodb/MongodbIT.java)

12. 类中默认的权限需要使用 `private`， 不可修改的需要设置 `final`， 特殊场景除外。

13. 类中的属性和方法参数倾向于使用基本数据类型(int boolean double float...)， 而不是包装类型(Integer Boolean Double Float...)， 特殊情况除外。

14. 开发一个 sink connector 的时候你需要知道 sink 需要被序列化，如果有不能被序列化的属性， 需要包装到一个类中，并且使用单例模式。

15. 如果代码中有多个 `if` 流程判断， 尽量简化为多个 if 而不是 if-else-if。

16. Pull request 具有单一职责的特点， 不允许在 pull request 包含与该功能无关的代码， 如果有这种情况， 需要在提交 pull request 之前单独处理好， 否则 Apache SeaTunnel 社区会主动关闭 pull request。

17. 贡献者需要对自己的 pull request 负责。 如果 pull request 包含新的特性， 或者修改了老的特性，增加测试用例或者 e2e 用例来证明合理性和保护完整性是一个很好的做法。

18. 如果你认为社区当前某部分代码不合理（尤其是核心的 `core` 和 `api` 模块），有函数需要更新修改，优先使用 `discuss issue` 和 `email` 与社区讨论是否有必要修改，社区同意后再提交 pull request, 请不要不经讨论直接提交 pull request, 社区会认为无效并且关闭。

