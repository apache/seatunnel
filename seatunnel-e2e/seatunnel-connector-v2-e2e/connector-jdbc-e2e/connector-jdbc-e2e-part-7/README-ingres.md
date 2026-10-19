<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Ingres JDBC 实例验证

`JdbcIngresIT` 默认跳过。它使用已有的、已获许可的 Ingres 实例和本地驱动，
不下载数据库镜像、不接受商业许可，也不将驱动加入 Maven 依赖。

没有现成实例时，可以申请 [Actian Ingres 12.1 的 30 天评估版](https://esd.actian.com/product/Ingres/12.1/Linux_X86_64-bit/Actian_Ingres_12.1_Evaluation_Edition)。
官方要求先向 `eval@actian.com` 申请评估许可证才能激活。获取许可并启动实例后，再使用下列参数运行测试。

运行前需要：

- 可运行 SeaTunnel E2E 的 Docker 环境，以及已构建的引擎和连接器。
- 从测试 JVM 和引擎容器均可访问的 Ingres JDBC URL。
- 对指定 owner 具有 CREATE TABLE、INSERT、SELECT、DROP TABLE 和元数据查询权限的测试账号。
- 自行从 Actian 获取的 `iijdbc.jar`。候选驱动为 `com.ingres.jdbc:iijdbc:12.1-4.6.5`；
  请在实际运行记录中填写服务端和驱动的准确版本，不能将候选版本视为已经验证。

通过环境变量配置以下参数，避免将密码写入命令行：

| 变量 | 内容 |
| --- | --- |
| `INGRES_E2E` | `true`，显式启用测试 |
| `INGRES_E2E_URL` | 例如 `jdbc:ingres://ingres.example.com:II7/testdb` |
| `INGRES_E2E_USER` | 测试账号 |
| `INGRES_E2E_PASSWORD` | 测试密码 |
| `INGRES_E2E_OWNER` | 已存在的 owner，填写实际大小写，不加 SQL 引号，不包含点号 |
| `INGRES_E2E_DRIVER_JAR` | 本地 `iijdbc.jar` 的绝对路径 |
| `RUN_ENGINES` | `zeta` |

在仓库根目录执行：

```shell
./mvnw -pl seatunnel-e2e/seatunnel-connector-v2-e2e/connector-jdbc-e2e/connector-jdbc-e2e-part-7 -am \
  -DskipUT -DskipIT=false -Dit.test=JdbcIngresIT -Dfailsafe.failIfNoSpecifiedTests=false verify
```

测试会创建具有随机名称的 `st_ingres_*` 表，插入两行数据，通过 JDBC Source 发现
owner 表的元数据，分别按主键读取两行，由 Assert sink 检查每次的行数、主键值、NULL 和字段值。数据包含整数主键、
带大小写且需要引用的 `Order` 字段、NULL 字符串和 `DECIMAL(10,2)`。
测试结束时只删除此次创建的表。如果进程被强制终止，需要根据测试日志手动清理该测试表。
连接配置仅复制到测试容器，驱动也仅在测试期间复制到容器。

这条路径验证标准 JDBC Source，不验证原生 upsert、Ingres 专有类型、CDC、catalog 或建表功能。
未提供实例时，测试的 skipped 结果不能作为真实兼容性通过的证据。
