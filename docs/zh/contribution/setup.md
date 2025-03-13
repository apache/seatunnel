# 搭建开发环境

在这个章节， 我们会向你展示如何搭建 SeaTunnel 的开发环境， 然后用 JetBrains IntelliJ IDEA 跑一个简单的示例。

> 你可以用任何你喜欢的开发环境进行开发和测试，我们只是用 [JetBrains IDEA](https://www.jetbrains.com/idea/)
> 作为示例来展示如何一步步完成设置。

## 准备

在设置开发环境之前， 需要做一些准备工作， 确保你安装了以下软件：

* 安装 [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)。
* 安装 [Java](https://www.java.com/en/download/) (目前只支持 JDK8/JDK11) 并且设置 `JAVA_HOME` 环境变量。
* 安装 [Scala](https://www.scala-lang.org/download/2.11.12.html) (目前只支持 scala 2.11.12)。
* 安装 [JetBrains IDEA](https://www.jetbrains.com/idea/)。

## 设置

### 克隆源码

首先使用以下命令从 [GitHub](https://github.com/apache/seatunnel) 克隆 SeaTunnel 源代码。

```shell
git clone git@github.com:apache/seatunnel.git
```

### 本地安装子项目

在克隆好源代码以后， 运行 `./mvnw` 命令安装子项目到 maven 本地仓库目录。 否则你的代码无法在 IDEA 中正常启动。

```shell
./mvnw install -Dmaven.test.skip
```

### 源码编译

在安装 maven 以后， 可以使用下面命令进行编译和打包。

```
mvn clean package -pl seatunnel-dist -am -Dmaven.test.skip=true
```

### 编译子模块

如果要单独编译子模块， 可以使用下面的命令进行编译和打包。

```ssh
# 这是一个单独构建 redis connector 的示例

 mvn clean package -pl seatunnel-connectors-v2/connector-redis -am -DskipTests -T 1C
```

### 安装 JetBrains IDEA Scala 插件

用 JetBrains IntelliJ IDEA 打开你的源码，如果有 Scala 的代码，则需要安装 JetBrains IntelliJ IDEA's [Scala plugin](https://plugins.jetbrains.com/plugin/1347-scala)。
可以参考 [install plugins for IDEA](https://www.jetbrains.com/help/idea/managing-plugins.html#install-plugins) 。

### 安装 JetBrains IDEA Lombok 插件

在运行示例之前, 安装 JetBrains IntelliJ IDEA 的 [Lombok plugin](https://plugins.jetbrains.com/plugin/6317-lombok)。
可以参考 [install plugins for IDEA](https://www.jetbrains.com/help/idea/managing-plugins.html#install-plugins) 。

### 代码风格

Apache SeaTunnel 使用 `Spotless` 来统一代码风格和格式检查。可以运行下面 `Spotless` 命令自动格式化。

```shell
./mvnw spotless:apply
```

拷贝 `pre-commit hook` 文件 `/tools/spotless_check/pre-commit.sh` 到你项目的 `.git/hooks/` 目录， 这样每次你使用 `git commit` 提交代码的时候会自动调用 `Spotless` 修复格式问题。

## 运行一个简单的示例

完成上面所有的工作后，环境搭建已经完成， 可以直接运行我们的示例了。 所有的示例在 `seatunnel-examples` 模块里， 你可以随意选择进行编译和调试，参考 [running or debugging
it in IDEA](https://www.jetbrains.com/help/idea/run-debug-configuration.html)。

我们使用 `seatunnel-examples/seatunnel-engine-examples/src/main/java/org/apache/seatunnel/example/engine/SeaTunnelEngineLocalExample.java`
作为示例, 运行成功后的输出如下:

```log
2024-08-10 11:45:32,839 INFO  org.apache.seatunnel.core.starter.seatunnel.command.ClientExecuteCommand - 
***********************************************
           Job Statistic Information
***********************************************
Start Time                : 2024-08-10 11:45:30
End Time                  : 2024-08-10 11:45:32
Total Time(s)             :                   2
Total Read Count          :                   5
Total Write Count         :                   5
Total Failed Count        :                   0
***********************************************
```

## 更多信息

所有的实例都用了简单的 source 和 sink， 这样可以使得运行更独立和更简单。
你可以修改 `resources/examples` 中的示例的配置。 例如下面的配置使用 PostgreSQL 作为源，并且输出到控制台。
请注意引用FakeSource 和 Console 以外的连接器时，需要修改seatunnel-example对应子模块下的`pom.xml`文件中的依赖。

```conf
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
    Jdbc {
        driver = org.postgresql.Driver
        url = "jdbc:postgresql://host:port/database"
        user = "postgres"
        password = "123456"
        query = "select * from test"
        table_path = "database.test"
    }
}

sink {
  Console {}
}
```

