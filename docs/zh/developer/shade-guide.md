---
sidebar_position: 7
---

# SeaTunnel Shade 指南

## 概述

[Apache SeaTunnel Shade](https://github.com/apache/seatunnel-shade) 是一个独立仓库，为 Apache SeaTunnel 提供 **shaded（包重定位）** 的第三方库 JAR 包。

每个模块封装一个第三方库，将其包名重定位到 `org.apache.seatunnel.shade.*` 下，避免 SeaTunnel 自身或其连接器因依赖同一库的不同版本而产生类路径冲突。

例如，`seatunnel-shade-guava` 将 Guava `27.0-jre` 重定位到 `org.apache.seatunnel.shade.com.google.common.*`，这样 SeaTunnel 内部可以使用不同版本的 Guava 而不发生冲突。

## 可用模块

| 模块 | 封装库 | 版本 |
|------|--------|------|
| `seatunnel-shade-guava` | Guava | 27.0-jre |
| `seatunnel-shade-jackson` | Jackson | 2.15.4 |
| `seatunnel-shade-commons-lang3` | Commons Lang3 | 3.18.0 |
| `seatunnel-shade-arrow` | Apache Arrow | 15.0.1 |
| `seatunnel-shade-hikari` | HikariCP | 4.0.3 |
| `seatunnel-shade-hazelcast` | Hazelcast | 5.1 |
| `seatunnel-shade-hadoop3-uber` | Hadoop Client | 3.1.4 |
| `seatunnel-shade-hadoop-aws` | Hadoop AWS | 3.1.4 |
| `seatunnel-shade-scala-compiler` | Scala Compiler | 2.12.15 |
| `seatunnel-shade-janino` | Janino | 3.1.12 |
| `seatunnel-shade-calcite` | Apache Calcite | 1.38.0 |
| `seatunnel-shade-jetty` | Jetty | 9.4.56 |
| `seatunnel-shade-thrift-service` | Apache Doris Thrift | 1.0.0 |

## 版本约定

Shade 模块的版本号格式为：

```
${library.version}-${seatunnel.shade.version}
```

例如：`seatunnel-shade-guava-27.0-jre-3.0.0.jar`

- `27.0-jre` 是被封装的第三方库（Guava）的版本
- `3.0.0` 是 SeaTunnel shade 版本（由 `seatunnel.shade.version` 属性控制）

## 仓库地址

- **GitHub**: [https://github.com/apache/seatunnel-shade](https://github.com/apache/seatunnel-shade)
- **制品**: 发布到 [Maven Central](https://search.maven.org/search?q=org.apache.seatunnel)，groupId 为 `org.apache.seatunnel`

## SeaTunnel 如何消费 Shade 模块

在 SeaTunnel 的根 `pom.xml` 中，所有 shade 模块在 `<dependencyManagement>` 中集中管理：

```xml
<properties>
    <seatunnel.shade.version>3.0.0</seatunnel.shade.version>
    <seatunnel.shade.guava.version>27.0-jre</seatunnel.shade.guava.version>
    <seatunnel.shade.hadoop.version>3.1.4</seatunnel.shade.hadoop.version>
    <!-- ... 其他 shade 模块版本 ... -->
</properties>

<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.apache.seatunnel</groupId>
            <artifactId>seatunnel-shade-guava</artifactId>
            <version>${seatunnel.shade.guava.version}-${seatunnel.shade.version}</version>
        </dependency>
        <!-- ... 其他 shade 模块 ... -->
    </dependencies>
</dependencyManagement>
```

子模块声明依赖时**不写** `<version>` 统一版本从根 `dependencyManagement` 继承：

```xml
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-shade-guava</artifactId>
</dependency>
```

## 构建 Shade 工程

### 前置条件

- Java 11+
- Maven 3.x

### 克隆并构建

```bash
git clone https://github.com/apache/seatunnel-shade.git
cd seatunnel-shade

# 完整构建（跳过测试，跳过 RAT 许可证检查）
mvn -B -DskipTests -Drat.skip=true clean install

# 并行构建（2 倍核心数）
mvn -B -DskipTests -Drat.skip=true -T 2C clean install

# 构建单个模块及其依赖
mvn -B -DskipTests -pl seatunnel-shade-guava -am clean install
```

## 发版流程

### 何时需要发版

以下情况需要发布 shade 模块：

- 升级了第三方库的版本（如 Guava 27.0 → 33.0）
- 新增了 shade 模块
- 变更了 `seatunnel.shade.version`（影响**所有**模块）

### 全量发版（所有模块）

将所有模块部署到 Apache Maven 仓库：

```bash
mvn -B -DskipTests -Drat.skip=true clean deploy
```

### 部分发版（指定模块）

当变更只影响部分模块时，使用 `-pl` 仅部署变更的模块。**禁止不加 `-pl` 运行 `mvn clean deploy`**——未变更的模块会被重复发布，Nexus 会返回 400 错误拒绝。

```bash
# 1. 本地验证（跳过 GPG 签名）
mvn -B -DskipTests -Drat.skip=true -Dgpg.skip=true \
  -pl seatunnel-shade-calcite,seatunnel-shade-janino,seatunnel-shade-scala-compiler \
  clean install

# 2. 部署到 Apache 仓库
mvn -B -DskipTests -Drat.skip=true \
  -pl seatunnel-shade-calcite,seatunnel-shade-janino,seatunnel-shade-scala-compiler \
  clean deploy
```

:::warning

模块版本遵循 `${library.version}-${seatunnel.shade.version}` 格式。只要库版本发生变化，就不会与已发布的制品冲突。如果只变更了 shade 插件配置（库版本未变），则必须在部署前手动升级版本号。

:::

### 打 Tag

发版成功后，为提交打上 tag：

```bash
git tag v3.0.0.fix
git push origin v3.0.0.fix
```

## 新增 Shade 模块

1. 在 shade 工程的根 `pom.xml` 的 `<modules>` 中添加新模块。

2. 添加版本属性：
   ```xml
   <seatunnel.shade.newlib.version>X.Y.Z</seatunnel.shade.newlib.version>
   ```

3. 参照模板创建模块 POM：

   ```xml
   <project xmlns="http://maven.apache.org/POM/4.0.0" ...>
       <modelVersion>4.0.0</modelVersion>
       <parent>
           <groupId>org.apache.seatunnel</groupId>
           <artifactId>seatunnel-shade</artifactId>
           <version>3.0.0</version>
       </parent>

       <artifactId>seatunnel-shade-newlib</artifactId>
       <version>${newlib.version}-${seatunnel.shade.version}</version>

       <dependencies>
           <dependency>
               <groupId>com.example</groupId>
               <artifactId>newlib</artifactId>
               <version>${newlib.version}</version>
           </dependency>
       </dependencies>

       <build>
           <plugins>
               <plugin>
                   <groupId>org.apache.maven.plugins</groupId>
                   <artifactId>maven-shade-plugin</artifactId>
                   <executions>
                       <execution>
                           <goals><goal>shade</goal></goals>
                           <phase>package</phase>
                           <configuration>
                               <relocations>
                                   <relocation>
                                       <pattern>com.example</pattern>
                                       <shadedPattern>${seatunnel.shade.package}.com.example</shadedPattern>
                                   </relocation>
                               </relocations>
                           </configuration>
                       </execution>
                   </executions>
               </plugin>
           </plugin>
       </build>
   </project>
   ```

4. 发布新模块。

5. 在 SeaTunnel 工程中：
   - 在根 `pom.xml` 中添加版本属性
   - 在根 `<dependencyManagement>` 中添加依赖条目
   - 在需要该模块的连接器或模块中添加依赖

## Shade 发版后更新 SeaTunnel

发布新的 shade 制品后，更新 SeaTunnel 工程：

| 步骤 | 位置 |
|------|------|
| 升级版本属性 | 根 `pom.xml` → `<properties>` → `seatunnel.shade.<lib>.version` |
| 声明依赖（新增模块） | 根 `pom.xml` → `<dependencyManagement>` |
| 更新代码 import | 所有使用该库的 `.java` 文件 |

如果仅变更了 `seatunnel.shade.version`（如 `3.0.0` → `3.0.1`），则会影响到**所有** shade 依赖，每个模块都必须重新发布。

### Shade 发版到 SeaTunnel CI 通过之间的传播延迟

新的 `seatunnel-shade-*` 制品发布到 [Apache 发布仓库](https://repository.apache.org/content/repositories/releases/) 后，Maven Central 的全球 CDN 通常需要 **最长约 24 小时** 才会拉取并分发到所有镜像。在这个时间窗口内，SeaTunnel 主工程（依赖通过 Maven Central 解析）会无法下载新发布的 shade 制品，CI 会以 `Could not find artifact org.apache.seatunnel:seatunnel-shade-*:...` 失败。

如果在 shade 发版后立即在 SeaTunnel 根 `pom.xml` 里升版本号，预计 CI 会在第一天左右持续红，直到 Central 传播完成。如果希望 CI 立刻通过，请等 Central 同步后再升 SeaTunnel 版本号（或者隔天对失败的 commit 重新触发 CI — 不需要改代码）。

可以通过以下方式确认制品已在 Central 上线：

- 访问 https://search.maven.org/search?q=org.apache.seatunnel 查看
- 或直接 curl：

```bash
curl -sI https://repo.maven.apache.org/maven2/org/apache/seatunnel/seatunnel-shade-guava/<lib.version>-<shade.version>/seatunnel-shade-guava-<lib.version>-<shade.version>.pom
```

返回 `200 OK` 即代表 Central 已经传播该发布。
