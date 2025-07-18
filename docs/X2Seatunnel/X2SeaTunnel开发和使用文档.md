# X2SeaTunnel 开发和使用文档

## 项目概述

X2SeaTunnel 是一个配置转换工具，用于将 DataX、Sqoop 等数据集成工具的配置文件转换为 SeaTunnel 配置格式。

## 项目结构

```
seatunnel/
├── seatunnel-tools/                    # 工具类父模块
│   ├── pom.xml                         # 父POM
│   └── x2seatunnel/                    # X2SeaTunnel子模块
│       ├── pom.xml                     # 子模块POM
│       ├── src/                        # 源代码
│       └── target/                     # 编译输出
├── bin/
│   ├── x2seatunnel.sh                  # Linux/Mac启动脚本
│   └── x2seatunnel.cmd                 # Windows启动脚本
└── examples/
    └── x2seatunnel/                    # 示例配置文件
        ├── datax-mysql2hdfs.json
        └── simple-datax.json
```

## 开发流程

### 1. 环境准备

- **Java**: JDK 8 或更高版本
- **Maven**: 3.6 或更高版本
- **操作系统**: Linux/Mac/Windows

### 2. 编译步骤

#### 2.1 首次编译（包含依赖）
```bash
# 切换到项目根目录
cd /path/to/seatunnel

# 编译必要的依赖模块（首次运行或依赖更新后）
mvn clean install -DskipTests -pl seatunnel-common,seatunnel-config/seatunnel-config-shade -am

# 编译 x2seatunnel 模块
mvn clean compile -pl seatunnel-tools -am
```

#### 2.2 日常开发编译
```bash
# 仅编译 x2seatunnel 模块
cd /path/to/seatunnel
mvn clean compile -pl seatunnel-tools -am

# 或者在子模块目录下编译
cd seatunnel-tools/x2seatunnel
mvn clean compile
```

### 3. 测试

#### 3.1 运行单元测试
```bash
# 在项目根目录
mvn test -pl seatunnel-tools

# 或者在子模块目录
cd seatunnel-tools/x2seatunnel
mvn test
```

#### 3.2 跳过格式检查的测试（开发阶段）
```bash
mvn test -Dspotless.check.skip=true
```

#### 3.3 代码格式化
```bash
# 应用 Spotless 格式化
mvn spotless:apply -pl seatunnel-tools/x2seatunnel

# 或者在子模块目录
cd seatunnel-tools/x2seatunnel
mvn spotless:apply
```

### 4. 打包

#### 4.1 完整打包
```bash
# 在项目根目录，推荐方式
cd /path/to/seatunnel
mvn clean package -pl seatunnel-tools -am -DskipTests
```

#### 4.2 输出文件
打包成功后会生成以下文件：
- `seatunnel-tools/x2seatunnel/target/x2seatunnel-2.3.12-SNAPSHOT-2.12.15.jar` - 完整可执行JAR（约37MB）
- `seatunnel-tools/x2seatunnel/target/original-x2seatunnel-2.3.12-SNAPSHOT-2.12.15.jar` - 原始JAR（约20KB）

## 使用方式

### 1. 命令行参数

```bash
# 基本用法
./bin/x2seatunnel.sh -s <源配置文件> -t <目标配置文件> [选项]

# 查看帮助
./bin/x2seatunnel.sh --help

# 参数说明
-s, --source <file>      源配置文件路径
-t, --target <file>      目标配置文件路径
-st, --source-type <type> 源配置类型 (datax, sqoop)
-tt, --target-type <type> 目标配置类型 (seatunnel)
-r, --report <file>      生成转换报告文件
-h, --help               显示帮助信息
-v, --version            显示版本信息
--verbose                详细输出模式
```

### 2. 使用示例

#### 2.1 DataX 到 SeaTunnel 转换
```bash
# 基本转换
./bin/x2seatunnel.sh -s examples/x2seatunnel/datax-mysql2hdfs.json -t output/seatunnel-config.conf

# 指定类型转换
./bin/x2seatunnel.sh -s examples/x2seatunnel/datax-mysql2hdfs.json -t output/seatunnel-config.conf -st datax -tt seatunnel

# 生成转换报告
./bin/x2seatunnel.sh -s examples/x2seatunnel/datax-mysql2hdfs.json -t output/seatunnel-config.conf -r output/conversion-report.md
```

#### 2.2 批量转换
```bash
# 转换目录下的所有配置文件
./bin/x2seatunnel.sh -s input-dir/ -t output-dir/ -st datax
```

## 开发规范

### 1. 代码风格
- 使用 Spotless 进行代码格式化
- 遵循 Apache SeaTunnel 项目的代码规范
- 提交前必须运行 `mvn spotless:apply`

### 2. 测试规范
- 编写必要的单元测试，覆盖核心功能
- 避免过度细化的测试用例
- 使用 JUnit 5 (`org.junit.jupiter.api.Test`)

### 3. 提交规范
- 提交前确保编译通过：`mvn clean compile -pl seatunnel-tools -am`
- 提交前确保测试通过：`mvn test -pl seatunnel-tools`
- 提交前确保格式检查通过：`mvn spotless:check -pl seatunnel-tools`

## 常见问题解决

### 1. 编译问题

#### 依赖下载失败
```bash
# 清理本地仓库缓存
rm -rf ~/.m2/repository/org/apache/seatunnel

# 重新编译依赖
mvn clean install -DskipTests -pl seatunnel-common,seatunnel-config/seatunnel-config-shade -am
```

#### Spotless 格式检查失败
```bash
# 应用格式化
mvn spotless:apply -pl seatunnel-tools/x2seatunnel

# 跳过格式检查（开发阶段）
mvn compile -Dspotless.check.skip=true
```

### 2. 运行问题

#### Java 版本检查失败
确保 Java 8 或更高版本，并设置正确的 `JAVA_HOME`：
```bash
export JAVA_HOME=/path/to/jdk
export PATH=$JAVA_HOME/bin:$PATH
```

#### 找不到 JAR 文件
确保已经完成打包：
```bash
mvn clean package -pl seatunnel-tools -am -DskipTests
```

### 3. 开发技巧

#### 并行编译依赖
在开发过程中，可以在一个终端窗口中编译依赖：
```bash
mvn clean install -DskipTests -pl seatunnel-common,seatunnel-config/seatunnel-config-shade -am
```

同时在另一个终端窗口中进行开发和测试：
```bash
mvn test -Dspotless.check.skip=true
```

#### 快速验证
```bash
# 编译 + 测试 + 打包一条龙
cd /path/to/seatunnel
mvn clean compile test package -pl seatunnel-tools -am -Dspotless.check.skip=true
```

## 版本历史

- **v1.0-SNAPSHOT**: 初始版本，支持基础的 DataX 到 SeaTunnel 转换
- **迭代 1.1**: 项目基础架构搭建完成

## 贡献指南

1. Fork 项目
2. 创建功能分支
3. 遵循代码规范进行开发
4. 编写测试用例
5. 提交 Pull Request

## 支持

如有问题，请查看：
1. 项目文档：`docs/X2Seatunnel/`
2. 示例配置：`examples/x2seatunnel/`
3. 提交 Issue 到项目仓库
