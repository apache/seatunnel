# 贡献 Transform 指南

本文档介绍了如何理解、开发和贡献 transform。

我们还提供了 [Transform E2E 测试](../seatunnel-e2e/seatunnel-transforms-v2-e2e) 来验证 transform 的数据输入和输出。

## 概念

使用 SeaTunnel，你可以通过连接器读取或写入数据，但如果你需要在读取数据后或写入数据前处理数据，就需要使用 transform。

使用 transform 可以对数据行或字段进行简单的编辑，例如拆分字段、修改字段值、添加或删除字段。

### 数据类型 Transform

Transform 从上游（源或 transform）接收数据类型输入，并将新的数据类型输出到下游（接收器或 transform）。这个过程就是数据类型转换。

示例 1：删除字段

```shell
| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |

| A         | B         |
|-----------|-----------|
| STRING    | INT       |
```

示例 2：排序字段

```shell
| B         | C         | A         |
|-----------|-----------|-----------|
| INT       | BOOLEAN   | STRING    |

| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |
```

示例 3：更新字段数据类型

```shell
| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |


| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | STRING    | STRING    |
```

示例 4：添加新字段

```shell
| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |


| A         | B         | C         | D         |
|-----------|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   | DOUBLE    |
```

### 数据 Transform

在数据类型转换之后，Transform 将接收来自上游（源或 transform）的数据行输入，编辑为具有新数据类型的数据行，并将其输出到下游（接收器或 transform）。这个过程称为数据转换。

### 翻译

Transform 与执行引擎解耦，任何 transform 实现都可以在所有引擎中运行，而无需更改代码或配置，这需要翻译层来适配 transform 和执行引擎。

示例：数据类型和数据的翻译

```shell
原始数据：

| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |

数据类型翻译：

| A                 | B                 | C                 |
|-------------------|-------------------|-------------------|
| ENGINE<STRING>    | ENGINE<INT>       | ENGINE<BOOLEAN>   |

数据翻译：

| A                 | B                 | C                 |
|-------------------|-------------------|-------------------|
| ENGINE<"test">    | ENGINE<1>         |  ENGINE<false>    |
```

## 核心 API

### TableTransformFactory

- 用于创建 transform 的工厂类，通过它可以使用 `createTransform` 方法创建 transform 实例。
- `factoryIdentifier` 用于标识当前工厂的名称，这在配置文件中也会进行配置，以区分不同的 transform。
- `optionRule` 用于定义当前 transform 支持的参数。此方法可以用来定义参数的逻辑，比如哪些参数是必需的，哪些是可选的，哪些是互斥的等等。SeaTunnel 会使用 `OptionRule` 来验证用户配置的有效性。请参考下面的 `Option`。
- 确保在 `TableTransformFactory` 上添加 `@AutoService(Factory.class)` 注解。

我们可以从上游接收目录表输入，并从 `TableTransformFactoryContext` 获取 transform 配置。

```java
    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () ->
                new SQLMultiCatalogFlatMapTransform(
                        context.getCatalogTables(), context.getOptions());
    }
```

### SeaTunnelTransform

`SeaTunnelTransform` 提供了所有主要和核心的 API，你可以通过继承它来实现 transform。

1. 获取该 transform 产生的目录表列表。

   ```java
   List<CatalogTable> getProducedCatalogTables();
   ```

   或者获取该 transform 产生的目录表。

   ```java
   CatalogTable getProducedCatalogTable();
   ```

2. 如果 transform 需要更改 schema，可以处理 `SchemaChangeEvent`。

   ```java
       default SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
      return schemaChangeEvent;
   }
   ```

3. 编辑输入数据并输出新的数据到下游，使用 `SeaTunnelMapTransform`。

   ```java
    T map(T row);
   ```

4. 或者编辑输入数据并输出新的数据到下游，使用 `SeaTunnelFlatMapTransform`。

   ```java
    List<T> flatMap(T row);
   ```

### SingleFieldOutputTransform

`SingleFieldOutputTransform` 抽象了单字段变换操作。

1. 定义输出字段列。

   ```java
   protected abstract Column getOutputColumn();
   ```

2. 定义输出字段的值。

   ```java
   protected abstract Object getOutputFieldValue(SeaTunnelRowAccessor inputRow);
   ```

### MultipleFieldOutputTransform

`MultipleFieldOutputTransform` 抽象了多字段变换操作。

1. 定义输出字段列。

   ```java
   protected abstract Column[] getOutputColumns();
   ```

2. 定义输出字段的值。

   ```java
   protected abstract Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow);
   ```

### AbstractSeaTunnelTransform

`AbstractSeaTunnelTransform` 抽象了数据类型、表路径和字段变换操作。

1. 转换输入行类型并输出新行类型。

   ```java
   protected abstract TableSchema transformTableSchema();
   ```

2. 转换输入行数据并输出新数据行。

   ```java
   protected abstract R transformRow(SeaTunnelRow inputRow);
   ```

3. 转换输入目录表路径并输出新目录表路径。

   ```java
   protected abstract TableIdentifier transformTableIdentifier();
   ```

### AbstractCatalogSupportFlatMapTransform & AbstractCatalogSupportMapTransform

包含了 transform 公共功能的基本实现，以及 transform 功能的高级封装。你可以通过实现这些类来快速开发 transform。

### AbstractMultiCatalogFlatMapTransform & AbstractMultiCatalogMapTransform

`AbstractCatalogSupportFlatMapTransform` 和 `AbstractCatalogSupportMapTransform` 的多表版本。包含了多表 transform 的封装。有关多表 transform 的更多信息，请参阅 [transform-multi-table.md](../docs/zh/transform-v2/transform-multi-table.md)

## 开发一个 Transform

你必须实现以下 API 中的一个：
- SeaTunnelMapTransform
- SeaTunnelFlatMapTransform
- AbstractSeaTunnelTransform
- AbstractCatalogSupportFlatMapTransform
- AbstractCatalogSupportMapTransform
- AbstractMultiCatalogFlatMapTransform
- AbstractMultiCatalogMapTransform
- SingleFieldOutputTransform
- MultipleFieldOutputTransform

将实现的子类添加到模块 `seatunnel-transforms-v2` 中。

在 SeaTunnel 根路径的 `plugin-mapping.properties` 文件中添加 transform 信息。

### 示例

请参考 [transform 的源代码](src/main/java/org/apache/seatunnel/transform)

## Transform 测试工具

一旦你添加了一个新的插件，建议为它添加 e2e 测试。
我们有一个 `seatunnel-e2e/seatunnel-transforms-v2-e2e` 模块来帮助你完成这项工作。

例如，如果你想为 `CopyFieldTransform` 添加 e2e 测试，可以在 `seatunnel-e2e/seatunnel-transforms-v2-e2e` 模块中创建一个新测试，并在测试中扩展 `TestSuiteBase` 类。

```java
public class TestCopyFieldTransformIT extends TestSuiteBase {

    @TestTemplate
    public void testCopyFieldTransform(TestContainer container) {
        Container.ExecResult execResult = container.executeJob("/copy_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
```

一旦你的测试用例实现了 `TestSuiteBase` 接口并使用 `@TestTemplate` 注解启动，它将针对所有引擎运行作业，你只需要执行 `executeJob` 方法并提供你的 SeaTunnel 配置文件，它将提交 SeaTunnel 作业。