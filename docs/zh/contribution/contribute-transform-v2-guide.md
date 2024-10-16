# 贡献 Transform 指南

本文描述了如何理解、开发和贡献一个 transform。

我们也提供了 [Transform E2E Test](../../../seatunnel-e2e/seatunnel-transforms-v2-e2e)
来验证 transform 的数据输入和输出。

## 概念

在 SeaTunnel 中你可以通过 connector 读写数据， 但如果你需要在读取数据后或者写入数据前处理数据， 你需要使用 transform。

使用 transform 可以简单修改数据行和字段， 例如拆分字段、修改字段的值或者删除字段。

### 类型转换

Transform 从上游（source 或者 transform）获取类型输入，然后给下游（sink 或者 transform）输出新的类型，这个过程就是类型转换。

案例 1：删除字段

```shell
| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |

| A         | B         |
|-----------|-----------|
| STRING    | INT       |
```

案例 2：字段排序

```shell
| B         | C         | A         |
|-----------|-----------|-----------|
| INT       | BOOLEAN   | STRING    |

| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |
```

案例 3：修改字段类型

```shell
| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |


| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | STRING    | STRING    |
```

案例 4：添加新的字段

```shell
| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |


| A         | B         | C         | D         |
|-----------|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   | DOUBLE    |
```

### 数据转换

转换类型后，Transform 会从上游（source 或者 transform）获取数据行， 使用[新的数据类型](#类型转换)编辑数据后输出到下游（sink 或者 transform）。这个过程叫数据转换。

### 翻译

Transform 已经从 execution engine 中解耦， 任何 transform 实现可以不需要修改和配置的适用所有引擎， 这就需要翻译层来做 transform 和 execution engine 的适配。

案例：翻译数据类型和数据

```shell
原始数据:

| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |

类型转换:

| A                 | B                 | C                 |
|-------------------|-------------------|-------------------|
| ENGINE<STRING>    | ENGINE<INT>       | ENGINE<BOOLEAN>   |

数据转换:

| A                 | B                 | C                 |
|-------------------|-------------------|-------------------|
| ENGINE<"test">    | ENGINE<1>         |  ENGINE<false>    |
```

## 核心 APIs

### SeaTunnelTransform

`SeaTunnelTransform` 提供了所有主要的 API, 你可以继承它实现任何转换。

1. 从上游获取数据类型。

```java
/**
 * Set the data type info of input data.
 *
 * @param inputDataType The data type info of upstream input.
 */
 void setTypeInfo(SeaTunnelDataType<T> inputDataType);
```

2. 输出新的数据类型给下游。

```java
/**
 * Get the data type of the records produced by this transform.
 *
 * @return Produced data type.
 */
SeaTunnelDataType<T> getProducedType();
```

3. 修改输入数据并且输出新的数据到下游。

```java
/**
 * Transform input data to {@link this#getProducedType()} types data.
 *
 * @param row the data need be transform.
 * @return transformed data.
 */
T map(T row);
```

### SingleFieldOutputTransform

`SingleFieldOutputTransform` 抽象了一个单字段修改操作

1. 定义输出字段

```java
/**
 * Outputs new field
 *
 * @return
 */
protected abstract String getOutputFieldName();
```

2. 定义输出字段类型

```java
/**
 * Outputs new field datatype
 *
 * @return
 */
protected abstract SeaTunnelDataType getOutputFieldDataType();
```

3. 定义输出字段值

```java
/**
 * Outputs new field value
 * 
 * @param inputRow The inputRow of upstream input.
 * @return
 */
protected abstract Object getOutputFieldValue(SeaTunnelRowAccessor inputRow);
```

### MultipleFieldOutputTransform

`MultipleFieldOutputTransform` 抽象了多字段修改操作

1. 定义多个输出的字段

```java
/**
 * Outputs new fields
 *
 * @return
 */
protected abstract String[] getOutputFieldNames();
```

2. 定义输出字段的类型

```java
/**
 * Outputs new fields datatype
 *
 * @return
 */
protected abstract SeaTunnelDataType[] getOutputFieldDataTypes();
```

3. 定义输出字段的值

```java
/**
 * Outputs new fields value
 *
 * @param inputRow The inputRow of upstream input.
 * @return
 */
protected abstract Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow);
```

### AbstractSeaTunnelTransform

`AbstractSeaTunnelTransform` 抽象了数据类型和字段的修改操作

1. 转换输入的行类型到新的行类型

```java
/**
 * Outputs transformed row type.
 *
 * @param inputRowType upstream input row type
 * @return
 */
protected abstract SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType);
```

2. 转换输入的行数据到新的行数据

```java
/**
 * Outputs transformed row data.
 * 
 * @param inputRow upstream input row data
 * @return
 */
protected abstract SeaTunnelRow transformRow(SeaTunnelRow inputRow);
```

## 开发一个 Transform

Transform 必须实现下面其中一个 API:
- SeaTunnelTransform
- AbstractSeaTunnelTransform
- SingleFieldOutputTransform
- MultipleFieldOutputTransform

将实现类放入模块 `seatunnel-transforms-v2`。

### 案例: 拷贝字段到一个新的字段

```java
@AutoService(SeaTunnelTransform.class)
public class CopyFieldTransform extends SingleFieldOutputTransform {

    private String srcField;
    private int srcFieldIndex;
    private SeaTunnelDataType srcFieldDataType;
    private String destField;

    @Override
    public String getPluginName() {
        return "Copy";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        this.srcField = pluginConfig.getString("src_field");
        this.destField = pluginConfig.getString("dest_fields");
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType inputRowType) {
        srcFieldIndex = inputRowType.indexOf(srcField);
        srcFieldDataType = inputRowType.getFieldType(srcFieldIndex);
    }

    @Override
    protected String getOutputFieldName() {
        return destField;
    }

    @Override
    protected SeaTunnelDataType getOutputFieldDataType() {
        return srcFieldDataType;
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        return inputRow.getField(srcFieldIndex);
    }
}
```

1. `getPluginName` 方法用来定义 transform 的名字。
2. @AutoService 注解用来自动生成 `META-INF/services/org.apache.seatunnel.api.transform.SeaTunnelTransform` 文件
3. `setConfig` 方法用来注入用户配置。

## Transform 测试工具

当你添加了一个新的插件， 推荐添加一个 e2e 测试用例来测试。
我们有 `seatunnel-e2e/seatunnel-transforms-v2-e2e` 来帮助你实现。

例如， 如果你想要添加一个 `CopyFieldTransform` 的测试用例， 你可以在 `seatunnel-e2e/seatunnel-transforms-v2-e2e`
模块中添加一个新的测试用例， 并且在用例中继承 `TestSuiteBase` 类。

```java
public class TestCopyFieldTransformIT extends TestSuiteBase {

    @TestTemplate
    public void testCopyFieldTransform(TestContainer container) {
        Container.ExecResult execResult = container.executeJob("/copy_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
```

一旦你的测试用例实现了 `TestSuiteBase` 接口， 并且添加 `@TestTemplate` 注解，它会在所有引擎运行作业，你只需要用你自己的 SeaTunnel 配置文件执行 executeJob 方法，
它会提交 SeaTunnel 作业。
