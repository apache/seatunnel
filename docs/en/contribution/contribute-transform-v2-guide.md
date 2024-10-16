# Contribute Transform Guide

This document describes how to understand, develop and contribute a transform.

We also provide the [Transform E2E Test](../../../seatunnel-e2e/seatunnel-transforms-v2-e2e)
to verify the data input and output by the transform.

## Concepts

Using SeaTunnel you can read or write data through the connector, but if you need to
process your data after reading or before writing, then need to use transform.

Use transform to make simple edits to your data rows or fields, such as split field,
change field values, add or remove field.

### DataType Transform

Transform receives datatype input from upstream(source or transform) and outputs new datatype to
downstream(sink or transform), this process is datatype transform.

Example 1：Remove fields

```shell
| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |

| A         | B         |
|-----------|-----------|
| STRING    | INT       |
```

Example 2：Sort fields

```shell
| B         | C         | A         |
|-----------|-----------|-----------|
| INT       | BOOLEAN   | STRING    |

| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |
```

Example 3：Update fields datatype

```shell
| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |


| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | STRING    | STRING    |
```

Example 4：Add new fields

```shell
| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |


| A         | B         | C         | D         |
|-----------|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   | DOUBLE    |
```

### Data Transform

After datatype transformed, Transform will receive data-row input from upstream(source or transform),
edit into data-row with [New Datatype](#DataType transform) and output to downstream (sink or transform).
This process is called data transform.

### Translation

Transform is decoupled from the execution engine, any transform implement can run into all engines
without changing the code & config, which requires the translation layer to adapt transform and execution engine.

Example：Translation datatype & data

```shell
Original:

| A         | B         | C         |
|-----------|-----------|-----------|
| STRING    | INT       | BOOLEAN   |

Datatype translation:

| A                 | B                 | C                 |
|-------------------|-------------------|-------------------|
| ENGINE<STRING>    | ENGINE<INT>       | ENGINE<BOOLEAN>   |

Data translation:

| A                 | B                 | C                 |
|-------------------|-------------------|-------------------|
| ENGINE<"test">    | ENGINE<1>         |  ENGINE<false>    |
```

## Core APIs

### SeaTunnelTransform

`SeaTunnelTransform` provides all major and primary APIs, you can subclass it to do whatever transform.

1. Receive datatype input from upstream.

```java
/**
 * Set the data type info of input data.
 *
 * @param inputDataType The data type info of upstream input.
 */
 void setTypeInfo(SeaTunnelDataType<T> inputDataType);
```

2. Outputs new datatype to downstream.

```java
/**
 * Get the data type of the records produced by this transform.
 *
 * @return Produced data type.
 */
SeaTunnelDataType<T> getProducedType();
```

3. Edit input data and outputs new data to downstream.

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

`SingleFieldOutputTransform` abstract single field change operator

1. Define output field

```java
/**
 * Outputs new field
 *
 * @return
 */
protected abstract String getOutputFieldName();
```

2. Define output field datatype

```java
/**
 * Outputs new field datatype
 *
 * @return
 */
protected abstract SeaTunnelDataType getOutputFieldDataType();
```

3. Define output field value

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

`MultipleFieldOutputTransform` abstract multiple fields change operator

1. Define output fields

```java
/**
 * Outputs new fields
 *
 * @return
 */
protected abstract String[] getOutputFieldNames();
```

2. Define output fields datatype

```java
/**
 * Outputs new fields datatype
 *
 * @return
 */
protected abstract SeaTunnelDataType[] getOutputFieldDataTypes();
```

3. Define output field values

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

`AbstractSeaTunnelTransform` abstract datatype & fields change operator

1. Transform input row type and outputs new row type

```java
/**
 * Outputs transformed row type.
 *
 * @param inputRowType upstream input row type
 * @return
 */
protected abstract SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType);
```

2. Transform input row data and outputs new row data

```java
/**
 * Outputs transformed row data.
 * 
 * @param inputRow upstream input row data
 * @return
 */
protected abstract SeaTunnelRow transformRow(SeaTunnelRow inputRow);
```

## Develop A Transform

It must implement one of the following APIs:
- SeaTunnelTransform
- AbstractSeaTunnelTransform
- SingleFieldOutputTransform
- MultipleFieldOutputTransform

Add implement subclass into module `seatunnel-transforms-v2`.

### Example: copy field to new field

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

1. The `getPluginName` method is used to identify the transform name.
2. The @AutoService is used to generate the `META-INF/services/org.apache.seatunnel.api.transform.SeaTunnelTransform`
   file automatically.
3. The `setConfig` method is used to inject user configs.

## Transform Test Tool

Once you add a new plugin, it is recommended to add e2e tests for it.
We have a `seatunnel-e2e/seatunnel-transforms-v2-e2e` module to help you to do this.

For example, if you want to add an e2e test for `CopyFieldTransform`, you can create a new test in
`seatunnel-e2e/seatunnel-transforms-v2-e2e` module and extend the `TestSuiteBase` class in the test.

```java
public class TestCopyFieldTransformIT extends TestSuiteBase {

    @TestTemplate
    public void testCopyFieldTransform(TestContainer container) {
        Container.ExecResult execResult = container.executeJob("/copy_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
```

Once your testcase implements the `TestSuiteBase` interface and use `@TestTemplate` annotation startup,
it will run job to all engines, and you just need to execute the executeJob method with your SeaTunnel configuration file,
it will submit the SeaTunnel job.
