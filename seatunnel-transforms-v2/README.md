# Contribute Transform Guide

This document describes how to understand, develop and contribute a transform.

We also provide the [Transform E2E Test](../seatunnel-e2e/seatunnel-transforms-v2-e2e)
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
edit into data-row with new datatype and output to downstream (sink or transform).
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

### TableTransformFactory

- Used to create a factory class for transform, through which transform instances are created using the `createTransform` method.
- `factoryIdentifier` is used to identify the name of the current Factory, which is also configured in the configuration file to distinguish different transform.
- `optionRule` is used to define the parameters supported by the current transform. This method can be used to define the logic of the parameters, such as which parameters are required, which are optional, which are mutually exclusive, etc.
  SeaTunnel will use `OptionRule` to verify the validity of the user's configuration. Please refer to the `Option` below.
- Make sure to add the `@AutoService(Factory.class)` annotation to `TableTransformFactory`.

We can receive catalog table input from upstream and the transform configuration from `TableTransformFactoryContext`.

```java
    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () ->
                new SQLMultiCatalogFlatMapTransform(
                        context.getCatalogTables(), context.getOptions());
    }
```

### SeaTunnelTransform

`SeaTunnelTransform` provides all major and primary APIs, you can subclass it to do whatever transform.

1. Get the produced catalog table list of this transform.

   ```java
   List<CatalogTable> getProducedCatalogTables();
   ```
   
   or get the produced catalog table of this transform.
   
   ```java
   CatalogTable getProducedCatalogTable();
   ```

2. Handle the SchemaChangeEvent if the transform needs to change the schema.

   ```java
       default SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
      return schemaChangeEvent;
   }
   ```

3. Edit input data and outputs new data to downstream with `SeaTunnelMapTransform`.

   ```java
    T map(T row);
   ```
   
4. Or edit input data and outputs new data to downstream with `SeaTunnelFlatMapTransform`.

   ```java
    List<T> flatMap(T row);
   ```

### SingleFieldOutputTransform

`SingleFieldOutputTransform` abstract single field change operator

1. Define output field column
   
   ```java
   protected abstract Column getOutputColumn();
   ```

2. Define output field value
   
   ```java
   protected abstract Object getOutputFieldValue(SeaTunnelRowAccessor inputRow);
   ```

### MultipleFieldOutputTransform

`MultipleFieldOutputTransform` abstract multiple fields change operator

1. Define output fields column

   ```java
   protected abstract Column[] getOutputColumns();
   ```

2. Define output field values

   ```java
   protected abstract Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow);
   ```

### AbstractSeaTunnelTransform

`AbstractSeaTunnelTransform` abstract datatype, table path and fields change operator

1. Transform input row type and outputs new row type
   
   ```java
   protected abstract TableSchema transformTableSchema();
   ```

2. Transform input row data and outputs new row data

   ```java
   protected abstract R transformRow(SeaTunnelRow inputRow);
   ```

3. Transform input catalog table path and outputs new catalog table path

   ```java
   protected abstract TableIdentifier transformTableIdentifier();
   ```
   
### AbstractCatalogSupportFlatMapTransform & AbstractCatalogSupportMapTransform

Contains the basic implementation of transform common functions and the advanced encapsulation of transform functions. 
You can quickly implement transform development by implementing this class.

### AbstractMultiCatalogFlatMapTransform & AbstractMultiCatalogMapTransform

The multi-table version of AbstractCatalogSupportFlatMapTransform & AbstractCatalogSupportMapTransform.
Contains the encapsulation of multi-table transform. For more information about multi-table transform, please refer to [transform-multi-table.md](../docs/en/transform-v2/transform-multi-table.md)

## Develop A Transform

It must implement one of the following APIs:
- SeaTunnelMapTransform
- SeaTunnelFlatMapTransform
- AbstractSeaTunnelTransform
- AbstractCatalogSupportFlatMapTransform
- AbstractCatalogSupportMapTransform
- AbstractMultiCatalogFlatMapTransform
- AbstractMultiCatalogMapTransform
- SingleFieldOutputTransform
- MultipleFieldOutputTransform

Add implement subclass into module `seatunnel-transforms-v2`.

Add transform info to `plugin-mapping.properties` file in seatunnel root path.

### Example

Please refer the [source code of transform](src/main/java/org/apache/seatunnel/transform)

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
