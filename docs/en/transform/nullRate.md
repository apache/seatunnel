# NullRate

> NULL rate transform plugin

## Description

When there is a large amount of data, the final result will always be greatly affected by the problem of data null value. Therefore, early null value detection is particularly important. For this reason, this function came into being

:::tip

This transform **ONLY** supported by Spark.

:::

## Options

| name                     | type         | required | default value |
| -------------------------| ------------ | -------- | ------------- |
| fields                   | string_list  | yes      | -             |
| rates                    | double_list  | yes      | -             |
| throw_exception_enable   | boolean      | no       | -             |
| save_to_table_name       | string       | no       | -             |



### field [string_list]

Which fields do you want to monitor .

### rates [double_list]

It is consistent with the number of fields. Double type indicates the set null rate value .

### throw_exception_enable [boolean]

Whether to throw an exception when it is greater than the set value. The default value is false .

### save_to_table_name [string]

Whether the current verification value is output to the table. It is not output by defaul .

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.mdx) for details

## Examples

```bash
  nullRate {
     fields = ["msg", "name"]
     rates = [10.0,3.45]
     save_to_table_name = "tmp"
     throw_exception_enable = true
  }
}
```

Use `NullRate` in transform's Dataset.

```bash
  transform {
    NullRate {
      fields = ["msg", "name"]
      rates = [10.0,3.45]
      save_to_table_name = "tmp"
      throw_exception_enable = true
    }
  }
```
