# Split

> Multi field split transform plugin

## Description

Split a field into multiple fields according to the specified delimiter, and support splitting of multiple fields at the same time.

## Options

|   name   |      type       | required | default value |
|----------|-----------------|----------|---------------|
| splitOPs | List\<SplitOP\> | yes      |               |

### splitOPs [List\<SplitOP\>]

A list of field splitting operations, each element represents the splitting of a field, and the definition of the operation is as follows:

```
{
    separator : delimiter used for splitting fields
    source_field : field name of the field to be split,
    output_fields : field names after splitting,
}
```

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Example

The data read from source is a table like this:

|   name   | age | card  |
|----------|-----|-------|
| Joy Ding | 20  | 123_1 |
| May Ding | 20  | 123_2 |
| Kin Dom  | 20  | 123_3 |
| Joy Dom  | 20  | 123_4 |

We will split the `name` field into `first_name` and `last_name`, and at the same time, split the `card` field into `card_head` and `card_tail`, we can add `Split` transform like this

```
transform {
  MultiFieldSplit {
    source_table_name = "fake"
    result_table_name = "fake1"
    splitOPs = [
    {    separator = " "
         split_field = "name"
         output_fields = ["first_name", "last_name"]
    },
    {    separator = "_"
         split_field = "card"
         output_fields = ["card_head", "card_tail"]
    }
    ]
  }
}
```

Then the data in result table `fake1` will like this

|   name   | age | card | first_name | last_name | card_head | card_tail |
|----------|-----|------|------------|-----------|-----------|-----------|
| Joy Ding | 20  | 123  | Joy        | Ding      | 123       | 1         |
| May Ding | 20  | 123  | May        | Ding      | 123       | 2         |
| Kin Dom  | 20  | 123  | Kin        | Dom       | 123       | 3         |
| Joy Dom  | 20  | 123  | Joy        | Dom       | 123       | 4         |

## Changelog

### new version

- Add Multi field Split Transform Connector

