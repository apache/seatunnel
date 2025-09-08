# Filter

> Filter transform plugin

## Description

Filter the field.

## Options

|      name      | type  | required | default value |
|----------------|-------|----------|---------------|
| include_fields | array | no       |               |
| exclude_fields | array | no       |               |

Notice, you must set one and only one of `include_fields` and `exclude_fields` properties

### include_fields [array]

The list of fields that need to be kept. Fields not in the list will be deleted.

### exclude_fields [array]

The list of fields that need to be deleted. Fields not in the list will be kept.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Example

The data read from source is a table like this:

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

we want to keep the field named `name`, `card`, we can add a `Filter` Transform like below:

```
transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    include_fields = [name, card]
  }
}
```

Or we can delete the field named `age` by adding a `Filter` Transform with `exclude_fields` field set like below:

```
transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    exclude_fields = [age]
  }
}
```

It is useful when you want to delete a small number of fields from a large table with tons of fields.

Then the data in result table `fake1` will like this

|   name   | card |
|----------|------|
| Joy Ding | 123  |
| May Ding | 123  |
| Kin Dom  | 123  |
| Joy Dom  | 123  |

## Changelog

### new version

- Add Filter Transform Connector

