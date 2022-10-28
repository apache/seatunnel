# Replace

> Replace transform plugin

## Description

Examines string value in a given field and replaces substring of the string value that matches the given string literal or regexes with the given replacement.

:::tip

This transform **ONLY** supported by Spark.

:::

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| source_field   | string | no       | raw_message   |
| fields         | string | yes      | -             |
| pattern        | string | yes      | -             |
| replacement    | string | yes      | -             |
| is_regex       | boolean| no       | false         |
| replace_first  | boolean| no       | false         |

### source_field [string]

Source field, if not configured, the default is `raw_message`

### field [string]

The name of the field to replaced.

### pattern [string]

The string to match.

### replacement [string]

The replacement pattern (is_regex is true) or string literal (is_regex is false).

### is_regex [boolean]

Whether or not to interpret the pattern as a regex (true) or string literal (false).

### replace_first [boolean]

Whether or not to skip any matches beyond the first match.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.mdx) for details

## Examples
the word `a` will be replaced by `b` at message field values.

```bash
replace {
    source_field = "message"
    fields = "_replaced"
    pattern = "a"
    replacement = "b"
}
```

Use `Replace` as udf in sql.

```bash
  Replace {
    fields = "_replaced"
    pattern = "([^ ]*) ([^ ]*)"
    replacement = "$2"
    isRegex = true
    replaceFirst = true
  }

  # Use the split function (confirm that the fake table exists)
  sql {
    sql = "select * from (select raw_message, replace(raw_message) as info_row from fake) t1"
  }
```
