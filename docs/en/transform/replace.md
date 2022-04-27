# Replace

## Description

Examines string value in a given field and replaces substring of the string value that matches the given string literal or regexes with the given replacement.

:::tip

This transform can supported by Spark and Flink,There are slight differences between the two engine configuration items.

:::

## Options(Spark)

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

### is_regex [string]

Whether or not to interpret the pattern as a regex (true) or string literal (false).

### replacement [boolean]

The replacement pattern (is_regex is true) or string literal (is_regex is false).

### replace_first [boolean]

Whether or not to skip any matches beyond the first match.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.mdx) for details

## Examples
the word `a` will be replaced by `b` at message field values.

```bash
Replace {
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
    replacement = "$2“
    isRegex = true
    replaceFirst = true
  }

  # Use the replace function (confirm that the fake table exists)
  sql {
    sql = "select * from (select raw_message, replace(raw_message) as info_row from fake) t1"
  }
```

## Options(Flink)

| name  | type   | required | default value |
| ----- | ------ | -------- |---------------|
| pattern      | string | no       | -             |
| replacement | string  | no      | -             |
| is_regex | boolean  | no      | false         |
| replace_first | boolean  | no      | false         |
| common-options | string | no       | -             |

### pattern [string]

the regular expression to which this string is to be matched or a common string literal, the default is ""

### replacement [string]

the string to be substituted for each match, the default is ""

### is_regex [boolean]

Whether or not to interpret the pattern as a regex (true) or string literal (false), the dafault is false

### replace_first [boolean]

when replace_first set true , The string constructed by replacing the first matching subsequence by the replacement
string,
Otherwise, replacing all matching subsequence by the replacement string

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.mdx) for details

## Examples

- Replace the string that the first regular expression matches  
  Input : Tom's phone number is 123456789 , he's age is 24  
  Output : Tom's phone number is * , he's age is 24

```bash
  # This just created a udf called replace
  Replace {
      pattern="\\d+"
      replacement="*"
      is_regex = "true"
      replace_first = "true"
   }
  # Use the replace function (confirm that the fake table exists)
  sql {
       sql = "select replace(text) as insensitive_text from fake"
  }
```

- Replace the string that all regular expression matches  
  Input : Tom's phone number is 123456789 , he's age is 24  
  Output : Tom's phone number is * , he's age is *

```bash
  # This just created a udf called replace
  Replace {
      pattern="\\d+"
      replacement="*"
      is_regex = "true"
      #replace_first = "false" 
   }
  # Use the replace function (confirm that the fake table exists)
  sql {
       sql = "select replace(text) as insensitive_text from fake"
  }
```

- Replaces the string contained "'s" in the string  
  Input : Tom's phone number is 123456789 , he's age is 24  
  Output : Tom is phone number is 123456789 , he is age is 24

```bash
  # This just created a udf called replace
  Replace {
      pattern="'s"
      replacement=" is"
      #is_regex = "false"
      #replace_first = "false"
   }
  # Use the replace function (confirm that the fake table exists)
  sql {
       sql = "select replace(text) as replaced_text from fake"
  }
```


