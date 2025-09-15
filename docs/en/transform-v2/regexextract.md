# RegexExtract

> RegexExtract transform plugin

## Description

The `RegexExtract` transform plugin uses regular expressions to extract data from a specified field and outputs the extracted values to new fields. It supports capture groups in regex patterns and allows setting default values for each output field when the pattern doesn't match.

## Options

| name           | type    | required | default value |
|----------------|---------|----------|---------------|
| source_field   | string  | yes      |               |
| regex_pattern  | string  | yes      |               |
| output_fields  | array   | yes      |               |
| default_values | array   | no       |               |

### source_field [string]

The source field name to extract data from.

### regex_pattern [string]

The regular expression pattern with capture groups. The number of capture groups must match the number of output fields.

### output_fields [array]

The names of the output fields for extracted values. The size must match the number of capture groups in the regex pattern.

### default_values [array]

Default values for output fields when the regex pattern does not match or the source field is null. If provided, the size must match the number of output fields.


## Example

The data read from source is a table like this:

| id | email              | log_entry                                            |
|----|--------------------|------------------------------------------------------|
| 1  | user1@example.com  | 2023-12-01 10:30:45 INFO User login successful       |
| 2  | admin@test.org     | 2023-12-01 11:15:22 ERROR Database connection failed |
| 3  | guest@domain.net   | 2023-12-01 12:00:00 WARN Memory usage high           |

We want to extract username, domain, and top-level domain from the `email` field:

```
transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "regex_result"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = ["username", "domain", "tld"]
    default_values = ["unknown", "unknown", "unknown"]
  }
}
```

Then the data in result table `regex_result` will be:

| id | email              | log_entry                                            | username | domain  | tld |
|----|--------------------|------------------------------------------------------|----------|---------|-----|
| 1  | user1@example.com  | 2023-12-01 10:30:45 INFO User login successful       | user1    | example | com |
| 2  | admin@test.org     | 2023-12-01 11:15:22 ERROR Database connection failed | admin    | test    | org |
| 3  | guest@domain.net   | 2023-12-01 12:00:00 WARN Memory usage high           | guest    | domain  | net |

## Job Config Example

```
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        email = "string"
        log_entry = "string"
      }
    }
    rows = [
      {
          kind = INSERT,
          fields = [1, "user1@example.com", "2023-12-01 10:30:45 INFO User login successful"]
      },
      {
        kind = INSERT,
        fields = [2, "admin@test.org", "2023-12-01 11:15:22 ERROR Database connection failed"]
      },
      {
        kind = INSERT,
        fields = [3, "guest@domain.net", "2023-12-01 12:00:00 WARN Memory usage high"]
      }
    ]
  }
}

transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "regex_result"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = ["username", "domain", "tld"]
    default_values = ["unknown", "unknown", "unknown"]
  }
}

sink {
  Console {
    plugin_input = "regex_result"
  }
}
```

## Changelog

