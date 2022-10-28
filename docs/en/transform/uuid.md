# UUID

> UUID transform plugin

## Description

Generate a universally unique identifier on a specified field.

:::tip

This transform **ONLY** supported by Spark.

:::

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| fields         | string | yes      | -             |
| prefix         | string | no       | -             |
| secure         | boolean| no       | false         |

### field [string]

The name of the field to generate.

### prefix [string]

The prefix string constant to prepend to each generated UUID.

### secure [boolean]

the cryptographically secure algorithm can be comparatively slow
The nonSecure algorithm uses a secure random seed but is otherwise deterministic

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.mdx) for details

## Examples

```bash
  UUID {
    fields = "u"
    prefix = "uuid-"
    secure = true
  }
}
```

Use `UUID` as udf in sql.

```bash
  UUID {
    fields = "u"
    prefix = "uuid-"
    secure = true
  }

  # Use the uuid function (confirm that the fake table exists)
  sql {
    sql = "select * from (select raw_message, UUID() as info_row from fake) t1"
  }
```
