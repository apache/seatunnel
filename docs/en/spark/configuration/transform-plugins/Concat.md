# Transform plugin : Concat [Spark]

## Description

Concat additional string according to `addition`

## Options

| name           | type   | required | default value       |
| -------------- | ------ | -------- | ------------------- |
| addition       | string | no       | `""` (Empty String) |
| source_field   | string | no       | `raw_message`       |
| target_field   | string | no       | `__root__`          |
| common-options | string | no       | -                   |

### addition [string]

The additional string will be concated to the source field. The default addition is a empty string `("")` .

### source_field [string]

The source field of the string before being concated, if not configured, the default is `raw_message`.

### target_field [string]

The target field of the string after being concated, if not configured, the default is `__root__`.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](./transform-plugin.md) for details

## Examples

```bash
concat {
  fields = ["msg"]
  addition = "+++++"
}
```

```bash
concat {
  fields = ["msg"]
  addition = "+++++"
  source_field = "raw_message"
  target_field = "msg_concat"
}
```

