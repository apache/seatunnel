# EncryptedPhone

## Description

Desensitize the user's mobile phone number to enhance data integrity.

:::tip

This transform **ONLY** supported by Spark.

:::

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| fields         | string | no       | -             |


### field [string]

The field which you setting or all fields will be encrypted .

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.mdx) for details

## Examples

```bash
  EncryptedPhone {
     fields = ["msg", "name"]
  }
}
```

Use `EncryptedPhone` in transform's Dataset.

```bash
  transform {
    EncryptedPhone {
      fields = ["msg", "name"]
    }
  }
```
