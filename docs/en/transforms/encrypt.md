# Encrypt

> Encrypt transform plugin

## Description

The Encrypt transform plugin is used to encrypt or decrypt specified fields in records using a symmetric encryption algorithm.

## Options

| name        | type   | required | default value | description                       |
|-------------|--------|----------|---------------|-----------------------------------|
| `fields`    | Array  | Yes      | -             | List of fields to encrypt/decrypt |
| `algorithm` | String | No       | `AES_CBC`     | Encryption algorithm              |
| `key`       | String | Yes      | -             | Base64-encoded encryption key     |
| `mode`      | String | No       | `ENCRYPT`     | `ENCRYPT`or `DECRYPT`             |

### algorithm [string]

Encryption algorithm used by this transform.
Currently, only `AES_CBC` is supported.

### key [string]

The encryption key must be provided in Base64-encoded format.
Make sure the key length matches the requirements of the selected algorithm.
For `AES_CBC`, valid key lengths are 16, 24, or 32 bytes (corresponding to AES-128, AES-192, or AES-256).

**Example**
- `base64:AAAAAAAAAAAAAAAAAAAAAA==`
- `AAAAAAAAAAAAAAAAAAAAAA==`

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Example

```
transform {
  FieldEncrypt {
	fields = ["name"]
    key = "base64:AAAAAAAAAAAAAAAAAAAAAA=="
    algorithm = "AES_CBC"
    mode = "ENCRYPT"
  }
}
```

```
transform {
  FieldEncrypt {
	fields = ["name"]
    key = "base64:AAAAAAAAAAAAAAAAAAAAAA=="
    algorithm = "AES_CBC"
    mode = "DECRYPT"
  }
}
```
