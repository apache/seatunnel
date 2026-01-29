# Encrypt

> 加密 Transform 插件

## 描述

Encrypt Transform 插件用于使用对称加密算法，对记录中指定的字段进行加密或解密。

## 参数说明

| 参数名         | 类型     | 是否必填 | 默认值       | 描述                         |
|-------------|--------|------|-----------|----------------------------|
| `fields`    | Array  | 是    | -         | 需要加密或解密的字段列表               |
| `algorithm` | String | 否    | `AES_CBC` | 加密算法                       |
| `key`       | String | 是    | -         | Base64 编码的加密密钥             |
| `mode`      | String | 否    | `ENCRYPT` | 操作模式：`ENCRYPT` 或 `DECRYPT` |

### algorithm [string]

本 Transform 使用的加密算法。
目前仅支持 `AES_CBC`。

### key [string]

加密密钥必须以 Base64 编码格式提供。
请确保密钥长度符合所选加密算法的要求。

对于 `AES_CBC`，支持的密钥长度为 16、24 或 32 字节
（分别对应 AES-128、AES-192 和 AES-256）。

**示例**

- `base64:AAAAAAAAAAAAAAAAAAAAAA==`
- `AAAAAAAAAAAAAAAAAAAAAA==`

### common options [string]

Transform 插件的通用参数，请参考 [Transform Plugin](common-options.md)。

## 示例

### 字段加密

```hocon
transform {
  FieldEncrypt {
    fields = ["name"]
    key = "base64:AAAAAAAAAAAAAAAAAAAAAA=="
    algorithm = "AES_CBC"
    mode = "encrypt"
  }
}
```

### 字段解密

```hocon
transform {
  FieldEncrypt {
    fields = ["name"]
    key = "base64:AAAAAAAAAAAAAAAAAAAAAA=="
    algorithm = "AES_CBC"
    mode = "decrypt"
  }
}
```
