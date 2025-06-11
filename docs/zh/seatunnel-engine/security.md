---
sidebar_position: 16
---

# Security

## Basic 认证

您可以通过开启 Basic 认证来保护您的 Web UI。这将要求用户在访问 Web 界面时输入用户名和密码。

| 参数名称 | 是否必填 | 参数描述 |
|--------|---------|--------|
| `enable-basic-auth` | 否 | 是否开启Basic 认证，默认为 `false` |
| `basic-auth-username` | 否 | Basic 认证的用户名，默认为 `admin` |
| `basic-auth-password` | 否 | Basic 认证的密码，默认为 `admin` |

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      enable-basic-auth: true
      basic-auth-username: "your_username"
      basic-auth-password: "your_password"
```

## HTTPS 配置

您可以通过开启 HTTPS 来保护您的 API 服务。HTTP 和 HTTPS 可同时开启，也可以只开启其中一个。

| 参数名称 | 是否必填 | 参数描述 |
|--------|---------|--------|
| `enable-http` | 否 | 是否开启 HTTP 服务，默认为 `true` |
| `port` | 否 | HTTP 服务端口，默认为 `8080` |
| `enable-https` | 否 | 是否开启 HTTPS 服务，默认为 `false` |
| `https-port` | 否 | HTTPS 服务端口，默认为 `8443` |
| `key-store-path` | 当 `enable-https` 为 `true` 时必填 | KeyStore 文件路径，用于存储服务器私钥和证书 |
| `key-store-password` | 当 `enable-https` 为 `true` 时必填 | KeyStore 密码 |
| `key-manager-password` | 当 `enable-https` 为 `true` 时必填 | KeyManager 密码，通常与 KeyStore 密码相同 |
| `trust-store-path` | 否 | TrustStore 文件路径，用于验证客户端证书 |
| `trust-store-password` | 否 | TrustStore 密码 |

**注意**：当 `trust-store-path` 和 `trust-store-password` 配置项不为空时，将启用双向 SSL 认证（客户端认证），要求客户端提供有效证书。

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      enable-https: true
      https-port: 8443
      key-store-path: "${YOUR_KEY_STORE_PATH}"
      key-store-password: "${YOUR_KEY_STORE_PASSWORD}"
      key-manager-password: "${YOUR_KEY_MANAGER_PASSWORD}"
      # 可选：双向认证
      trust-store-path: "${YOUR_TRUST_STORE_PATH}"
      trust-store-password: "${YOUR_TRUST_STORE_PASSWORD}"
```

### 生成密钥样例

```shell
#!/bin/bash

# 定义项目根目录
PROJECT_DIR="/Users/mac/IdeaProjects/data"

# 定义密码
SERVER_KEYSTORE_PASSWORD="server_keystore_password"
SERVER_KEY_PASSWORD="server_keystore_password"
CLIENT_KEYSTORE_PASSWORD="client_keystore_password"
CLIENT_KEY_PASSWORD="client_keystore_password"
SERVER_TRUSTSTORE_PASSWORD="server_truststore_password"
CLIENT_TRUSTSTORE_PASSWORD="client_truststore_password"

# 生成服务端密钥库
keytool -genkeypair \
  -alias server \
  -keyalg RSA \
  -keysize 2048 \
  -validity 365 \
  -keystore "$PROJECT_DIR/server_keystore.jks" \
  -storepass "$SERVER_KEYSTORE_PASSWORD" \
  -keypass "$SERVER_KEY_PASSWORD" \
  -dname "CN=localhost,OU=IT,O=MyCompany,L=Shanghai,ST=Shanghai,C=CN"

# 导出服务端证书
keytool -exportcert \
  -alias server \
  -keystore "$PROJECT_DIR/server_keystore.jks" \
  -storepass "$SERVER_KEYSTORE_PASSWORD" \
  -file "$PROJECT_DIR/server.crt"

# 生成客户端密钥库
keytool -genkeypair \
  -alias client \
  -keyalg RSA \
  -keysize 2048 \
  -validity 365 \
  -keystore "$PROJECT_DIR/client_keystore.jks" \
  -storepass "$CLIENT_KEYSTORE_PASSWORD" \
  -keypass "$CLIENT_KEY_PASSWORD" \
  -dname "CN=client,OU=IT,O=MyCompany,L=Shanghai,ST=Shanghai,C=CN"

# 导出客户端证书
keytool -exportcert \
  -alias client \
  -keystore "$PROJECT_DIR/client_keystore.jks" \
  -storepass "$CLIENT_KEYSTORE_PASSWORD" \
  -file "$PROJECT_DIR/client.crt"

# 创建服务端信任库并导入客户端证书
keytool -importcert \
  -alias client \
  -file "$PROJECT_DIR/client.crt" \
  -keystore "$PROJECT_DIR/server_truststore.jks" \
  -storepass "$SERVER_TRUSTSTORE_PASSWORD" \
  -noprompt

# 创建客户端信任库并导入服务端证书
keytool -importcert \
  -alias server \
  -file "$PROJECT_DIR/server.crt" \
  -keystore "$PROJECT_DIR/client_truststore.jks" \
  -storepass "$CLIENT_TRUSTSTORE_PASSWORD" \
  -noprompt
```