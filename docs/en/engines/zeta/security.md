# Security

## Basic Authentication

You can secure your Web UI by enabling basic authentication. This will require users to enter a username and password when accessing the web interface.

| Parameter Name | Required | Description |
|----------------|----------|-------------|
| `enable-basic-auth` | No | Whether to enable basic authentication, default is `false` |
| `basic-auth-username` | No | The username for basic authentication, default is `admin` |
| `basic-auth-password` | No | The password for basic authentication, default is `admin` |

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

## HTTPS Configuration

You can secure your REST-API-V2 service by enabling HTTPS. Both HTTP and HTTPS can be enabled simultaneously, or only one of them can be enabled.

| Parameter Name | Required | Description |
|----------------|----------|-------------|
| `enable-http` | No | Whether to enable HTTP service, default is `true` |
| `port` | No | HTTP service port, default is `8080` |
| `enable-https` | No | Whether to enable HTTPS service, default is `false` |
| `https-port` | No | HTTPS service port, default is `8443` |
| `key-store-path` | Required when `enable-https` is `true` | Path to the KeyStore file, used to store the server's private key and certificate |
| `key-store-password` | Required when `enable-https` is `true` | KeyStore password |
| `key-manager-password` | Required when `enable-https` is `true` | KeyManager password, usually the same as the KeyStore password |
| `trust-store-path` | No | Path to the TrustStore file, used to verify client certificates |
| `trust-store-password` | No | TrustStore password |

**Note**: When `trust-store-path` and `trust-store-password` are not empty, mutual SSL authentication (client authentication) will be enabled, requiring the client to provide a valid certificate.

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
      # Optional: Mutual authentication
      trust-store-path: "${YOUR_TRUST_STORE_PATH}"
      trust-store-password: "${YOUR_TRUST_STORE_PASSWORD}"
```

### Example of Generating Keys

```shell
#!/bin/bash

# Define the project root directory
PROJECT_DIR="/Users/mac/IdeaProjects/data"

# Define passwords
SERVER_KEYSTORE_PASSWORD="server_keystore_password"
SERVER_KEY_PASSWORD="server_keystore_password"
CLIENT_KEYSTORE_PASSWORD="client_keystore_password"
CLIENT_KEY_PASSWORD="client_keystore_password"
SERVER_TRUSTSTORE_PASSWORD="server_truststore_password"
CLIENT_TRUSTSTORE_PASSWORD="client_truststore_password"

# Generate server keystore
keytool -genkeypair \
  -alias server \
  -keyalg RSA \
  -keysize 2048 \
  -validity 365 \
  -keystore "$PROJECT_DIR/server_keystore.jks" \
  -storepass "$SERVER_KEYSTORE_PASSWORD" \
  -keypass "$SERVER_KEY_PASSWORD" \
  -dname "CN=localhost,OU=IT,O=MyCompany,L=Shanghai,ST=Shanghai,C=CN"

# Export server certificate
keytool -exportcert \
  -alias server \
  -keystore "$PROJECT_DIR/server_keystore.jks" \
  -storepass "$SERVER_KEYSTORE_PASSWORD" \
  -file "$PROJECT_DIR/server.crt"

# Generate client keystore
keytool -genkeypair \
  -alias client \
  -keyalg RSA \
  -keysize 2048 \
  -validity 365 \
  -keystore "$PROJECT_DIR/client_keystore.jks" \
  -storepass "$CLIENT_KEYSTORE_PASSWORD" \
  -keypass "$CLIENT_KEY_PASSWORD" \
  -dname "CN=client,OU=IT,O=MyCompany,L=Shanghai,ST=Shanghai,C=CN"

# Export client certificate
keytool -exportcert \
  -alias client \
  -keystore "$PROJECT_DIR/client_keystore.jks" \
  -storepass "$CLIENT_KEYSTORE_PASSWORD" \
  -file "$PROJECT_DIR/client.crt"

# Create server truststore and import client certificate
keytool -importcert \
  -alias client \
  -file "$PROJECT_DIR/client.crt" \
  -keystore "$PROJECT_DIR/server_truststore.jks" \
  -storepass "$SERVER_TRUSTSTORE_PASSWORD" \
  -noprompt

# Create client truststore and import server certificate
keytool -importcert \
  -alias server \
  -file "$PROJECT_DIR/server.crt" \
  -keystore "$PROJECT_DIR/client_truststore.jks" \
  -storepass "$CLIENT_TRUSTSTORE_PASSWORD" \
  -noprompt
```