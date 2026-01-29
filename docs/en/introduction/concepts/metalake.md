# METALAKE

Since Seatunnel requires database usernames, passwords, and other sensitive information to be written in plaintext within scripts when executing tasks, this may lead to information leakage and is also difficult to maintain. When data source information changes, manual modifications are often required.

To address this, Metalake is introduced. Data source information can be stored in Metalake systems such as Apache Gravitino. Task scripts then use `sourceId` and placeholders instead of actual usernames and passwords. At runtime, the Seatunnel engine retrieves the information from Metalake via HTTP requests and replaces the placeholders accordingly.

To enable Metalake, you first need to modify the environment variables in **seatunnel-env.sh**:

* `METALAKE_ENABLED`
* `METALAKE_TYPE`
* `METALAKE_URL`

Set `METALAKE_ENABLED` to `true`. Currently, `METALAKE_TYPE` only supports `gravitino`.

For Apache Gravitino, set `METALAKE_URL` to:

```
http://host:port/api/metalakes/your_metalake_name/catalogs/
```

---

## Usage Example

First, create a catalog in Gravitino, for example:

```bash
curl -L 'http://localhost:8090/api/metalakes/test_metalake/catalogs' \
-H 'Content-Type: application/json' \
-H 'Accept: application/vnd.gravitino.v1+json' \
-d '{
    "name": "test_catalog",
    "type": "relational",
    "provider": "jdbc-mysql",
    "comment": "for metalake test",
    "properties": {
        "jdbc-driver": "com.mysql.cj.jdbc.Driver",
        "jdbc-url": "not used",
        "jdbc-user": "root",
        "jdbc-password": "Abc!@#135_seatunnel"
    }
}'
```

This creates a `test_catalog` under `test_metalake` (note: `metalake` itself must be created in advance).

Thus, `METALAKE_URL` can be set to:

```
http://localhost:8090/api/metalakes/test_metalake/catalogs/
```

You can then define the source as:

```hocon
source {
    Jdbc {
        url = "jdbc:mysql://mysql-e2e:3306/seatunnel?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
        driver = "${jdbc-driver}"
        connection_check_timeout_sec = 100
        sourceId = "test_catalog"
        user = "${jdbc-user}"
        password = "${jdbc-password}"
        query = "select * from source"
    }
}
```

Here, `sourceId` refers to the catalog name, allowing other fields to use `${}` placeholders. At runtime, they will be automatically replaced. Note that in sinks, the same `sourceId` name is used, and placeholders must always start with `${` and end with `}`. Each item can contain at most one placeholder, and there can be content outside the placeholder as well.