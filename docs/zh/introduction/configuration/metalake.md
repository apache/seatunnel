# METALAKE

由于Seatunnel在执行任务时，需要将数据库用户名与密码等隐私信息明文写在脚本中，可能会导致信息泄露；并且维护较为困难，数据源信息发生变更时可能需要手动更改。

因此引入了metalake，将数据源的信息存储于Apache Gravitino等metalake中，任务脚本采用`sourceId`和占位符的方法来代替原本的用户名和密码等信息，运行时seatunnel-engine通过http请求从metalake获取信息，根据占位符进行替换。

若要使用metalake，首先要修改**seatunnel-env.sh**中的环境变量：

* `METALAKE_ENABLED`
* `METALAKE_TYPE`
* `METALAKE_URL`

将`METALAKE_ENABLED`设为`true`，`METALAKE_TYPE`当前仅支持设为`gravitino`。

对于Apache Gravitino，`METALAKE_URL`设为

```
http://host:port/api/metalakes/your_metalake_name/catalogs/
```

---

## 使用示例：

用户需要先在Gravitino中创建catalog，如

```bash
curl -L 'http://localhost:8090/api/metalakes/test_metalake/catalogs'
-H 'Content-Type: application/json'
-H 'Accept: application/vnd.gravitino.v1+json'
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

这样便在`test_metalake`中创建了一个`test_catalog`（`metalake`需要提前创建）

于是`METALAKE_URL`可以设为

```
http://localhost:8090/api/metalakes/test_metalake/catalogs/
```

source可以写为

```
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

其中`sourceId`指代catalog的名称，从而其他项可以使用`${}`占位符，运行时会自动替换。注意，在sink中使用时，同样叫`sourceId`；使用占位符时必须以`${`开头，以`}`结尾，每一项最多只能包含一个占位符，占位符以外也可以有内容