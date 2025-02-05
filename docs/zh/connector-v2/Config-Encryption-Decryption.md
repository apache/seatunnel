# 配置文件加密和解密

## 介绍

在大多数生产环境中，需要对敏感的配置项（如密码）进行加密，不能以明文形式存储。SeaTunnel 为此提供了一个方便的一站式解决方案。

## 如何使用

SeaTunnel 具备Base64编码和解码的功能，但不建议在生产环境中使用，SeaTunnel 建议用户根据自身需求，实现个性化的加密和解密逻辑。您可以参考本章节[如何实现用户自定义的加密和解密](#如何实现用户自定义的加密和解密)以获取更多相关细节。

Base64编码支持加密以下参数：
- username
- password
- auth
- token
- access_key
- secret_key

接下来，将展示如何快速使用 SeaTunnel 自带的 `base64` 加密功能：

1. 在配置文件的环境变量（env）部分新增了一个选项 `shade.identifier`。此选项用于表示您想要使用的加密方法。
2. 在这个示例中，我们在配置文件中添加了 `shade.identifier = base64`，如下所示：

   ```hocon
   #
   # Licensed to the Apache Software Foundation (ASF) under one or more
   # contributor license agreements.  See the NOTICE file distributed with
   # this work for additional information regarding copyright ownership.
   # The ASF licenses this file to You under the Apache License, Version 2.0
   # (the "License"); you may not use this file except in compliance with
   # the License.  You may obtain a copy of the License at
   #
   #     http://www.apache.org/licenses/LICENSE-2.0
   #
   # Unless required by applicable law or agreed to in writing, software
   # distributed under the License is distributed on an "AS IS" BASIS,
   # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   # See the License for the specific language governing permissions and
   # limitations under the License.
   #

   env {
     parallelism = 1
     shade.identifier = "base64"
   }

   source {
     MySQL-CDC {
       plugin_output = "fake"
       parallelism = 1
       server-id = 5656
       port = 56725
       hostname = "127.0.0.1"
       username = "seatunnel"
       password = "seatunnel_password"
       database-name = "inventory_vwyw0n"
       table-name = "products"
       base-url = "jdbc:mysql://localhost:56725"
     }
   }

   transform {
   }

   sink {
     # 将数据输出到 Clickhouse。
     Clickhouse {
       host = "localhost:8123"
       database = "default"
       table = "fake_all"
       username = "seatunnel"
       password = "seatunnel_password"

       # cdc options
       primary_key = "id"
       support_upsert = true
     }
   }
   ```
3. 通过Shell脚本调用不同的计算引擎来对配置文件进行加密操作。在本示例中，我们使用 Zeta 引擎对配置文件进行加密。

   ```shell
   ${SEATUNNEL_HOME}/bin/seatunnel.sh --config config/v2.batch.template --encrypt
   ```

   然后，您可以在终端中看到加密后的配置文件。

   ```log
   2023-02-20 17:50:58,319 INFO  org.apache.seatunnel.core.starter.command.ConfEncryptCommand - Encrypt config: 
   {
       "env" : {
           "parallelism" : 1,
           "shade.identifier" : "base64"
       },
       "source" : [
           {
               "base-url" : "jdbc:mysql://localhost:56725",
               "hostname" : "127.0.0.1",
               "password" : "c2VhdHVubmVsX3Bhc3N3b3Jk",
               "port" : 56725,
               "database-name" : "inventory_vwyw0n",
               "parallelism" : 1,
               "plugin_output" : "fake",
               "table-name" : "products",
               "plugin_name" : "MySQL-CDC",
               "server-id" : 5656,
               "username" : "c2VhdHVubmVs"
           }
       ],
       "transform" : [],
       "sink" : [
           {
               "database" : "default",
               "password" : "c2VhdHVubmVsX3Bhc3N3b3Jk",
               "support_upsert" : true,
               "host" : "localhost:8123",
               "plugin_name" : "Clickhouse",
               "primary_key" : "id",
               "table" : "fake_all",
               "username" : "c2VhdHVubmVs"
           }
       ]
   }
   ```
4. 当然，不仅支持加密配置文件，还支持对配置文件的解密。如果用户想要查看解密后的配置文件，可以执行以下命令：

   ```shell
   ${SEATUNNEL_HOME}/bin/seatunnel.sh --config config/v2.batch.template --decrypt
   ```

## 如何实现用户自定义的加密和解密

如果您希望自定义加密方法和加密配置，本章节将帮助您解决问题。

1. 创建一个 java maven 项目

2. 在 maven 依赖中添加 `seatunnel-api` 模块，如下所示:

   ```xml
   <dependency>
       <groupId>org.apache.seatunnel</groupId>
       <artifactId>seatunnel-api</artifactId>
       <version>${seatunnel.version}</version>
       <scope>provided</scope>
   </dependency>
   ```
3. 创建一个 java 类并实现 `ConfigShade` 接口，该接口包含以下方法：

   ```java
   /**
    * The interface that provides the ability to encrypt and decrypt {@link
    * org.apache.seatunnel.shade.com.typesafe.config.Config}
    */
   public interface ConfigShade {

       /**
        * The unique identifier of the current interface, used it to select the correct {@link
        * ConfigShade}
        */
       String getIdentifier();

       /**
        * Encrypt the content
        *
        * @param content The content to encrypt
        */
       String encrypt(String content);

       /**
        * Decrypt the content
        *
        * @param content The content to decrypt
        */
       String decrypt(String content);

       /** To expand the options that user want to encrypt */
       default String[] sensitiveOptions() {
           return new String[0];
       }
   }
   ```
4. 在 `resources/META-INF/services` 目录下创建名为 `org.apache.seatunnel.api.configuration.ConfigShade`的文件， 文件内容是您在步骤 3 中定义的类的完全限定类名。
5. 将其打成 jar 包, 并添加到 `${SEATUNNEL_HOME}/lib` 目录下。
6. 将选项 `shade.identifier` 的值更改为上面定义在配置文件中的 `ConfigShade#getIdentifier` 的值。

### 在加密解密方法中使用自定义参数

如果您想要使用自定义参数进行加密和解密，可以按照以下步骤操作：
1. 在配置文件的env 中添加`shade.properties`配置，该配置的值是键值对形式（键的类型必须是字符串） ，如下所示：

   ```hocon
    env {
        shade.properties = {
           suffix = "666"
        }
    }

   ```
2. 覆写 `ConfigShade` 接口的 `open` 方法，如下所示：

   ```java
    public static class ConfigShadeWithProps implements ConfigShade {

        private String suffix;
        private String identifier = "withProps";

        @Override
        public void open(Map<String, Object> props) {
            this.suffix = String.valueOf(props.get("suffix"));
        }
   }
   ```
   3. 在加密和解密方法中使用open 方法中传入的参数，如下所示：

   ```java
    @Override
    public String encrypt(String content) {
        return content + suffix;
    }

    @Override
    public String decrypt(String content) {
        return content.substring(0, content.length() - suffix.length());
    }
   ```