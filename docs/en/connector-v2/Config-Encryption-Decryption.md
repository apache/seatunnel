# Config File Encryption And Decryption

## Introduction

In most production environments, sensitive configuration items such as passwords are required to be encrypted and cannot be stored in plain text, SeaTunnel provides a convenient one-stop solution for this.

## How to use

SeaTunnel comes with the function of base64 encryption and decryption, but it is not recommended for production use, it is recommended that users implement custom encryption and decryption logic. You can refer to this chapter [How to implement user-defined encryption and decryption](#How to implement user-defined encryption and decryption) get more details about it.

Base64 encryption support encrypt the following parameters:
- username
- password
- auth
- token
- access_key
- secret_key

Next, I'll show how to quickly use SeaTunnel's own `base64` encryption:

1. And a new option `shade.identifier` in env block of config file, this option indicate what the encryption method that you want to use, in this example, we should add `shade.identifier = base64` in config as the following shown:

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
     # choose stdout output plugin to output data to console
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
2. Using the shell based on different calculate engine to encrypt config file, in this example we use zeta:

   ```shell
   ${SEATUNNEL_HOME}/bin/seatunnel.sh --config config/v2.batch.template --encrypt
   ```

   Then you can see the encrypted configuration file in the terminal:

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
3. Of course, not only encrypted configuration files are supported, but if the user wants to see the decrypted configuration file, you can execute this command:

   ```shell
   ${SEATUNNEL_HOME}/bin/seatunnel.sh --config config/v2.batch.template --decrypt
   ```

## How to implement user-defined encryption and decryption

If you want to customize the encryption method and the configuration of the encryption, this section will help you to solve the problem.

1. Create a java maven project

2. Add `seatunnel-api` module with the provided scope in dependencies like the following shown:

   ```xml
   <dependency>
       <groupId>org.apache.seatunnel</groupId>
       <artifactId>seatunnel-api</artifactId>
       <version>${seatunnel.version}</version>
       <scope>provided</scope>
   </dependency>
   ```
3. Create a new class and implement interface `ConfigShade`, this interface has the following methods:

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
4. Create a file named `org.apache.seatunnel.api.configuration.ConfigShade` in `resources/META-INF/services`, the file content should be the fully qualified class name of the class that you defined in step 3.

5. Package it to jar and add jar to `${SEATUNNEL_HOME}/lib`
6. Change the option `shade.identifier` to the value that you defined in `ConfigShade#getIdentifier`of you config file, please enjoy it \^_\^

### How to encrypt and decrypt with customized params

If you want to encrypt and decrypt with customized params, you can follow the steps below:
1. Add a configuration named `shade.properties` in the env part of the configuration file, the value of this configuration is in the form of key-value pairs (the type of the key must be a string), as shown below:

   ```hocon
    env {
        shade.properties = {
           suffix = "666"
        }
    }

   ```

2. Override the `ConfigShade` interface's `open` method, as shown below:

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
3. Use the parameters passed in the open method in the encryption and decryption methods, as shown below:

   ```java
       public String encrypt(String content) {
           return content + suffix;
       }

       public String decrypt(String content) {
           return content.substring(0, content.length() - suffix.length());
       }
   ```