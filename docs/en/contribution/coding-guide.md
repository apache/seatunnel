# Coding guide

## Modules Overview

| Module Name                            | Introduction                                                 |
| -------------------------------------- | ------------------------------------------------------------ |
| seatunnel-api                          | SeaTunnel connector V2 API module                            |
| seatunnel-apis                         | SeaTunnel connector V1 API module                            |
| seatunnel-common                       | SeaTunnel common module                                      |
| seatunnel-connectors                   | SeaTunnel connector V1 module, currently connector V1 is in a stable state, the community will continue to maintain it, but there will be no major feature updates |
| seatunnel-connectors-v2                | SeaTunnel connector V2 module, currently connector V2 is under development and the community will focus on it |
| seatunnel-core/seatunnel-spark         | SeaTunnel core starter module of connector V1 on spark engine |
| seatunnel-core/seatunnel-flink         | SeaTunnel core starter module of connector V1 on flink engine |
| seatunnel-core/seatunnel-flink-sql     | SeaTunnel core starter module of connector V1 on flink-sql engine |
| seatunnel-core/seatunnel-spark-starter | SeaTunnel core starter module of connector V2 on Spark engine |
| seatunnel-core/seatunnel-flink-starter | SeaTunnel core starter module of connector V2 on Flink engine |
| seatunnel-core/seatunnel-starter       | SeaTunnel core starter module of connector V2 on SeaTunnel engine |
| seatunnel-e2e                          | SeaTunnel end-to-end test module                             |
| seatunnel-examples                     | SeaTunnel local examples module, developer can use it to do unit test and integration test |
| seatunnel-engine                       | SeaTunnel engine module, seatunnel-engine is a new computational engine developed by the SeaTunnel Community that focuses on data synchronization. |
| seatunnel-formats                      | SeaTunnel formats module, used to offer the ability of formatting data |
| seatunnel-plugin-discovery             | SeaTunnel plugin discovery moudle, used to offer the ability of loading SPI plugins from classpath |
| seatunnel-transforms                   | SeaTunnel transform plugin module                            |
| seatunnel-translation                  | SeaTunnel translation module, used to adapt Connector V2 and other computing engines such as Spark Flink etc... |

## Pull Request Rules

1. Create entity classes using annotations in the `lombok` plugin (`@Data` `@Getter` `@Setter` `@NonNull` etc...) to reduce the amount of code

2. If you need to use log4j to print logs in a class, preferably use the annotation `@Slf4j` in the `lombok` plugin

3. Issue, pr submission specification:

   > Title specification: [purpose] [module name] Description

   1. pr purpose includes: `Hotfix`, `Feature`, `Improve`, `Docs`, `WIP`.Please note that if your pr purpose is WIP, then you need to use github's draft pr
   2. issue purpose includes: `Feature`, `Bug`, `Docs`, `Discuss`
   3. module name: the current pr or issue involves the name of the module, for example: `Core`, `Connector-V2`, `Connector-V1`, etc.
   4. description: highly summarize what the current pr and issue to do, as far as possible to do the name to know the meaning

4. The community code style has the following specifications (it is recommended to use the auto-formatted code feature of idea).

   > https://github.com/apache/incubator-seatunnel/pull/2641
   >
   > #2641 will introduce the spotless plugin, contributors need to use the plugin to format the code before submitting pr
   1. Indent to 4 spaces
   2. Keyword (try if else catch...) there should be 1 space between keyword and `(`
   3. There should be 1 space between `)` and `{`
   4. There should be 1 space between parameter splitting
   5. If there are more than 3 chain calls, there should be a separate line for each call
   6. If-else should be wrapped in `{}` even if the method body is only one line
   7. etc....

   Tips: **For more details, please check `checkStyle.xml` that at tools/checkstyle**

5. Code segments are never repeated. If a code segment is used multiple times, you should not define it multiple times, but make it a public segment for other modules to use

6. When throwing an exception, you need to throw the exception along with a hint message and the exception should be smaller in scope.Throwing overly broad exceptions promotes complex error handling code that is more likely to contain security vulnerabilities.For example, if your connector encounters an `IOException` while reading data, a reasonable approach would be to the following:

   ```java
   try {
       // read logic
   } catch (IOException e) {
       throw IOException("Meet a IOException, it might has some problems between client and database", e);
   }
   ```

7. Please check that each new code file you add contains the `Apache License Header` before submitting pr:

   ```java
   /*
    * Licensed to the Apache Software Foundation (ASF) under one or more
    * contributor license agreements.  See the NOTICE file distributed with
    * this work for additional information regarding copyright ownership.
    * The ASF licenses this file to You under the Apache License, Version 2.0
    * (the "License"); you may not use this file except in compliance with
    * the License.  You may obtain a copy of the License at
    *
    *    http://www.apache.org/licenses/LICENSE-2.0
    *
    * Unless required by applicable law or agreed to in writing, software
    * distributed under the License is distributed on an "AS IS" BASIS,
    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    * See the License for the specific language governing permissions and
    * limitations under the License.
    */
   ```

8. Please check the format of your code before submitting pr:

   ```shell
   ./mvnw checkstyle:check
   ```

9. Please test the entire project for compilation before submitting pr:

   ```shell
   # multi threads compile
   ./mvnw -T 1C clean package
   ```

   ```shell
   # single thread compile
   ./mvnw clean package
   ```

10. Before submitting pr, please do a full unit test and integration test locally, best practice is to use the `seatunnel-examples` module's ability to self-test to ensure that the multi-engine is running properly and the results are correct

11. If you submit a pr with a feature that requires updated documentation, please update the documentation

12. If a new dependency is introduced in pr, you need to add the license declaration of the new dependency in the specified location

13. Submit the pr of connector type can write e2e test to ensure the robustness and robustness of the code, e2e test should include the full data type, and e2e test as little as possible to initialize the docker image, write the test cases of sink and source together to reduce the loss of resources, while using asynchronous features to ensure the stability of the test. A good example can be found at: [MongodbIT.java](https://github.com/apache/incubator-seatunnel/blob/dev/seatunnel-e2e/seatunnel-flink-connector-v2-e2e/connector-mongodb-flink-e2e/src/test/java/org/apache/seatunnel/e2e/flink/v2/mongodb/MongodbIT.java)

14. The priority of property permission in the class is set to `private`, and mutability is set to `final`, which can be changed reasonably if special circumstances are encountered

15. The properties in the class and method parameters prefer to use the base type(int boolean double float...), not recommended to use the wrapper type(Integer Boolean Double Float...), if encounter special circumstances reasonable change

16. When developing a sink connector you need to be aware that the sink will be serialized, and if some properties cannot be serialized, encapsulate the properties into classes and use the singleton pattern

17. If there are multiple if process judgments in the code flow, try to simplify the flow to multiple ifs instead of if-else-if

18. Pr has the characteristic of single responsibility, not allowed to include in pr, and you submit the feature of the irrelevant code, once this situation please deal with their own branch before submitting pr, otherwise the community will actively close pr

19. Contributors should be responsible for their own pr. If your pr contains new features or modifies old features, please add test cases or e2e tests to prove the reasonableness and functional integrity of your pr

20. If you think which part of the community's current code is unreasonable (especially the core `core` module and the `api` module), the function needs to be updated or modified, please first propose a `discuss issue` or `email` with the community to discuss the need to modify this part of the function, if the community agrees to submit pr again, do not submit the issue and pr directly without discussion, so the community will directly consider this pr is useless, and will be closed down

