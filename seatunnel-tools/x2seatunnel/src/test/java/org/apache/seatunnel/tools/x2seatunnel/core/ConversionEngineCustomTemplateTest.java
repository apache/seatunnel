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

package org.apache.seatunnel.tools.x2seatunnel.core;

import org.apache.seatunnel.tools.x2seatunnel.util.FileUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ConversionEngine 自定义模板转换集成测试 */
public class ConversionEngineCustomTemplateTest {

    @TempDir Path tempDir;

    private ConversionEngine conversionEngine;
    private String testDataXConfigPath;
    private String testOutputPath;

    @BeforeEach
    public void setUp() {
        conversionEngine = new ConversionEngine();

        // 创建测试用DataX配置文件
        String testDataXConfig =
                "{\n"
                        + "  \"job\": {\n"
                        + "    \"setting\": {\n"
                        + "      \"speed\": {\n"
                        + "        \"channel\": 1\n"
                        + "      }\n"
                        + "    },\n"
                        + "    \"content\": [\n"
                        + "      {\n"
                        + "        \"reader\": {\n"
                        + "          \"name\": \"mysqlreader\",\n"
                        + "          \"parameter\": {\n"
                        + "            \"username\": \"root\",\n"
                        + "            \"password\": \"123456\",\n"
                        + "            \"connection\": [\n"
                        + "              {\n"
                        + "                \"querySql\": [\"SELECT * FROM user_info\"],\n"
                        + "                \"jdbcUrl\": [\"jdbc:mysql://localhost:3306/test_db\"]\n"
                        + "              }\n"
                        + "            ]\n"
                        + "          }\n"
                        + "        },\n"
                        + "        \"writer\": {\n"
                        + "          \"name\": \"hdfswriter\",\n"
                        + "          \"parameter\": {\n"
                        + "            \"defaultFS\": \"hdfs://localhost:9000\",\n"
                        + "            \"path\": \"/warehouse/ecology_ods/ods_user_info/\",\n"
                        + "            \"fileType\": \"parquet\"\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }\n"
                        + "}";

        testDataXConfigPath =
                new File(tempDir.toFile(), "test-datax-config.json").getAbsolutePath();
        testOutputPath = new File(tempDir.toFile(), "test-output.conf").getAbsolutePath();

        // 写入测试配置文件
        FileUtils.writeFile(testDataXConfigPath, testDataXConfig);
    }

    @Test
    public void testMysqlToHiveCustomTemplateConversion() {
        // 测试MySQL到Hive的自定义模板转换
        conversionEngine.convert(
                testDataXConfigPath,
                testOutputPath,
                "datax",
                "seatunnel",
                "datax/custom/mysql-to-hive.conf",
                null);

        // 验证输出文件存在
        assertTrue(FileUtils.exists(testOutputPath), "输出文件应该存在");

        // 读取并验证输出内容
        String outputContent = FileUtils.readFile(testOutputPath);
        assertNotNull(outputContent, "输出内容不能为空");

        // 验证模板内容被正确加载（至少包含基本的配置结构）
        assertTrue(outputContent.contains("env {"), "应该包含env配置块");
        assertTrue(outputContent.contains("source {"), "应该包含source配置块");
        assertTrue(outputContent.contains("sink {"), "应该包含sink配置块");

        System.out.println("生成的MySQL到Hive配置内容:");
        System.out.println(outputContent);
    }
}
