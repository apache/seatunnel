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

package org.apache.seatunnel.tools.x2seatunnel.template;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/** HoconTemplateAnalyzer 单元测试 */
public class HoconTemplateAnalyzerTest {

    private HoconTemplateAnalyzer analyzer;

    @BeforeEach
    public void setUp() {
        analyzer = new HoconTemplateAnalyzer();
    }

    @Test
    public void testExtractFieldVariables_SimpleTemplate() {
        String template =
                "Jdbc {\n"
                        + "    url = \"${datax:job.content[0].reader.parameter.connection[0].jdbcUrl}\"\n"
                        + "    driver = \"${datax:job.content[0].reader.parameter.connection[0].driver}\"\n"
                        + "    username = \"${datax:job.content[0].reader.parameter.username}\"\n"
                        + "    password = \"${datax:job.content[0].reader.parameter.password}\"\n"
                        + "    query = \"${datax:job.content[0].reader.parameter.querySql[0]}\"\n"
                        + "    \n"
                        + "    connection_check_timeout_sec = 60\n"
                        + "    partition_column = \"${datax:job.content[0].reader.parameter.splitPk|}\"\n"
                        + "}";

        Map<String, List<String>> result = analyzer.extractFieldVariables(template, "source");

        // 验证字段路径是否正确
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.containsKey("source.Jdbc.url"));
        Assertions.assertTrue(result.containsKey("source.Jdbc.driver"));
        Assertions.assertTrue(result.containsKey("source.Jdbc.username"));
        Assertions.assertTrue(result.containsKey("source.Jdbc.password"));
        Assertions.assertTrue(result.containsKey("source.Jdbc.query"));
        Assertions.assertTrue(result.containsKey("source.Jdbc.partition_column"));

        // 验证变量提取是否正确
        Assertions.assertEquals(1, result.get("source.Jdbc.url").size());
        Assertions.assertEquals(
                "datax:job.content[0].reader.parameter.connection[0].jdbcUrl",
                result.get("source.Jdbc.url").get(0));

        Assertions.assertEquals(1, result.get("source.Jdbc.driver").size());
        Assertions.assertEquals(
                "datax:job.content[0].reader.parameter.connection[0].driver",
                result.get("source.Jdbc.driver").get(0));

        // 验证带默认值的变量
        Assertions.assertEquals(1, result.get("source.Jdbc.partition_column").size());
        Assertions.assertEquals(
                "datax:job.content[0].reader.parameter.splitPk|",
                result.get("source.Jdbc.partition_column").get(0));
    }

    @Test
    public void testExtractFieldVariables_NestedTemplate() {
        String template =
                "Jdbc {\n"
                        + "    url = \"${datax:job.content[0].writer.parameter.connection[0].jdbcUrl}\"\n"
                        + "    driver = \"${datax:job.content[0].writer.parameter.connection[0].driver}\"\n"
                        + "    \n"
                        + "    database = \"${datax:job.content[0].writer.parameter.connection[0].table[0].database}\"\n"
                        + "    table = \"${datax:job.content[0].writer.parameter.connection[0].table[0].name}\"\n"
                        + "    \n"
                        + "    connection_config {\n"
                        + "        max_retries = 3\n"
                        + "        timeout = \"${datax:job.content[0].writer.parameter.timeout|30}\"\n"
                        + "    }\n"
                        + "    \n"
                        + "    write_mode {\n"
                        + "        mode = \"${datax:job.content[0].writer.parameter.writeMode|insert}\"\n"
                        + "        batch_size = 1000\n"
                        + "    }\n"
                        + "}";

        Map<String, List<String>> result = analyzer.extractFieldVariables(template, "sink");

        // 验证嵌套字段路径
        Assertions.assertTrue(result.containsKey("sink.Jdbc.url"));
        Assertions.assertTrue(result.containsKey("sink.Jdbc.driver"));
        Assertions.assertTrue(result.containsKey("sink.Jdbc.database"));
        Assertions.assertTrue(result.containsKey("sink.Jdbc.table"));
        Assertions.assertTrue(result.containsKey("sink.Jdbc.connection_config.timeout"));
        Assertions.assertTrue(result.containsKey("sink.Jdbc.write_mode.mode"));

        // 验证嵌套字段的变量提取
        Assertions.assertEquals(
                "datax:job.content[0].writer.parameter.timeout|30",
                result.get("sink.Jdbc.connection_config.timeout").get(0));
        Assertions.assertEquals(
                "datax:job.content[0].writer.parameter.writeMode|insert",
                result.get("sink.Jdbc.write_mode.mode").get(0));
    }

    @Test
    public void testValidateTemplate_ValidHocon() {
        String validTemplate =
                "Jdbc {\n"
                        + "    url = \"${datax:job.content[0].reader.parameter.connection[0].jdbcUrl}\"\n"
                        + "    driver = \"com.mysql.cj.jdbc.Driver\"\n"
                        + "    query = \"SELECT * FROM users\"\n"
                        + "}";

        Assertions.assertTrue(analyzer.validateTemplate(validTemplate));
    }

    @Test
    public void testValidateTemplate_InvalidHocon() {
        String invalidTemplate =
                "Jdbc {\n"
                        + "    url = \"${datax:job.content[0].reader.parameter.connection[0].jdbcUrl\"\n"
                        + "    driver = \"com.mysql.cj.jdbc.Driver\n"
                        + "    query = \"SELECT * FROM users\"\n"
                        + "}";

        Assertions.assertFalse(analyzer.validateTemplate(invalidTemplate));
    }

    @Test
    public void testExtractRootKey() {
        String template =
                "Jdbc {\n"
                        + "    url = \"${datax:job.content[0].reader.parameter.connection[0].jdbcUrl}\"\n"
                        + "    driver = \"com.mysql.cj.jdbc.Driver\"\n"
                        + "}";

        String rootKey = analyzer.extractRootKey(template);
        Assertions.assertEquals("Jdbc", rootKey);
    }

    @Test
    public void testExtractFieldVariables_ArrayValues() {
        String template =
                "Kafka {\n"
                        + "    bootstrap.servers = [\"${datax:job.content[0].reader.parameter.server1}\", \"${datax:job.content[0].reader.parameter.server2}\"]\n"
                        + "    topics = [\"${datax:job.content[0].reader.parameter.topic}\"]\n"
                        + "    \n"
                        + "    consumer {\n"
                        + "        group.id = \"${datax:job.content[0].reader.parameter.groupId}\"\n"
                        + "    }\n"
                        + "}";

        Map<String, List<String>> result = analyzer.extractFieldVariables(template, "source");

        // 验证数组字段
        Assertions.assertTrue(result.containsKey("source.Kafka.bootstrap.servers[0]"));
        Assertions.assertTrue(result.containsKey("source.Kafka.bootstrap.servers[1]"));
        Assertions.assertTrue(result.containsKey("source.Kafka.topics[0]"));
        Assertions.assertTrue(result.containsKey("source.Kafka.consumer.group.id"));
    }

    @Test
    public void testExtractFieldVariables_NoVariables() {
        String template =
                "Jdbc {\n"
                        + "    url = \"jdbc:mysql://localhost:3306/test\"\n"
                        + "    driver = \"com.mysql.cj.jdbc.Driver\"\n"
                        + "    username = \"root\"\n"
                        + "    password = \"password\"\n"
                        + "}";

        Map<String, List<String>> result = analyzer.extractFieldVariables(template, "source");

        // 没有变量的字段不应该出现在结果中
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }
}
