/*
* Licensed to the Apache Software Foundation (ASF) under one or mo    @Test
   public vo    @Test
   public void    @Test
   public void testMissingFieldTracking() {
       // 测试缺失字段跟踪
       String template = "host: {{ datax.job.content[0].reader.parameter.nonexistent }}}}";

       String result = resolver.resolve(template, testDataXJson);

       Assertions.assertEquals("host: ", result); // 缺失字段应返回空字符串aultValueUsage() {
       // 测试默认值使用并跟踪
       String template =
               "host: {{ datax.job.content[0].reader.parameter.host | default('localhost') }}}}";

       String result = resolver.resolve(template, testDataXJson);

       Assertions.assertEquals("host: localhost", result);sicFieldExtraction() {
       // 测试基础字段提取并跟踪映射过程
       String template = "user: {{ datax.job.content[0].reader.parameter.username }}}}";

       String result = resolver.resolve(template, testDataXJson);

       Assertions.assertEquals("user: root", result);ontributor license agreements.  See the NOTICE file distributed with
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

import org.apache.seatunnel.tools.x2seatunnel.model.MappingResult;
import org.apache.seatunnel.tools.x2seatunnel.model.MappingTracker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** TemplateVariableResolver 与 MappingTracker 集成测试 */
public class TemplateVariableResolverMappingTest {

    private TemplateVariableResolver resolver;
    private MappingTracker mappingTracker;
    private String testDataXJson;

    @BeforeEach
    public void setUp() {
        mappingTracker = new MappingTracker();
        resolver = new TemplateVariableResolver(null, mappingTracker);

        // 测试用的DataX配置JSON
        testDataXJson =
                "{\n"
                        + "  \"job\": {\n"
                        + "    \"content\": [{\n"
                        + "      \"reader\": {\n"
                        + "        \"name\": \"mysqlreader\",\n"
                        + "        \"parameter\": {\n"
                        + "          \"username\": \"root\",\n"
                        + "          \"password\": \"123456\",\n"
                        + "          \"connection\": [{\n"
                        + "            \"jdbcUrl\": [\"jdbc:mysql://localhost:3306/test_db\"],\n"
                        + "            \"table\": [\"user_info\"]\n"
                        + "          }]\n"
                        + "        }\n"
                        + "      },\n"
                        + "      \"writer\": {\n"
                        + "        \"name\": \"hdfswriter\",\n"
                        + "        \"parameter\": {\n"
                        + "          \"path\": \"/warehouse/ecology_ods/ods_user_info/\",\n"
                        + "          \"fileType\": \"orc\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }],\n"
                        + "    \"setting\": {\n"
                        + "      \"speed\": {\n"
                        + "        \"channel\": 3\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";
    }

    @Test
    public void testBasicFieldExtraction() {
        // 测试基础字段提取并跟踪映射过程
        String template = "user: {{ datax.job.content[0].reader.parameter.username }}";

        String result = resolver.resolve(template, testDataXJson);

        Assertions.assertEquals("user: root", result);

        // 验证映射跟踪
        MappingResult mappingResult = mappingTracker.generateMappingResult();
        Assertions.assertEquals(1, mappingResult.getSuccessMappings().size());
        Assertions.assertEquals(
                "job.content[0].reader.parameter.username",
                mappingResult.getSuccessMappings().get(0).getSourceField());
        Assertions.assertEquals("root", mappingResult.getSuccessMappings().get(0).getValue());
    }

    @Test
    public void testDefaultValueUsage() {
        // 测试默认值使用并跟踪
        String template =
                "host: {{ datax.job.content[0].reader.parameter.host | default('localhost') }}";

        String result = resolver.resolve(template, testDataXJson);

        Assertions.assertEquals("host: localhost", result);

        // 验证映射跟踪 - 默认值应该被记录
        MappingResult mappingResult = mappingTracker.generateMappingResult();
        Assertions.assertEquals(1, mappingResult.getDefaultValues().size());
        Assertions.assertEquals("localhost", mappingResult.getDefaultValues().get(0).getValue());
        Assertions.assertTrue(
                mappingResult.getDefaultValues().get(0).getReason().contains("应用默认值"));
    }

    @Test
    public void testMissingFieldTracking() {
        // 测试缺失字段跟踪
        String template = "host: {{ datax.job.content[0].reader.parameter.nonexistent }}";

        String result = resolver.resolve(template, testDataXJson);

        Assertions.assertEquals("host: ", result); // 缺失字段应返回空字符串

        // 验证映射跟踪 - 缺失字段应该被记录
        MappingResult mappingResult = mappingTracker.generateMappingResult();
        Assertions.assertTrue(mappingResult.getMissingRequiredFields().size() >= 1);

        // 查找对应的缺失字段
        boolean foundMissingField =
                mappingResult.getMissingRequiredFields().stream()
                        .anyMatch(
                                field ->
                                        field.getFieldName()
                                                .equals(
                                                        "job.content[0].reader.parameter.nonexistent"));
        Assertions.assertTrue(foundMissingField);
    }

    @Test
    public void testFilterTransformationTracking() {
        // 测试过滤器转换跟踪
        String template = "username: {{ datax.job.content[0].reader.parameter.username | upper }}";

        String result = resolver.resolve(template, testDataXJson);

        Assertions.assertEquals("username: ROOT", result);

        // 验证映射跟踪 - 过滤器转换应该被记录为转换映射
        MappingResult mappingResult = mappingTracker.generateMappingResult();

        // 原字段提取记录为直接映射
        Assertions.assertTrue(mappingResult.getSuccessMappings().size() >= 1);
        Assertions.assertEquals("root", mappingResult.getSuccessMappings().get(0).getValue());

        // 过滤器转换记录为转换映射
        Assertions.assertEquals(1, mappingResult.getTransformMappings().size());
        Assertions.assertEquals("ROOT", mappingResult.getTransformMappings().get(0).getValue());
        Assertions.assertTrue(
                mappingResult.getTransformMappings().get(0).getFilterName().contains("upper"));
    }

    @Test
    public void testComplexTemplateWithMixedMappingTypes() {
        // 测试复杂模板，包含多种映射类型
        String template =
                "source {\n"
                        + "  Jdbc {\n"
                        + "    url = \"{{ datax.job.content[0].reader.parameter.connection[0].jdbcUrl[0] }}\"\n"
                        + "    user = \"{{ datax.job.content[0].reader.parameter.username }}\"\n"
                        + "    password = \"{{ datax.job.content[0].reader.parameter.password }}\"\n"
                        + "    table = \"{{ datax.job.content[0].reader.parameter.connection[0].table[0] }}\"\n"
                        + "    port = \"{{ datax.job.content[0].reader.parameter.port | default('3306') }}\"\n"
                        + "    driver = \"{{ datax.job.content[0].reader.parameter.driver | default('com.mysql.cj.jdbc.Driver') }}\"\n"
                        + "    fetchSize = \"{{ datax.job.content[0].reader.parameter.fetchSize }}\"\n"
                        + "  }\n"
                        + "}";

        String result = resolver.resolve(template, testDataXJson);

        // 验证解析结果
        Assertions.assertTrue(result.contains("url = \"jdbc:mysql://localhost:3306/test_db\""));
        Assertions.assertTrue(result.contains("user = \"root\""));
        Assertions.assertTrue(result.contains("password = \"123456\""));
        Assertions.assertTrue(result.contains("table = \"user_info\""));
        Assertions.assertTrue(result.contains("port = \"3306\""));
        Assertions.assertTrue(result.contains("driver = \"com.mysql.cj.jdbc.Driver\""));
        Assertions.assertTrue(result.contains("fetchSize = \"\""));

        // 验证映射统计
        MappingResult mappingResult = mappingTracker.generateMappingResult();

        // 直接映射：url, user, password, table
        Assertions.assertEquals(4, mappingResult.getSuccessMappings().size());

        // 默认值：port, driver
        Assertions.assertEquals(2, mappingResult.getDefaultValues().size());

        // 缺失字段：fetchSize
        Assertions.assertEquals(1, mappingResult.getMissingRequiredFields().size());

        // 验证统计总数
        int totalFields =
                mappingResult.getSuccessMappings().size()
                        + mappingResult.getTransformMappings().size()
                        + mappingResult.getDefaultValues().size()
                        + mappingResult.getMissingRequiredFields().size()
                        + mappingResult.getUnmappedFields().size();
        Assertions.assertEquals(7, totalFields); // 与模板中的字段数量一致
    }

    @Test
    public void testMappingTrackerReset() {
        // 测试 MappingTracker 重置功能
        String template1 = "user: {{ datax.job.content[0].reader.parameter.username }}";
        resolver.resolve(template1, testDataXJson);

        MappingResult result1 = mappingTracker.generateMappingResult();
        Assertions.assertEquals(1, result1.getSuccessMappings().size());

        // 重置跟踪器
        mappingTracker.reset();

        String template2 = "password: {{ datax.job.content[0].reader.parameter.password }}";
        resolver.resolve(template2, testDataXJson);

        MappingResult result2 = mappingTracker.generateMappingResult();
        Assertions.assertEquals(1, result2.getSuccessMappings().size());
        Assertions.assertEquals(
                "job.content[0].reader.parameter.password",
                result2.getSuccessMappings().get(0).getSourceField());
    }

    @Test
    public void testRegexFilterWithMappingTracking() {
        // 测试正则表达式过滤器与映射跟踪
        String template =
                "database: {{ datax.job.content[0].writer.parameter.path | regex_extract('/warehouse/([^/]+)/.*', '$1') | default('unknown') }}";

        String result = resolver.resolve(template, testDataXJson);

        Assertions.assertEquals("database: ecology_ods", result);

        // 验证映射跟踪
        MappingResult mappingResult = mappingTracker.generateMappingResult();

        // 原路径提取为直接映射
        Assertions.assertTrue(mappingResult.getSuccessMappings().size() >= 1);
        Assertions.assertEquals(
                "/warehouse/ecology_ods/ods_user_info/",
                mappingResult.getSuccessMappings().get(0).getValue());

        // 正则提取为转换映射
        Assertions.assertEquals(1, mappingResult.getTransformMappings().size());
        Assertions.assertEquals(
                "ecology_ods", mappingResult.getTransformMappings().get(0).getValue());
        Assertions.assertTrue(
                mappingResult
                        .getTransformMappings()
                        .get(0)
                        .getFilterName()
                        .contains("regex_extract"));
    }
}
