/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

/** TemplateVariableResolver and MappingTracker integration tests */
public class TemplateVariableResolverMappingTest {

    private TemplateVariableResolver resolver;
    private MappingTracker mappingTracker;
    private String testDataXJson;

    @BeforeEach
    public void setUp() {
        mappingTracker = new MappingTracker();
        resolver = new TemplateVariableResolver(null, mappingTracker);

        // Test DataX configuration JSON
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
                        + "          \"path\": \"/warehouse/test_ods/ods_user_info/\",\n"
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
        // Test basic field extraction and track the mapping process
        String template = "user: {{ datax.job.content[0].reader.parameter.username }}";

        String result = resolver.resolve(template, testDataXJson);

        Assertions.assertEquals("user: root", result);

        // Verify mapping tracking
        MappingResult mappingResult = mappingTracker.generateMappingResult();
        Assertions.assertEquals(1, mappingResult.getSuccessMappings().size());
        Assertions.assertEquals(
                "job.content[0].reader.parameter.username",
                mappingResult.getSuccessMappings().get(0).getSourceField());
        Assertions.assertEquals("root", mappingResult.getSuccessMappings().get(0).getValue());
    }

    @Test
    public void testDefaultValueUsage() {
        // Test default value usage and tracking
        String template =
                "host: {{ datax.job.content[0].reader.parameter.host | default('localhost') }}";

        String result = resolver.resolve(template, testDataXJson);

        Assertions.assertEquals("host: localhost", result);

        // Verify mapping tracking - default values should be recorded
        MappingResult mappingResult = mappingTracker.generateMappingResult();
        Assertions.assertEquals(1, mappingResult.getDefaultValues().size());
        Assertions.assertEquals("localhost", mappingResult.getDefaultValues().get(0).getValue());
        Assertions.assertTrue(
                mappingResult.getDefaultValues().get(0).getReason().contains("default value"));
    }

    @Test
    public void testMissingFieldTracking() {
        // Test missing field tracking
        String template = "host: {{ datax.job.content[0].reader.parameter.nonexistent }}";

        String result = resolver.resolve(template, testDataXJson);

        // Missing field should return an empty string
        Assertions.assertEquals("host: ", result);

        // Verify mapping tracking - missing fields should be recorded
        MappingResult mappingResult = mappingTracker.generateMappingResult();
        Assertions.assertTrue(mappingResult.getMissingRequiredFields().size() >= 1);

        // Find the corresponding missing field
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
        // Test filter transformation tracking
        String template = "username: {{ datax.job.content[0].reader.parameter.username | upper }}";

        String result = resolver.resolve(template, testDataXJson);

        Assertions.assertEquals("username: ROOT", result);

        // Verify mapping tracking - filter transformations should be recorded as transformation
        // mappings
        MappingResult mappingResult = mappingTracker.generateMappingResult();

        // Original field extraction is recorded as a direct mapping
        Assertions.assertTrue(mappingResult.getSuccessMappings().size() >= 1);
        Assertions.assertEquals("root", mappingResult.getSuccessMappings().get(0).getValue());

        // Filter transformation is recorded as a transformation mapping
        Assertions.assertEquals(1, mappingResult.getTransformMappings().size());
        Assertions.assertEquals("ROOT", mappingResult.getTransformMappings().get(0).getValue());
        Assertions.assertTrue(
                mappingResult.getTransformMappings().get(0).getFilterName().contains("upper"));
    }

    @Test
    public void testComplexTemplateWithMixedMappingTypes() {
        // Test complex template with mixed mapping types
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

        // Verify parsing result
        Assertions.assertTrue(result.contains("url = \"jdbc:mysql://localhost:3306/test_db\""));
        Assertions.assertTrue(result.contains("user = \"root\""));
        Assertions.assertTrue(result.contains("password = \"123456\""));
        Assertions.assertTrue(result.contains("table = \"user_info\""));
        Assertions.assertTrue(result.contains("port = \"3306\""));
        Assertions.assertTrue(result.contains("driver = \"com.mysql.cj.jdbc.Driver\""));
        Assertions.assertTrue(result.contains("fetchSize = \"\""));

        // Verify mapping statistics
        MappingResult mappingResult = mappingTracker.generateMappingResult();

        // Direct mappings: url, user, password, table
        Assertions.assertEquals(4, mappingResult.getSuccessMappings().size());

        // Default values: port, driver
        Assertions.assertEquals(2, mappingResult.getDefaultValues().size());

        // Missing fields: fetchSize
        Assertions.assertEquals(1, mappingResult.getMissingRequiredFields().size());

        // Verify total count
        int totalFields =
                mappingResult.getSuccessMappings().size()
                        + mappingResult.getTransformMappings().size()
                        + mappingResult.getDefaultValues().size()
                        + mappingResult.getMissingRequiredFields().size()
                        + mappingResult.getUnmappedFields().size();

        // Should match the number of fields in the template
        Assertions.assertEquals(7, totalFields);
    }

    @Test
    public void testMappingTrackerReset() {
        // Test MappingTracker reset functionality
        String template1 = "user: {{ datax.job.content[0].reader.parameter.username }}";
        resolver.resolve(template1, testDataXJson);

        MappingResult result1 = mappingTracker.generateMappingResult();
        Assertions.assertEquals(1, result1.getSuccessMappings().size());

        // Reset the tracker
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
        // Test regex filter with mapping tracking
        String template =
                "database: {{ datax.job.content[0].writer.parameter.path | regex_extract('/warehouse/([^/]+)/.*', '$1') | default('unknown') }}";

        String result = resolver.resolve(template, testDataXJson);

        Assertions.assertEquals("database: test_ods", result);

        // Verify mapping tracking
        MappingResult mappingResult = mappingTracker.generateMappingResult();

        // Original path extraction is a direct mapping
        Assertions.assertTrue(mappingResult.getSuccessMappings().size() >= 1);
        Assertions.assertEquals(
                "/warehouse/test_ods/ods_user_info/",
                mappingResult.getSuccessMappings().get(0).getValue());

        // Regex extraction is a transformation mapping
        Assertions.assertEquals(1, mappingResult.getTransformMappings().size());
        Assertions.assertEquals("test_ods", mappingResult.getTransformMappings().get(0).getValue());
        Assertions.assertTrue(
                mappingResult
                        .getTransformMappings()
                        .get(0)
                        .getFilterName()
                        .contains("regex_extract"));
    }
}
