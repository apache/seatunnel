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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TemplateVariableResolver unit tests */
public class TemplateVariableResolverTest {

    private TemplateVariableResolver resolver;
    private String testDataXJson;

    @BeforeEach
    public void setUp() {
        resolver = new TemplateVariableResolver();

        // Simplified DataX configuration JSON string
        testDataXJson =
                "{\n"
                        + "  \"job\": {\n"
                        + "    \"content\": [{\n"
                        + "      \"reader\": {\n"
                        + "        \"name\": \"mysqlreader\",\n"
                        + "        \"parameter\": {\n"
                        + "          \"username\": \"root\",\n"
                        + "          \"connection\": [{\n"
                        + "            \"jdbcUrl\": [\"jdbc:mysql://localhost:3306/test_db\"],\n"
                        + "            \"table\": [\"user_info\"]\n"
                        + "          }]\n"
                        + "        }\n"
                        + "      },\n"
                        + "      \"writer\": {\n"
                        + "        \"parameter\": {\n"
                        + "          \"path\": \"/warehouse/test_ods/ods_user_info/\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }]\n"
                        + "  }\n"
                        + "}";
    }

    @Test
    public void testBasicVariableResolution() {
        String template = "username: {{ datax.job.content[0].reader.parameter.username }}";
        String result = resolver.resolve(template, testDataXJson);
        assertEquals("username: root", result);
    }

    @Test
    public void testRegexVariableResolution() {
        String template =
                "database: {{ datax.job.content[0].writer.parameter.path | regex_extract('/warehouse/([^/]+)/.*', '$1') | default('default_db') }}";
        String result = resolver.resolve(template, testDataXJson);
        assertEquals("database: test_ods", result);
    }

    @Test
    public void testComplexTemplate() {
        String template =
                "source {\n"
                        + "  Jdbc {\n"
                        + "    url = \"{{ datax.job.content[0].reader.parameter.connection[0].jdbcUrl[0] }}\"\n"
                        + "    user = \"{{ datax.job.content[0].reader.parameter.username }}\"\n"
                        + "    table = \"{{ datax.job.content[0].reader.parameter.connection[0].table[0] }}\"\n"
                        + "  }\n"
                        + "}";

        String result = resolver.resolve(template, testDataXJson);

        assertTrue(result.contains("url = \"jdbc:mysql://localhost:3306/test_db\""));
        assertTrue(result.contains("user = \"root\""));
        assertTrue(result.contains("table = \"user_info\""));
    }

    @Test
    public void testDefaultValue() {
        String template =
                "host: {{ datax.job.content[0].reader.parameter.host | default('localhost') }}";
        String result = resolver.resolve(template, testDataXJson);
        assertEquals("host: localhost", result);
    }
}
