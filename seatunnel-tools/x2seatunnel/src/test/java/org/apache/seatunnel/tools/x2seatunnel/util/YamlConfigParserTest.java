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
package org.apache.seatunnel.tools.x2seatunnel.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for YamlConfigParser, verifying YAML configuration mapping is correct */
public class YamlConfigParserTest {

    @Test
    public void testParseConversionYaml() {
        // Example file located at resources/examples/datax-mysql2hdfs2hive.yaml
        String yamlPath = "src/main/resources/examples/yaml/datax-mysql2hdfs2hive.yaml";
        ConversionConfig config = YamlConfigParser.parse(yamlPath);
        Assertions.assertNotNull(config);
        Assertions.assertEquals("examples/source/datax-mysql2hdfs2hive.json", config.getSource());
        Assertions.assertEquals("datax", config.getSourceType());
        Assertions.assertEquals("examples/target/mysql2hdfs2hive-result.conf", config.getTarget());
        Assertions.assertEquals("examples/report/mysql2hdfs2hive-report.md", config.getReport());
        Assertions.assertEquals("datax/custom/mysql-to-hive.conf", config.getTemplate());
        Assertions.assertTrue(config.isVerbose(), "YAML options.verbose should be true");
    }

    @Test
    public void testParseSimpleYamlWithStringSource() {
        // Dynamically create and parse simple YAML, containing only source field
        String yamlContent = "source: foo.json\n" + "target: bar.conf\n" + "report: report.md\n";
        try {
            java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("test", ".yaml");
            java.nio.file.Files.write(tempFile, yamlContent.getBytes());
            ConversionConfig config = YamlConfigParser.parse(tempFile.toString());
            Assertions.assertEquals("foo.json", config.getSource());
            Assertions.assertEquals("bar.conf", config.getTarget());
            Assertions.assertEquals("report.md", config.getReport());
            // Default values
            Assertions.assertNull(config.getTemplate());
            Assertions.assertFalse(config.isVerbose());
        } catch (Exception e) {
            Assertions.fail("Failed to parse simple YAML: " + e.getMessage());
        }
    }
}
