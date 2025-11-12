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

package org.apache.seatunnel.connectors.seatunnel.file.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.JsonReadStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class FileFilterPatternTest {
    /**
     * filter based on the file directory at the same time, the expression needs to start with
     * `path`
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    @Test
    @DisabledOnOs(OS.WINDOWS)
    public void testJsonFilterPatternWithFilePath() throws URISyntaxException, IOException {
        URL filterPattern = FileFilterPatternTest.class.getResource("/filter-pattern/json");
        URL conf =
                ExcelReadStrategyTest.class.getResource(
                        "/filter-pattern/json/json2025/test_read_json.conf");
        Assertions.assertNotNull(filterPattern);
        Assertions.assertNotNull(conf);
        // path
        String jsonPathDir = filterPattern.toURI().getPath();
        // the expression needs to start with `path`
        String fileFilterPattern = jsonPathDir + "/json202[^/]*/.*.json";

        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig =
                ConfigFactory.parseFile(new File(confPath))
                        .withValue(
                                FileBaseSourceOptions.FILE_FILTER_PATTERN.key(),
                                ConfigValueFactory.fromAnyRef(fileFilterPattern))
                        .withValue(
                                FileBaseSourceOptions.FILE_PATH.key(),
                                ConfigValueFactory.fromAnyRef(jsonPathDir));

        JsonReadStrategy jsonReadStrategy = new JsonReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        jsonReadStrategy.setPluginConfig(pluginConfig);
        jsonReadStrategy.init(localConf);

        List<String> filterFileNames = jsonReadStrategy.getFileNamesByPath(jsonPathDir);
        Assertions.assertEquals(2, filterFileNames.size());
        String fileName = filterFileNames.get(0);
        Assertions.assertTrue(fileName.endsWith(".json"));
    }

    /**
     * filter based on file names, just simply write the regular file names
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    @Test
    @DisabledOnOs(OS.WINDOWS)
    public void testJsonFilterPatternWithFileName() throws URISyntaxException, IOException {
        URL filterPattern = FileFilterPatternTest.class.getResource("/filter-pattern/json");
        URL conf =
                ExcelReadStrategyTest.class.getResource(
                        "/filter-pattern/json/json2025/test_read_json.conf");
        Assertions.assertNotNull(filterPattern);
        Assertions.assertNotNull(conf);
        // path
        String jsonPathDir = filterPattern.toURI().getPath();
        // just simply write the regular file names
        String fileFilterPattern = ".*.json";
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig =
                ConfigFactory.parseFile(new File(confPath))
                        .withValue(
                                FileBaseSourceOptions.FILE_FILTER_PATTERN.key(),
                                ConfigValueFactory.fromAnyRef(fileFilterPattern))
                        .withValue(
                                FileBaseSourceOptions.FILE_PATH.key(),
                                ConfigValueFactory.fromAnyRef(jsonPathDir));
        JsonReadStrategy jsonReadStrategy = new JsonReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        jsonReadStrategy.setPluginConfig(pluginConfig);
        jsonReadStrategy.init(localConf);

        List<String> filterFileNames = jsonReadStrategy.getFileNamesByPath(jsonPathDir);
        Assertions.assertEquals(3, filterFileNames.size());
        for (String fileName : filterFileNames) {
            Assertions.assertTrue(fileName.endsWith(".json"));
        }
    }

    public static class LocalConf extends HadoopConf {
        private static final String HDFS_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
        private static final String SCHEMA = "file";

        public LocalConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return HDFS_IMPL;
        }

        @Override
        public String getSchema() {
            return SCHEMA;
        }
    }
}
