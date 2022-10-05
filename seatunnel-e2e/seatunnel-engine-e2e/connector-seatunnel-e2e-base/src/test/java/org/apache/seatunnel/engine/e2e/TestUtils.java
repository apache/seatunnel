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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.VariablesSubstitute;

import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Map;

public class TestUtils {
    public static String getResource(String confFile) {
        return System.getProperty("user.dir") + File.separator + "src" + File.separator + "test" + File.separator + "resources" + File.separator + confFile;
    }

    /**
     * For reduce the config files num, we can define a job config template and then create new job config file base on it.
     *
     * @param templateFile   The basic job configuration file, which often contains some content that needs to be replaced
     *                       at runtime, generates a new final job configuration file for testing after replacement
     * @param valueMap  replace kv
     * @param targetFilePath    The new config file path
     */
    public static void createTestConfigFileFromTemplate(@NonNull String templateFile,
                                                        @NonNull Map<String, String> valueMap,
                                                        @NonNull String targetFilePath) {
        String templateFilePath = getResource(templateFile);
        String confContent = FileUtils.readFile2Str(Paths.get(templateFilePath));
        String targetConfContent = VariablesSubstitute.substitute(confContent, valueMap);
        FileUtils.createNewFile(targetFilePath);
        FileUtils.writeStringToFile(targetFilePath, targetConfContent);
    }

    public static String getClusterName(String testClassName) {
        return System.getProperty("user.name") + "_" + testClassName;
    }

    public static void initPluginDir() {
        // TODO change connector get method
        // copy connectors to project_root/connectors dir
        System.setProperty("SEATUNNEL_HOME", System.getProperty("user.dir") +
            String.format("%s..%s..%s..%s", File.separator, File.separator, File.separator, File.separator));
        File seatunnelRootDir = new File(System.getProperty("SEATUNNEL_HOME"));

        File connectorDir = new File(seatunnelRootDir +
            File.separator +
            "connectors/seatunnel");

        if (connectorDir.exists()) {
            connectorDir.delete();
        }

        connectorDir.mkdirs();

        File connectorDistDir = new File(
            seatunnelRootDir +
                File.separator +
                "seatunnel-connectors-v2-dist" +
                File.separator +
                "target" +
                File.separator +
                "lib");

        Arrays.stream(connectorDistDir.listFiles()).forEach(file -> {
            if (file.getName().startsWith("connector-")) {
                Path copied = Paths.get(connectorDir + File.separator + file.getName());
                Path originalPath = file.toPath();
                try {
                    Files.copy(originalPath, copied, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Path targetPluginMappingFile = Paths.get(seatunnelRootDir +
            File.separator +
            "connectors" +
            File.separator +
            "plugin-mapping.properties");

        Path sourcePluginMappingFile = Paths.get(seatunnelRootDir + File.separator + "plugin-mapping.properties");
        try {
            Files.copy(sourcePluginMappingFile, targetPluginMappingFile, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
