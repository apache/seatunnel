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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

public class TestUtils {
    public static String getResource(String confFile) {
        return System.getProperty("user.dir") + "/src/test/resources" + confFile;
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
