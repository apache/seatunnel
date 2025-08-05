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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/** Batch processing directory scanning tool */
public class DirectoryProcessor {
    private final String inputDir;
    private final String outputDir;

    public DirectoryProcessor(String inputDir, String outputDir) {
        this.inputDir = inputDir;
        this.outputDir = outputDir;
    }

    /**
     * Get all files to be converted, filtered by extension (JSON/XML/TXT)
     *
     * @return list of file paths
     */
    public List<String> listSourceFiles() {
        List<String> result = new ArrayList<>();
        try {
            Files.walk(Paths.get(inputDir))
                    .filter(Files::isRegularFile)
                    .filter(
                            path -> {
                                String ext = FileUtils.getFileExtension(path.toString());
                                return "json".equals(ext) || "xml".equals(ext) || "txt".equals(ext);
                            })
                    .forEach(path -> result.add(path.toString()));
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan directory: " + inputDir, e);
        }
        return result;
    }

    /**
     * Generate the target file path based on the source file path
     *
     * @param sourceFile the path of the source file
     * @return the path of the target file
     */
    public String resolveTargetPath(String sourceFile) {
        String name = FileUtils.getFileNameWithoutExtension(sourceFile);
        return Paths.get(outputDir, name + ".conf").toString();
    }

    /**
     * Generate the report file path based on the source file path
     *
     * @param sourceFile the path of the source file
     * @return the path of the report file
     */
    public String resolveReportPath(String sourceFile) {
        String name = FileUtils.getFileNameWithoutExtension(sourceFile);
        return Paths.get(outputDir, name + ".md").toString();
    }
}
