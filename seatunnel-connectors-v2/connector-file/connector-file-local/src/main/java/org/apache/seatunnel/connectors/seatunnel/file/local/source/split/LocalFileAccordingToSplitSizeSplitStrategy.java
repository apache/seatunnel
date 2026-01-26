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
package org.apache.seatunnel.connectors.seatunnel.file.local.source.split;

import org.apache.seatunnel.connectors.seatunnel.file.source.split.AccordingToSplitSizeSplitStrategy;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LocalFileAccordingToSplitSizeSplitStrategy extends AccordingToSplitSizeSplitStrategy {

    public LocalFileAccordingToSplitSizeSplitStrategy(
            String rowDelimiter, long skipHeaderRowNumber, String encodingName, long splitSize) {
        super(rowDelimiter, skipHeaderRowNumber, encodingName, splitSize);
    }

    @Override
    protected InputStream getInputStream(String filePath) throws IOException {
        Path path = toLocalNioPath(filePath);
        return new BufferedInputStream(Files.newInputStream(path));
    }

    @Override
    protected long getFileSize(String filePath) throws IOException {
        Path path = toLocalNioPath(filePath);
        return Files.size(path);
    }

    private static Path toLocalNioPath(String filePath) {
        try {
            URI uri = URI.create(filePath);
            if ("file".equalsIgnoreCase(uri.getScheme())) {
                return Paths.get(uri);
            }
        } catch (Exception ignored) {
            // ignore malformed URI
        }
        return Paths.get(filePath);
    }
}
