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

package org.apache.seatunnel.connectors.seatunnel.file.source;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

/** Resolves the stable identity used to follow a local file across path changes. */
public final class LocalFileIdentity {

    private LocalFileIdentity() {}

    /**
     * Returns an identity that remains stable when the file is renamed.
     *
     * <p>File keys are preferred because they identify the underlying filesystem object directly.
     * Some providers, including older Windows JDK providers, do not expose a file key. For those
     * providers the file creation time is the only rename-stable identity available through the
     * standard Java file API.
     */
    public static String read(String filePath) throws IOException {
        BasicFileAttributes attributes =
                Files.readAttributes(toNioPath(filePath), BasicFileAttributes.class);
        Object fileKey = attributes.fileKey();
        if (fileKey != null) {
            return "file-key:" + fileKey;
        }
        FileTime creationTime = attributes.creationTime();
        if (creationTime == null || creationTime.toMillis() <= 0L) {
            throw new IOException("Local filesystem does not expose a stable file identity");
        }
        return "creation-time:" + creationTime;
    }

    private static java.nio.file.Path toNioPath(String filePath) {
        URI uri = new Path(filePath).toUri();
        if (uri.getScheme() == null) {
            return Paths.get(filePath);
        }
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("Not a local file path: " + filePath);
        }
        return Paths.get(uri);
    }
}
