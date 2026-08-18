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

/** Resolves the stable identity used to follow a local file across path changes. */
public final class LocalFileIdentity {

    private LocalFileIdentity() {}

    /**
     * Returns an identity that remains stable when the file is renamed.
     *
     * <p>A file key is required because it identifies the underlying filesystem object directly.
     * Creation time is not a safe fallback: some providers preserve it when a different file
     * replaces the path.
     */
    public static String read(String filePath) throws IOException {
        BasicFileAttributes attributes =
                Files.readAttributes(toNioPath(filePath), BasicFileAttributes.class);
        return fromAttributes(filePath, attributes);
    }

    static String fromAttributes(String filePath, BasicFileAttributes attributes)
            throws IOException {
        Object fileKey = attributes.fileKey();
        if (fileKey == null) {
            throw new IOException(
                    "Local filesystem does not expose BasicFileAttributes.fileKey() for "
                            + filePath);
        }
        return "file-key:" + fileKey;
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
