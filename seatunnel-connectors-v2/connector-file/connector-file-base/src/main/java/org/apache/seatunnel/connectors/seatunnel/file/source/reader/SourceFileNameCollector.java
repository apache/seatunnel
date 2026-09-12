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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

/** Adds the source file identity to every row emitted for a file split. */
public final class SourceFileNameCollector implements Collector<SeaTunnelRow> {

    public static final String SOURCE_FILE_NAME = "source_file_name";
    public static final String SOURCE_FILE_ID = "source_file_id";

    private final Collector<SeaTunnelRow> delegate;
    private final String sourceFileName;
    private final String sourceFileId;

    private SourceFileNameCollector(Collector<SeaTunnelRow> delegate, String sourceFilePath) {
        this.delegate = delegate;
        this.sourceFileName = extractFileName(sourceFilePath);
        this.sourceFileId =
                UUID.nameUUIDFromBytes(sourceFilePath.getBytes(StandardCharsets.UTF_8)).toString();
    }

    public static Collector<SeaTunnelRow> wrap(
            Collector<SeaTunnelRow> delegate, String sourceFilePath) {
        return new SourceFileNameCollector(delegate, sourceFilePath);
    }

    static String extractFileName(String sourceFilePath) {
        if (sourceFilePath == null || sourceFilePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Source file path must not be empty");
        }
        String normalizedPath = sourceFilePath.replace('\\', '/');
        if (normalizedPath.contains("://")) {
            try {
                String uriPath = new URI(normalizedPath).getPath();
                if (uriPath != null) {
                    normalizedPath = uriPath;
                }
            } catch (URISyntaxException ignored) {
                // Keep the original path and let the underlying reader report malformed URIs.
            }
        }
        int separator = normalizedPath.lastIndexOf('/');
        String fileName = normalizedPath.substring(separator + 1);
        if (fileName.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot determine a file name from source path: " + sourceFilePath);
        }
        return fileName;
    }

    @Override
    public void collect(SeaTunnelRow record) {
        record.getOptions().put(SOURCE_FILE_NAME, sourceFileName);
        record.getOptions().put(SOURCE_FILE_ID, sourceFileId);
        delegate.collect(record);
    }

    @Override
    public void markSchemaChangeBeforeCheckpoint() {
        delegate.markSchemaChangeBeforeCheckpoint();
    }

    @Override
    public void collect(SchemaChangeEvent event) {
        delegate.collect(event);
    }

    @Override
    public void markSchemaChangeAfterCheckpoint() {
        delegate.markSchemaChangeAfterCheckpoint();
    }

    @Override
    public Object getCheckpointLock() {
        return delegate.getCheckpointLock();
    }

    @Override
    public boolean isEmptyThisPollNext() {
        return delegate.isEmptyThisPollNext();
    }

    @Override
    public void resetEmptyThisPollNext() {
        delegate.resetEmptyThisPollNext();
    }
}
