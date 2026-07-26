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

package org.apache.seatunnel.connectors.seatunnel.file.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;

/** A closeable, reusable session that emits directory metadata without materializing an array. */
public interface FileStatusListingSession extends Closeable {

    /** Returns metadata for the scan root without enumerating its parent directory. */
    FileStatus getFileStatus(Path path) throws IOException;

    /** Emits direct children of one directory to the consumer as metadata becomes available. */
    void list(Path directory, FileStatusConsumer consumer) throws IOException;

    /** A checked consumer used by protocol callbacks while listing directory entries. */
    @FunctionalInterface
    interface FileStatusConsumer {
        /** Handles one direct child from the active directory listing. */
        void accept(FileStatus status) throws IOException;
    }
}
