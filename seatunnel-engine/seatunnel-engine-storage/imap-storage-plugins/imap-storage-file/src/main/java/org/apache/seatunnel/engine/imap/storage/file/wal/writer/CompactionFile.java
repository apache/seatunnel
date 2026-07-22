/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.wal.writer;

import org.apache.hadoop.fs.Path;

import lombok.Getter;

import java.util.Objects;

@Getter
public class CompactionFile implements Comparable<CompactionFile> {
    private final Path path;
    private final long size;

    public CompactionFile(Path path, long size) {
        this.path = path;
        this.size = size;
    }

    @Override
    public int compareTo(CompactionFile o) {
        int cmp = Long.compare(this.size, o.size);
        if (cmp != 0) {
            return cmp;
        }
        return this.path.toString().compareTo(o.path.toString());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        CompactionFile that = (CompactionFile) object;
        return this.size == that.size && Objects.equals(this.path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, path);
    }
}
