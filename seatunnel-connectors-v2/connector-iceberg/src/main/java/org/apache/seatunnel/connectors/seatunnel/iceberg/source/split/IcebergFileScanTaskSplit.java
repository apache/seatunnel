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

package org.apache.seatunnel.connectors.seatunnel.iceberg.source.split;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;

import org.apache.iceberg.FileScanTask;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@AllArgsConstructor
public class IcebergFileScanTaskSplit implements SourceSplit {

    private static final long serialVersionUID = -9043797960947110643L;

    private final TablePath tablePath;
    private final FileScanTask task;
    @Setter private volatile long recordOffset;

    public IcebergFileScanTaskSplit(TablePath tablePath, @NonNull FileScanTask task) {
        this(tablePath, task, 0);
    }

    // TODO: Waiting for old version migration to complete before remove
    @Deprecated
    public IcebergFileScanTaskSplit(@NonNull FileScanTask task) {
        this(null, task, 0);
    }

    @Override
    public String splitId() {
        return task.file().path().toString();
    }

    @Override
    public String toString() {
        return "IcebergFileScanTaskSplit{"
                + "task="
                + toString(task)
                + ", recordOffset="
                + recordOffset
                + '}';
    }

    private String toString(FileScanTask task) {
        Map<String, Object> taskInfo = new HashMap<>();
        taskInfo.put("file", task.file().path().toString());
        taskInfo.put("start", task.start());
        taskInfo.put("length", task.length());
        taskInfo.put(
                "deletes",
                task.deletes().stream()
                        .map(deleteFile -> deleteFile.path())
                        .collect(Collectors.toList()));
        return taskInfo.toString();
    }
}
