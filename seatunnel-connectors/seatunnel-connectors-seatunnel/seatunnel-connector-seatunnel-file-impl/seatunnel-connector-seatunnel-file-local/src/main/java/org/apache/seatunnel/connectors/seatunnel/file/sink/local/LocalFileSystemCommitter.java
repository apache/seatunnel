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

package org.apache.seatunnel.connectors.seatunnel.file.sink.local;

import org.apache.seatunnel.connectors.seatunnel.file.sink.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystemCommitter;

import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class LocalFileSystemCommitter implements FileSystemCommitter {
    @Override
    public void commitTransaction(@NonNull FileAggregatedCommitInfo aggregateCommitInfo) throws IOException {
        for (Map.Entry<String, Map<String, String>> entry : aggregateCommitInfo.getTransactionMap().entrySet()) {
            for (Map.Entry<String, String> mvFileEntry : entry.getValue().entrySet()) {
                FileUtils.renameFile(mvFileEntry.getKey(), mvFileEntry.getValue());
            }
            // delete the transaction dir
            FileUtils.deleteFile(entry.getKey());
        }
    }

    @Override
    public void abortTransaction(@NonNull FileAggregatedCommitInfo aggregateCommitInfo) throws IOException {
        for (Map.Entry<String, Map<String, String>> entry : aggregateCommitInfo.getTransactionMap().entrySet()) {
            // rollback the file
            for (Map.Entry<String, String> mvFileEntry : entry.getValue().entrySet()) {
                File oldFile = new File(mvFileEntry.getKey());
                File newFile = new File(mvFileEntry.getValue());
                if (newFile.exists() && !oldFile.exists()) {
                    FileUtils.renameFile(mvFileEntry.getValue(), mvFileEntry.getKey());
                }
            }
            // delete the transaction dir
            FileUtils.deleteFile(entry.getKey());
        }
    }
}
