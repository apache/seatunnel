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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit;

import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.WriteResult;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Slf4j
public class IcebergFilesCommitter implements Serializable {
    private IcebergTableLoader icebergTableLoader;
    private boolean caseSensitive;
    private String branch;

    private IcebergFilesCommitter(SinkConfig config, IcebergTableLoader icebergTableLoader) {
        this.icebergTableLoader = icebergTableLoader;
        this.caseSensitive = config.isCaseSensitive();
        this.branch = config.getCommitBranch();
    }

    public static IcebergFilesCommitter of(
            SinkConfig config, IcebergTableLoader icebergTableLoader) {
        return new IcebergFilesCommitter(config, icebergTableLoader);
    }

    public void doCommit(List<WriteResult> results) {
        TableIdentifier tableIdentifier = icebergTableLoader.getTableIdentifier();
        Table table = icebergTableLoader.loadTable();
        log.info("do commit table : " + table.toString());
        commit(tableIdentifier, table, results);
    }

    private void commit(TableIdentifier tableIdentifier, Table table, List<WriteResult> results) {
        List<DataFile> dataFiles =
                results.stream()
                        .filter(payload -> payload.getDataFiles() != null)
                        .flatMap(payload -> payload.getDataFiles().stream())
                        .filter(dataFile -> dataFile.recordCount() > 0)
                        .collect(toList());

        List<DeleteFile> deleteFiles =
                results.stream()
                        .filter(payload -> payload.getDeleteFiles() != null)
                        .flatMap(payload -> payload.getDeleteFiles().stream())
                        .filter(deleteFile -> deleteFile.recordCount() > 0)
                        .collect(toList());

        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            log.info(String.format("Nothing to commit to table %s, skipping", tableIdentifier));
        } else {
            if (deleteFiles.isEmpty()) {
                AppendFiles append = table.newAppend();
                if (branch != null) {
                    append.toBranch(branch);
                }
                dataFiles.forEach(append::appendFile);
                append.commit();
            } else {
                RowDelta delta = table.newRowDelta();
                if (branch != null) {
                    delta.toBranch(branch);
                }
                delta.caseSensitive(caseSensitive);
                dataFiles.forEach(delta::addRows);
                deleteFiles.forEach(delta::addDeletes);
                delta.commit();
            }
        }
    }
}
