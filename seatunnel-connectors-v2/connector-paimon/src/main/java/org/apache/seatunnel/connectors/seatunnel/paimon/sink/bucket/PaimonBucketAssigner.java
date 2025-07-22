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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

public class PaimonBucketAssigner {

    private boolean isRunning;

    private final FixedBucketRowKeyExtractor extractor;

    private final HashBucketAssigner hashBucketAssigner;

    public PaimonBucketAssigner(Table table, int numAssigners, int assignId) {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        this.extractor = new FixedBucketRowKeyExtractor(fileStoreTable.schema());
        long dynamicBucketTargetRowNum = fileStoreTable.coreOptions().dynamicBucketTargetRowNum();
        this.hashBucketAssigner =
                new HashBucketAssigner(
                        fileStoreTable.snapshotManager(),
                        "hash-bucket",
                        fileStoreTable.store().newIndexFileHandler(),
                        numAssigners,
                        numAssigners,
                        assignId,
                        dynamicBucketTargetRowNum);
        this.isRunning = true;
    }

    public int assign(InternalRow rowData) {
        extractor.setRecord(rowData);
        return hashBucketAssigner.assign(
                extractor.partition(), extractor.trimmedPrimaryKey().hashCode());
    }

    public void prepareCommit(long commitIdentifier) {
        hashBucketAssigner.prepareCommit(commitIdentifier);
    }

    public void finish() {
        this.isRunning = false;
    }

    public boolean isRunning() {
        return isRunning;
    }
}
