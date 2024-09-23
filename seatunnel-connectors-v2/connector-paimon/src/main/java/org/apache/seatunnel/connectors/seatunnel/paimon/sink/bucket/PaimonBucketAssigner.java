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

import org.apache.commons.collections.CollectionUtils;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.SimpleHashBucketAssigner;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;

import java.io.IOException;

public class PaimonBucketAssigner {

    private final RowPartitionKeyExtractor extractor;

    private final Projection bucketKeyProjection;

    private final SimpleHashBucketAssigner simpleHashBucketAssigner;

    private final TableSchema schema;

    public PaimonBucketAssigner(Table table, int numAssigners, int assignId) {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        this.schema = fileStoreTable.schema();
        this.extractor = new RowPartitionKeyExtractor(fileStoreTable.schema());
        this.bucketKeyProjection =
                CodeGenUtils.newProjection(
                        fileStoreTable.schema().logicalRowType(),
                        fileStoreTable.schema().projection(fileStoreTable.schema().bucketKeys()));
        long dynamicBucketTargetRowNum =
                ((FileStoreTable) table).coreOptions().dynamicBucketTargetRowNum();
        this.simpleHashBucketAssigner =
                new SimpleHashBucketAssigner(numAssigners, assignId, dynamicBucketTargetRowNum);
        loadBucketIndex(fileStoreTable, numAssigners, assignId);
    }

    private void loadBucketIndex(FileStoreTable fileStoreTable, int numAssigners, int assignId) {
        IndexBootstrap indexBootstrap = new IndexBootstrap(fileStoreTable);
        try (RecordReader<InternalRow> recordReader =
                indexBootstrap.bootstrap(numAssigners, assignId)) {
            RecordReaderIterator<InternalRow> readerIterator =
                    new RecordReaderIterator<>(recordReader);
            while (readerIterator.hasNext()) {
                InternalRow row = readerIterator.next();
                assign(row);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int assign(InternalRow rowData) {
        int hash;
        if (CollectionUtils.isEmpty(this.schema.bucketKeys())) {
            hash = extractor.trimmedPrimaryKey(rowData).hashCode();
        } else {
            hash = bucketKeyProjection.apply(rowData).hashCode();
        }
        return Math.abs(
                this.simpleHashBucketAssigner.assign(this.extractor.partition(rowData), hash));
    }
}
