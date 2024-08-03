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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink;

import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndex;
import org.apache.paimon.index.SimpleHashBucketAssigner;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PaimonTableBloomFilterIndex {

    private final IndexBootstrap indexBootstrap;
    private final RowPartitionKeyExtractor extractor;
    private final RowType indexRowType;
    private final long targetRowNum;
    private final SimpleHashBucketAssigner simpleHashBucketAssigner;
    private final Map<Integer, BloomFilterIndexHolder> bloomFilterFileIndexMap = new HashMap<>();

    public PaimonTableBloomFilterIndex(FileStoreTable table) {
        this.indexBootstrap = new IndexBootstrap(table);
        this.extractor = new RowPartitionKeyExtractor(table.schema());
        this.indexRowType = bootstrapType(table.schema());
        this.targetRowNum = table.coreOptions().dynamicBucketTargetRowNum();
        this.simpleHashBucketAssigner = new SimpleHashBucketAssigner(1, 0, targetRowNum);
        BucketMode bucketMode = table.bucketMode();
        if (bucketMode == BucketMode.DYNAMIC || bucketMode == BucketMode.GLOBAL_DYNAMIC) {
            loadBloomFilterFileIndex();
        }
    }

    private void loadBloomFilterFileIndex() {
        try (RecordReader<InternalRow> recordReader = indexBootstrap.bootstrap(1, 0); ) {
            RecordReaderIterator<InternalRow> readerIterator =
                    new RecordReaderIterator<>(recordReader);
            int fieldIndex = indexRowType.getFieldIndex("_BUCKET");
            while (readerIterator.hasNext()) {
                InternalRow row = readerIterator.next();
                int hashCode = extractor.trimmedPrimaryKey(row).hashCode();
                int bucketKey = row.getInt(fieldIndex);
                BloomFilterFileIndex bloomFilterFileIndex = createBloomFilterFileIndex();
                BloomFilterIndexHolder bloomFilterIndexHolder =
                        bloomFilterFileIndexMap.computeIfAbsent(
                                bucketKey, k -> new BloomFilterIndexHolder(bloomFilterFileIndex));
                FileIndexWriter writer = bloomFilterIndexHolder.getWriter();
                writer.write(hashCode);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private BloomFilterFileIndex createBloomFilterFileIndex() {
        Options options = new Options();
        options.set("items", String.valueOf(targetRowNum));
        options.set("fpp", "0.1");
        return new BloomFilterFileIndex(DataTypes.INT(), options);
    }

    public RowType bootstrapType(TableSchema schema) {
        List<String> primaryKeys = schema.primaryKeys();
        List<String> partitionKeys = schema.partitionKeys();
        List<DataField> bootstrapFields =
                new ArrayList<>(
                        schema.projectedLogicalRowType(
                                        Stream.concat(primaryKeys.stream(), partitionKeys.stream())
                                                .distinct()
                                                .collect(Collectors.toList()))
                                .getFields());
        bootstrapFields.add(
                new DataField(
                        RowType.currentHighestFieldId(bootstrapFields) + 1,
                        "_BUCKET",
                        DataTypes.INT().notNull()));
        return new RowType(bootstrapFields);
    }

    public int obtainDataRowBucket(InternalRow row) {
        int hashCode = extractor.trimmedPrimaryKey(row).hashCode();
        Optional<Integer> bucketOptional =
                bloomFilterFileIndexMap.entrySet().stream()
                        .filter(
                                entry -> {
                                    BloomFilterIndexHolder filterIndexHolder = entry.getValue();
                                    FileIndexReader reader = filterIndexHolder.getReader();
                                    return reader.visitEqual(null, hashCode);
                                })
                        .map(Map.Entry::getKey)
                        .findFirst();
        if (bucketOptional.isPresent()) {
            return bucketOptional.get();
        } else {
            int bucket =
                    Math.abs(simpleHashBucketAssigner.assign(extractor.partition(row), hashCode));
            BloomFilterIndexHolder bloomFilterIndexHolder = bloomFilterFileIndexMap.get(bucket);
            FileIndexWriter writer;
            if (bloomFilterIndexHolder == null) {
                BloomFilterFileIndex bloomFilterFileIndex = createBloomFilterFileIndex();
                writer = bloomFilterFileIndex.createWriter();
                bloomFilterFileIndexMap.put(
                        bucket, new BloomFilterIndexHolder(bloomFilterFileIndex));
            } else {
                writer = bloomFilterIndexHolder.getWriter();
            }
            writer.write(hashCode);
            return bucket;
        }
    }

    static class BloomFilterIndexHolder {
        private final BloomFilterFileIndex bloomFilterFileIndex;
        private final FileIndexWriter writer;

        public BloomFilterIndexHolder(BloomFilterFileIndex bloomFilterFileIndex) {
            this.bloomFilterFileIndex = bloomFilterFileIndex;
            this.writer = bloomFilterFileIndex.createWriter();
        }

        public BloomFilterFileIndex getBloomFilterFileIndex() {
            return bloomFilterFileIndex;
        }

        public FileIndexWriter getWriter() {
            return writer;
        }

        public FileIndexReader getReader() {
            return bloomFilterFileIndex.createReader(this.writer.serializedBytes());
        }
    }
}
