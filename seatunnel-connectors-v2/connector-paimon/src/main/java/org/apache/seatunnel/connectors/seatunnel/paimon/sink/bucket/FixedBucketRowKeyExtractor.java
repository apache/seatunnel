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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;
import org.apache.paimon.table.sink.RowKeyExtractor;

public class FixedBucketRowKeyExtractor extends RowKeyExtractor {
    private final int numBuckets;
    private final boolean sameBucketKeyAndTrimmedPrimaryKey;
    private final Projection bucketKeyProjection;
    private BinaryRow reuseBucketKey;
    private Integer reuseBucket;

    public FixedBucketRowKeyExtractor(TableSchema schema) {
        super(schema);
        this.numBuckets = (new CoreOptions(schema.options())).bucket();
        this.sameBucketKeyAndTrimmedPrimaryKey =
                schema.bucketKeys().equals(schema.trimmedPrimaryKeys());
        this.bucketKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.bucketKeys()));
    }

    public void setRecord(InternalRow record) {
        super.setRecord(record);
        this.reuseBucketKey = null;
        this.reuseBucket = null;
    }

    private BinaryRow bucketKey() {
        if (this.sameBucketKeyAndTrimmedPrimaryKey) {
            return this.trimmedPrimaryKey();
        } else {
            if (this.reuseBucketKey == null) {
                this.reuseBucketKey = this.bucketKeyProjection.apply(this.record);
            }
            return this.reuseBucketKey;
        }
    }

    public int bucket() {
        BinaryRow bucketKey = this.bucketKey();
        if (this.reuseBucket == null) {
            this.reuseBucket =
                    KeyAndBucketExtractor.bucket(
                            KeyAndBucketExtractor.bucketKeyHashCode(bucketKey), this.numBuckets);
        }

        return this.reuseBucket;
    }
}
