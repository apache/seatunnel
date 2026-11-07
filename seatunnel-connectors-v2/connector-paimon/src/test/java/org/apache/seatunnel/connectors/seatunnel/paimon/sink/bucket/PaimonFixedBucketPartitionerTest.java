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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

class PaimonFixedBucketPartitionerTest {

    @Test
    void testSamePrimaryKeyUsesSameWriterForAllRowKinds() {
        PaimonFixedBucketPartitioner partitioner =
                new PaimonFixedBucketPartitioner(sourceRowType(), sinkTableSchema(), 2);

        SeaTunnelRow insert = new SeaTunnelRow(new Object[] {99, "before"});
        SeaTunnelRow delete = new SeaTunnelRow(new Object[] {99, "before"});
        SeaTunnelRow update = new SeaTunnelRow(new Object[] {99, "after"});
        delete.setRowKind(RowKind.DELETE);
        update.setRowKind(RowKind.UPDATE_AFTER);

        int insertWriter = partitioner.select(insert);
        Assertions.assertEquals(insertWriter, partitioner.select(delete));
        Assertions.assertEquals(insertWriter, partitioner.select(update));
        Assertions.assertTrue(insertWriter >= 0 && insertWriter < 2);
    }

    private SeaTunnelRowType sourceRowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "value"},
                new org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] {
                    BasicType.INT_TYPE, BasicType.STRING_TYPE
                });
    }

    private TableSchema sinkTableSchema() {
        RowType rowType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.INT(), DataTypes.STRING()
                        },
                        new String[] {"id", "value"});
        return new TableSchema(
                0,
                TableSchema.newFields(rowType),
                rowType.getFieldCount(),
                Collections.emptyList(),
                Arrays.asList("id"),
                Collections.singletonMap("bucket", "2"),
                "");
    }
}
