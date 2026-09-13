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

package org.apache.seatunnel.connectors.seatunnel.lance.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

class FragmentConverterTest {

    @Test
    void shouldMaterializeDatasetFieldsMissingFromRestoredRuntimeSchema() {
        SeaTunnelRowType restoredRowType =
                new SeaTunnelRowType(
                        new String[] {"c_string"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        Schema datasetSchema =
                new Schema(
                        Arrays.asList(
                                Field.nullable("c_string", new ArrowType.Utf8()),
                                Field.notNullable("c_bytes", new ArrowType.Binary())));

        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root =
                        FragmentConverter.convertToVectorSchemaRoot(
                                new SeaTunnelRow(new Object[] {"restored"}),
                                restoredRowType,
                                datasetSchema,
                                allocator)) {
            Assertions.assertEquals(1, root.getRowCount());
            Assertions.assertEquals(
                    "restored",
                    new String(
                            ((VarCharVector) root.getVector("c_string")).get(0),
                            StandardCharsets.UTF_8));
            Assertions.assertTrue(root.getVector("c_bytes").isNull(0));
            Assertions.assertEquals(1, root.getVector("c_bytes").getValueCount());
        }
    }
}
