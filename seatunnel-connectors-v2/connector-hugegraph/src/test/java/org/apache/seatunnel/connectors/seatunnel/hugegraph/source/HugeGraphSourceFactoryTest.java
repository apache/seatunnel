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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.source;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class HugeGraphSourceFactoryTest {

    @Test
    void testVertexReservedFields() {
        SeaTunnelRowType rowType =
                HugeGraphSourceFactory.prependReservedFields(
                        propertyRowType(), MappingConfig.LabelType.VERTEX);

        assertArrayEquals(new String[] {"~id", "~label", "name", "age"}, rowType.getFieldNames());
    }

    @Test
    void testEdgeReservedFields() {
        SeaTunnelRowType rowType =
                HugeGraphSourceFactory.prependReservedFields(
                        propertyRowType(), MappingConfig.LabelType.EDGE);

        assertArrayEquals(
                new String[] {
                    "~id",
                    "~label",
                    "~source_id",
                    "~source_label",
                    "~target_id",
                    "~target_label",
                    "name",
                    "age"
                },
                rowType.getFieldNames());
    }

    private SeaTunnelRowType propertyRowType() {
        return new SeaTunnelRowType(
                new String[] {"name", "age"},
                new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
    }
}
