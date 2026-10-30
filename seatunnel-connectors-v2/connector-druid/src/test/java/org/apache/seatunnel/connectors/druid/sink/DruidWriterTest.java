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

package org.apache.seatunnel.connectors.druid.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class DruidWriterTest {

    @Test
    public void testWriteRowWithNullFieldPreservesColumnPositions() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "amount"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.LONG_TYPE
                        });
        DruidWriter writer = new DruidWriter(rowType, "localhost:8888", "test_datasource", 100);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {2, null, 20L});
        writer.write(row);

        String written = writer.getData().toString();
        Assertions.assertTrue(
                written.startsWith("2,,20,"),
                "Expected empty column for null field between delimiters, got: " + written);
    }
}
