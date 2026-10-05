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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.csv.CSVPrinter;

import org.junit.jupiter.api.Test;

import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CopyManagerBatchStatementExecutorTest {

    @Test
    public void testTimeValuePreservesMicroseconds() throws Exception {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("time_col")
                                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                                        .build())
                        .build();
        CopyManagerBatchStatementExecutor executor =
                new CopyManagerBatchStatementExecutor("COPY test", tableSchema);
        executor.csvPrinter = new CSVPrinter(new StringBuilder(), executor.csvFormat);

        executor.addToBatch(new SeaTunnelRow(new Object[] {LocalTime.parse("12:34:56.123456")}));
        executor.csvPrinter.flush();

        assertTrue(executor.csvPrinter.getOut().toString().contains("12:34:56.123456"));
    }
}
