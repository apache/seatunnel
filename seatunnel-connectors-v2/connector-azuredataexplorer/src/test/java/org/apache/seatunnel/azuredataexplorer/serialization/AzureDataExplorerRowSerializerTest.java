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

package org.apache.seatunnel.azuredataexplorer.serialization;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies that ADX CSV rows preserve embedded quotes and field boundaries. */
class AzureDataExplorerRowSerializerTest {

    @Test
    void testCsvRowEscapesQuotesAndDelimiters() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"quoted", "comma", "newline", "plain", "null"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE
                        });
        AzureDataExplorerRowSerializer serializer = new AzureDataExplorerRowSerializer(rowType);

        assertEquals(
                "\"a\"\"b\",\"a,b\",\"a\r\nb\",plain,\n",
                serializer.toCsvLine(
                        new SeaTunnelRow(new Object[] {"a\"b", "a,b", "a\r\nb", "plain", null})));
    }
}
