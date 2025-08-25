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

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.shade.com.google.common.hash.Hashing;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.SQLEngineFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/** Test for murmur64 function */
@Slf4j
public class Murmur64Test {

    /** Test MURMUR64 function through SQL engine integration */
    @Test
    public void testMurmur64ThroughSQLEngine() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"str_v1", "str_v2", "str_v3", "str_v4", "str_v5"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE
                        });

        SeaTunnelRow inputRow =
                new SeaTunnelRow(new Object[] {"hello world", "", "test123", "unicode_test", null});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select MURMUR64(str_v1) as hash_v1, MURMUR64(str_v2) as hash_v2, MURMUR64(str_v3) as hash_v3, MURMUR64(str_v4) as hash_v4, MURMUR64(str_v5) as hash_v5 from test");

        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);

        // Verify results match direct implementation
        Assertions.assertEquals(murmur64Direct("hello world"), outRow.getField(0));
        Assertions.assertEquals(murmur64Direct(""), outRow.getField(1));
        Assertions.assertEquals(murmur64Direct("test123"), outRow.getField(2));
        Assertions.assertEquals(murmur64Direct("unicode_test"), outRow.getField(3));
        Assertions.assertEquals(murmur64Direct(null), outRow.getField(4));
    }

    /**
     * Direct implementation of murmur64 logic for testing This avoids loading the StringFunction
     * class which might cause dependency conflicts
     */
    private static Long murmur64Direct(String input) {
        if (input == null) {
            return null;
        }
        return Hashing.murmur3_128().hashString(input, StandardCharsets.UTF_8).asLong();
    }
}
