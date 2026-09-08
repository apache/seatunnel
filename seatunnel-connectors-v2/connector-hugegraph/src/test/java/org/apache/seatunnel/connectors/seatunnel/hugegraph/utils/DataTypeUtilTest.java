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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.utils;

import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.ListFormat;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.schema.PropertyKey;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DataTypeUtilTest {

    @Test
    void testConvertSeaTunnelArrayToHugeGraphList() {
        PropertyKey propertyKey = propertyKey("scores", DataType.INT, Cardinality.LIST);

        Object converted = DataTypeUtil.convert(new Integer[] {1, 2, 3}, propertyKey, null, null);

        assertEquals(Arrays.asList(1, 2, 3), converted);
    }

    @Test
    void testConvertPrimitiveArrayToHugeGraphSet() {
        PropertyKey propertyKey = propertyKey("scores", DataType.LONG, Cardinality.SET);

        Object converted = DataTypeUtil.convert(new long[] {1L, 2L, 2L}, propertyKey, null, null);

        assertEquals(2, ((Collection<?>) converted).size());
    }

    @Test
    void testConvertDateOnlyStringWithDocumentedDefaults() {
        PropertyKey propertyKey = propertyKey("created_at", DataType.DATE, Cardinality.SINGLE);

        Object converted = DataTypeUtil.convert("2026-07-11", propertyKey, "yyyy-MM-dd", "GMT+8");

        assertEquals(Date.from(java.time.Instant.parse("2026-07-10T16:00:00Z")), converted);
    }

    @Test
    void testExtraDateFormatsAreTriedInOrder() {
        PropertyKey propertyKey = propertyKey("created_at", DataType.DATE, Cardinality.SINGLE);

        // The primary format does not match "2026/07/11"; the extra "yyyy/MM/dd" does.
        Object converted =
                DataTypeUtil.convert(
                        "2026/07/11",
                        propertyKey,
                        "yyyy-MM-dd",
                        "GMT+8",
                        Arrays.asList("yyyy/MM/dd"),
                        new ListFormat());

        assertEquals(Date.from(java.time.Instant.parse("2026-07-10T16:00:00Z")), converted);
    }

    @Test
    void testCustomListFormatDelimiterAndNoBrackets() {
        PropertyKey propertyKey = propertyKey("tags", DataType.TEXT, Cardinality.LIST);
        ListFormat listFormat = new ListFormat();
        listFormat.setStartSymbol("");
        listFormat.setEndSymbol("");
        listFormat.setElemDelimiter("|");

        Object converted = DataTypeUtil.convert("a|b|c", propertyKey, null, null, listFormat);

        assertEquals(Arrays.asList("a", "b", "c"), converted);
    }

    @Test
    void testListFormatIgnoredElems() {
        PropertyKey propertyKey = propertyKey("tags", DataType.TEXT, Cardinality.LIST);
        ListFormat listFormat = new ListFormat();
        listFormat.setIgnoredElems(Collections.singletonList("NULL"));

        // Default start/end "[" "]" are stripped; the "NULL" element is dropped.
        Object converted = DataTypeUtil.convert("[a,NULL,b]", propertyKey, null, null, listFormat);

        assertEquals(Arrays.asList("a", "b"), converted);
    }

    private static PropertyKey propertyKey(
            String name, DataType dataType, Cardinality cardinality) {
        PropertyKey propertyKey = mock(PropertyKey.class);
        when(propertyKey.name()).thenReturn(name);
        when(propertyKey.dataType()).thenReturn(dataType);
        when(propertyKey.cardinality()).thenReturn(cardinality);
        return propertyKey;
    }
}
