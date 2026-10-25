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

package org.apache.seatunnel.connectors.seatunnel.easysearch.serialize;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.easysearch.config.EasysearchSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.easysearch.config.KeyEncoding;
import org.apache.seatunnel.connectors.seatunnel.easysearch.dto.IndexInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

class KeyExtractorTest {
    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"first", "second"},
                    new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

    @Test
    void lengthPrefixedIdsSeparateDelimiterCollisions() {
        Function<SeaTunnelRow, String> legacy =
                KeyExtractor.createKeyExtractor(ROW_TYPE, new String[] {"first", "second"}, "_");
        Function<SeaTunnelRow, String> safe =
                KeyExtractor.createKeyExtractor(
                        ROW_TYPE,
                        new String[] {"first", "second"},
                        "_",
                        KeyEncoding.LENGTH_PREFIXED);
        SeaTunnelRow left = new SeaTunnelRow(new Object[] {"1_1", "2"});
        SeaTunnelRow right = new SeaTunnelRow(new Object[] {"1", "1_2"});

        Assertions.assertEquals("1_1_2", legacy.apply(left));
        Assertions.assertEquals(legacy.apply(left), legacy.apply(right));
        Assertions.assertEquals("3:1_11:2", safe.apply(left));
        Assertions.assertEquals("1:13:1_2", safe.apply(right));
    }

    @Test
    void oneKeyKeepsItsExistingIdInBothModes() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"1_1", "2"});
        Assertions.assertEquals(
                "1_1",
                KeyExtractor.createKeyExtractor(
                                ROW_TYPE, new String[] {"first"}, "_", KeyEncoding.LENGTH_PREFIXED)
                        .apply(row));
    }

    @Test
    void serializerUsesTheSameSafeIdForUpsertAndDelete() {
        Map<String, Object> options = new HashMap<>();
        options.put(EasysearchSinkOptions.INDEX.key(), "id_probe");
        options.put(EasysearchSinkOptions.PRIMARY_KEYS.key(), Arrays.asList("first", "second"));
        options.put(EasysearchSinkOptions.KEY_ENCODING.key(), "LENGTH_PREFIXED");
        EasysearchRowSerializer serializer =
                new EasysearchRowSerializer(
                        new IndexInfo(ReadonlyConfig.fromMap(options)), ROW_TYPE);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"1_1", "2"});

        row.setRowKind(RowKind.INSERT);
        Assertions.assertTrue(serializer.serializeRow(row).contains("3:1_11:2"));
        row.setRowKind(RowKind.DELETE);
        Assertions.assertTrue(serializer.serializeRow(row).contains("3:1_11:2"));
    }
}
