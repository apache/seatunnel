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

package org.apache.seatunnel.connectors.seatunnel.google.sheets.deserialize;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GoogleSheetsDeserializerTest {
    @Test
    public void testJsonParseError() {
        SeaTunnelRowType schema =
                new SeaTunnelRowType(new String[] {"name"}, new SeaTunnelDataType[] {STRING_TYPE});

        CatalogTable catalogTables = CatalogTableUtil.getCatalogTable("", "", "", "test", schema);

        final DeserializationSchema<SeaTunnelRow> deser =
                new JsonDeserializationSchema(catalogTables, false, false);
        final GoogleSheetsDeserializer googleSheetsDeser =
                new GoogleSheetsDeserializer(schema.getFieldNames(), deser);
        List<Object> row = new ArrayList<>();
        Object mockObj = new Object();
        row.add(mockObj);

        String expectedPayload = String.format("{name=%s}", mockObj.toString());
        SeaTunnelRuntimeException expected =
                CommonError.jsonOperationError("GoogleSheets", expectedPayload);

        SeaTunnelRuntimeException actual =
                assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> {
                            googleSheetsDeser.deserializeRow(row);
                        },
                        "expecting exception message: " + expected.getMessage());

        assertEquals(expected.getMessage(), actual.getMessage());
    }
}
