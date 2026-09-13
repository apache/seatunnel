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
package org.apache.seatunnel.transform;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.TextNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.json.JsonToRowConverters;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;
import org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode;
import org.apache.seatunnel.transform.jsonpath.JsonPathTransform;
import org.apache.seatunnel.transform.jsonpath.JsonPathTransformConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class JsonPathErrorHandlingTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "int",
                "date",
                "time",
                "timestamp",
                "decimal(10,2)",
                "bytes",
                "array<int>",
                "map<string,int>"
            })
    void testConversionErrorPolicies(String type) {
        String invalidJson = "{\"amount\":\"!invalid!\",\"description\":\"retained\"}";
        if (type.startsWith("array")) {
            invalidJson = "{\"amount\":[\"!invalid!\"],\"description\":\"retained\"}";
        } else if (type.startsWith("map")) {
            invalidJson = "{\"amount\":{\"amount\":\"!invalid!\"},\"description\":\"retained\"}";
        }
        SeaTunnelRow input = new SeaTunnelRow(new Object[] {invalidJson});
        input.setRowKind(RowKind.UPDATE_AFTER);
        input.setTableId("orders");
        JsonPathTransform skipColumn =
                createTransform(type, ErrorHandleWay.SKIP, ErrorHandleWay.FAIL);
        SeaTunnelRow output = skipColumn.map(input);
        Assertions.assertNotNull(output);
        Assertions.assertNull(output.getField(1));
        Assertions.assertEquals("retained", output.getField(2));
        Assertions.assertEquals(invalidJson, input.getField(0));
        Assertions.assertEquals(input.getRowKind(), output.getRowKind());
        Assertions.assertEquals(input.getTableId(), output.getTableId());

        Assertions.assertNull(
                createTransform(type, ErrorHandleWay.SKIP_ROW, ErrorHandleWay.FAIL).map(input));
        Assertions.assertNull(createTransform(type, null, ErrorHandleWay.SKIP).map(input));

        JsonPathTransform failColumn =
                createTransform(type, ErrorHandleWay.FAIL, ErrorHandleWay.SKIP);
        ErrorDataTransformException failure =
                Assertions.assertThrows(
                        ErrorDataTransformException.class, () -> failColumn.map(input));
        Assertions.assertEquals(ErrorHandleWay.FAIL, failure.getErrorHandleWay());
        Assertions.assertEquals(
                JsonPathTransformErrorCode.JSON_PATH_CONVERSION_ERROR,
                failure.getSeaTunnelErrorCode());
        Assertions.assertNotNull(failure.getCause());

        JsonPathTransform defaultPolicy = createTransform(type, null, null);
        Assertions.assertThrows(ErrorDataTransformException.class, () -> defaultPolicy.map(input));
    }

    @Test
    void testDateConversionAfterValidRow() {
        JsonPathTransform transform = createTransform("date", ErrorHandleWay.SKIP, null);
        Assertions.assertEquals(
                LocalDate.of(2026, 9, 9), transform.map(row("\"2026-09-09\"")).getField(1));
        Assertions.assertNull(transform.map(row("\"invalid\"")).getField(1));
        Assertions.assertEquals(
                LocalDate.of(2026, 9, 10), transform.map(row("\"2026-09-10\"")).getField(1));
    }

    @Test
    void testNullAndValidValues() {
        JsonPathTransform transform = createTransform("int", null, null);
        Assertions.assertEquals(42, transform.map(row("42")).getField(1));
        Assertions.assertNull(transform.map(row("null")).getField(1));
        SeaTunnelRow nullSource = transform.map(new SeaTunnelRow(new Object[] {null}));
        Assertions.assertNull(nullSource.getField(1));
        Assertions.assertNull(nullSource.getField(2));
    }

    @Test
    void testMissingPathAndInvalidJsonPolicies() {
        JsonPathTransform skipColumn = createTransform("int", ErrorHandleWay.SKIP, null);
        Assertions.assertNull(skipColumn.map(new SeaTunnelRow(new Object[] {"{}"})).getField(1));
        JsonPathTransform skipRow = createTransform("int", null, ErrorHandleWay.SKIP);
        Assertions.assertNull(skipRow.map(new SeaTunnelRow(new Object[] {"{invalid"})));
    }

    @Test
    void testSourceFailureIsNotSkipped() {
        IllegalStateException sourceFailure = new IllegalStateException("Source value unavailable");
        Object sourceValue =
                new Object() {
                    @Override
                    public String toString() {
                        throw sourceFailure;
                    }
                };
        JsonPathTransform transform =
                createTransform("int", ErrorHandleWay.SKIP, ErrorHandleWay.SKIP);
        Assertions.assertSame(
                sourceFailure,
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () -> transform.map(new SeaTunnelRow(new Object[] {sourceValue}))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"SKIP", "SKIP_ROW", "ROW_SKIP"})
    void testWrappedErrorsAreNotSkipped(String policy) throws Exception {
        for (Error fatal :
                new Error[] {
                    new OutOfMemoryError("synthetic failure"),
                    new StackOverflowError("synthetic failure"),
                    new LinkageError("synthetic failure")
                }) {
            JsonPathTransform transform = createSkippingTransform(policy);
            ObjectNode object = JsonNodeFactory.instance.objectNode();
            object.set(
                    "amount",
                    new TextNode("42") {
                        @Override
                        public String asText() {
                            throw fatal;
                        }
                    });
            JsonToRowConverters.JsonToObjectConverter rowConverter =
                    new JsonToRowConverters(false, false)
                            .createRowConverter(
                                    new SeaTunnelRowType(
                                            new String[] {"amount"},
                                            new SeaTunnelDataType[] {BasicType.INT_TYPE}));
            // Exercise the real row converter's Throwable wrapper without exhausting resources.
            setConverter(transform, (node, field) -> rowConverter.convert(object, field));
            Assertions.assertSame(
                    fatal, Assertions.assertThrows(Error.class, () -> transform.map(row("42"))));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"SKIP", "SKIP_ROW", "ROW_SKIP"})
    void testWrappedDataErrorsStillUsePolicy(String policy) throws Exception {
        JsonPathTransform transform = createSkippingTransform(policy);
        JsonToRowConverters.JsonToObjectConverter rowConverter =
                new JsonToRowConverters(false, false)
                        .createRowConverter(
                                new SeaTunnelRowType(
                                        new String[] {"amount"},
                                        new SeaTunnelDataType[] {BasicType.INT_TYPE}));
        ObjectNode object = JsonNodeFactory.instance.objectNode();
        object.put("amount", "invalid");
        setConverter(transform, (node, field) -> rowConverter.convert(object, field));
        SeaTunnelRow output = transform.map(row("42"));
        if ("SKIP".equals(policy)) {
            Assertions.assertNotNull(output);
            Assertions.assertNull(output.getField(1));
            Assertions.assertEquals("retained", output.getField(2));
        } else {
            Assertions.assertNull(output);
        }
    }

    @Test
    void testCyclicConversionCauseDoesNotLoop() throws Exception {
        RuntimeException outer = new IllegalArgumentException("outer");
        RuntimeException inner = new IllegalArgumentException("inner", outer);
        outer.initCause(inner);
        JsonPathTransform transform = createSkippingTransform("SKIP");
        setConverter(
                transform,
                (node, field) -> {
                    throw outer;
                });
        Assertions.assertNull(transform.map(row("42")).getField(1));
    }

    private static JsonPathTransform createSkippingTransform(String policy) {
        return createTransform(
                "int",
                "ROW_SKIP".equals(policy) ? null : ErrorHandleWay.valueOf(policy),
                ErrorHandleWay.SKIP);
    }

    private static void setConverter(
            JsonPathTransform transform, JsonToRowConverters.JsonToObjectConverter converter)
            throws Exception {
        Field field = JsonPathTransform.class.getDeclaredField("converters");
        field.setAccessible(true);
        ((JsonToRowConverters.JsonToObjectConverter[]) field.get(transform))[0] = converter;
    }

    private static SeaTunnelRow row(String value) {
        return new SeaTunnelRow(
                new Object[] {"{\"amount\":" + value + ",\"description\":\"retained\"}"});
    }

    private static JsonPathTransform createTransform(
            String type, ErrorHandleWay columnPolicy, ErrorHandleWay rowPolicy) {
        Map<String, Object> column = new HashMap<>();
        column.put("src_field", "content");
        column.put("path", "$.amount");
        column.put("dest_field", "amount");
        column.put("dest_type", type);
        if (columnPolicy != null) {
            column.put("column_error_handle_way", columnPolicy.name());
        }
        Map<String, Object> description = new HashMap<>();
        description.put("src_field", "content");
        description.put("path", "$.description");
        description.put("dest_field", "description");
        description.put("column_error_handle_way", "SKIP");
        Map<String, Object> options = new HashMap<>();
        options.put("columns", Arrays.asList(column, description));
        if (rowPolicy != null) {
            options.put("row_error_handle_way", rowPolicy.name());
        }
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        "orders",
                        new SeaTunnelRowType(
                                new String[] {"content"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}));
        JsonPathTransform transform =
                new JsonPathTransform(
                        JsonPathTransformConfig.of(ReadonlyConfig.fromMap(options), table), table);
        transform.getProducedCatalogTable();
        return transform;
    }
}
