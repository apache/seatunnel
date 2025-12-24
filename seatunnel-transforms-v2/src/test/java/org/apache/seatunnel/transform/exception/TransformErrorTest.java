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

package org.apache.seatunnel.transform.exception;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.copy.CopyFieldTransformFactory;
import org.apache.seatunnel.transform.copy.CopyTransformConfig;
import org.apache.seatunnel.transform.fieldmapper.FieldMapperTransformConfig;
import org.apache.seatunnel.transform.fieldmapper.FieldMapperTransformFactory;
import org.apache.seatunnel.transform.filter.FilterFieldTransformConfig;
import org.apache.seatunnel.transform.filter.FilterFieldTransformFactory;
import org.apache.seatunnel.transform.jsonpath.JsonPathTransformConfig;
import org.apache.seatunnel.transform.jsonpath.JsonPathTransformFactory;
import org.apache.seatunnel.transform.replace.ReplaceTransformConfig;
import org.apache.seatunnel.transform.replace.ReplaceTransformFactory;
import org.apache.seatunnel.transform.split.SplitTransformConfig;
import org.apache.seatunnel.transform.split.SplitTransformFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TransformErrorTest {

    private static final CatalogTable table =
            CatalogTableUtil.getCatalogTable(
                    "test",
                    "test",
                    "test",
                    "test",
                    new SeaTunnelRowType(
                            new String[] {"name"},
                            new SeaTunnelDataType[] {BasicType.STRING_TYPE}));

    @Test
    void testFieldMapperTransformWithError() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(
                                        FieldMapperTransformConfig.FIELD_MAPPER.key(),
                                        new HashMap<String, String>() {
                                            {
                                                put("age", "age1");
                                            }
                                        });
                            }
                        });
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Collections.singletonList(table),
                        config,
                        Thread.currentThread().getContextClassLoader());
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new FieldMapperTransformFactory()
                                        .createTransform(context)
                                        .createTransform());
        Assertions.assertEquals(
                "ErrorCode:[TRANSFORM_COMMON-02], ErrorDescription:[The input fields 'age' of 'FieldMapper' transform not found in upstream schema]",
                exception.getMessage());
    }

    @Test
    void testCopyTransformWithError() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(
                                        CopyTransformConfig.FIELDS.key(),
                                        new HashMap<String, String>() {
                                            {
                                                put("ageA", "age1");
                                            }
                                        });
                            }
                        });
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Collections.singletonList(table),
                        config,
                        Thread.currentThread().getContextClassLoader());
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new CopyFieldTransformFactory()
                                        .createTransform(context)
                                        .createTransform());
        Assertions.assertEquals(
                "ErrorCode:[TRANSFORM_COMMON-01], ErrorDescription:[The input field 'age1' of 'Copy' transform not found in upstream schema]",
                exception.getMessage());

        ReadonlyConfig config2 =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(CopyTransformConfig.SRC_FIELD.key(), "ageB");
                                put(CopyTransformConfig.DEST_FIELD.key(), "age1");
                            }
                        });
        TableTransformFactoryContext context2 =
                new TableTransformFactoryContext(
                        Collections.singletonList(table),
                        config2,
                        Thread.currentThread().getContextClassLoader());
        TransformException exception2 =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new CopyFieldTransformFactory()
                                        .createTransform(context2)
                                        .createTransform());
        Assertions.assertEquals(
                "ErrorCode:[TRANSFORM_COMMON-01], ErrorDescription:[The input field 'ageB' of 'Copy' transform not found in upstream schema]",
                exception2.getMessage());
    }

    @Test
    void testFilterTransformWithError() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(
                                        FilterFieldTransformConfig.INCLUDE_FIELDS.key(),
                                        new ArrayList<String>() {
                                            {
                                                add("age");
                                                add("gender");
                                            }
                                        });
                            }
                        });
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Collections.singletonList(table),
                        config,
                        Thread.currentThread().getContextClassLoader());
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new FilterFieldTransformFactory()
                                        .createTransform(context)
                                        .createTransform());
        Assertions.assertEquals(
                "ErrorCode:[TRANSFORM_COMMON-02], ErrorDescription:[The input fields 'age,gender' of 'Filter' transform not found in upstream schema]",
                exception.getMessage());
    }

    @Test
    void testJsonPathTransformWithError() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(
                                        JsonPathTransformConfig.COLUMNS.key(),
                                        new ArrayList<Map<String, String>>() {
                                            {
                                                add(
                                                        new HashMap<String, String>() {
                                                            {
                                                                put(
                                                                        JsonPathTransformConfig.PATH
                                                                                .key(),
                                                                        "path");
                                                                put(
                                                                        JsonPathTransformConfig
                                                                                .SRC_FIELD
                                                                                .key(),
                                                                        "age");
                                                                put(
                                                                        JsonPathTransformConfig
                                                                                .DEST_FIELD
                                                                                .key(),
                                                                        "age2");
                                                            }
                                                        });
                                            }
                                        });
                            }
                        });
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Collections.singletonList(table),
                        config,
                        Thread.currentThread().getContextClassLoader());
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new JsonPathTransformFactory()
                                        .createTransform(context)
                                        .createTransform());
        Assertions.assertEquals(
                "ErrorCode:[TRANSFORM_COMMON-01], ErrorDescription:[The input field 'age' of 'JsonPath' transform not found in upstream schema]",
                exception.getMessage());
    }

    @Test
    void testReplaceTransformWithError() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(ReplaceTransformConfig.KEY_REPLACE_FIELD.key(), "age");
                                put(ReplaceTransformConfig.KEY_PATTERN.key(), "1");
                                put(ReplaceTransformConfig.KEY_REPLACEMENT.key(), "2");
                                put(ReplaceTransformConfig.KEY_IS_REGEX.key(), "false");
                                put(ReplaceTransformConfig.KEY_REPLACE_FIRST.key(), "false");
                            }
                        });
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Collections.singletonList(table),
                        config,
                        Thread.currentThread().getContextClassLoader());
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new ReplaceTransformFactory()
                                        .createTransform(context)
                                        .createTransform());
        Assertions.assertEquals(
                "ErrorCode:[TRANSFORM_COMMON-01], ErrorDescription:[The input field 'age' of 'Replace' transform not found in upstream schema]",
                exception.getMessage());
    }

    @Test
    void testSplitTransformWithError() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(SplitTransformConfig.KEY_SPLIT_FIELD.key(), "age");
                                put(
                                        SplitTransformConfig.KEY_OUTPUT_FIELDS.key(),
                                        Arrays.asList("age1", "age2"));
                                put(SplitTransformConfig.KEY_SEPARATOR.key(), ",");
                            }
                        });
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Collections.singletonList(table),
                        config,
                        Thread.currentThread().getContextClassLoader());
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                new SplitTransformFactory()
                                        .createTransform(context)
                                        .createTransform());
        Assertions.assertEquals(
                "ErrorCode:[TRANSFORM_COMMON-01], ErrorDescription:[The input field 'age' of 'Split' transform not found in upstream schema]",
                exception.getMessage());
    }
}
