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

package org.apache.seatunnel.connectors.seatunnel.hive.source;

import org.apache.seatunnel.common.utils.ReflectionUtils;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

public class TypeConvertTest {

    @Test
    void testWithUnsupportedType() {
        Optional<Method> parseSchema =
                ReflectionUtils.getDeclaredMethod(HiveSource.class, "parseSchema", Table.class);
        Assertions.assertTrue(parseSchema.isPresent());
        Table table = new Table();
        table.setSd(new StorageDescriptor());
        table.getSd().addToCols(new FieldSchema("test", "char", null));
        InvocationTargetException exception =
                Assertions.assertThrows(
                        InvocationTargetException.class,
                        () -> parseSchema.get().invoke(new HiveSource(), table));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-16], ErrorDescription:['Hive' source unsupported convert type 'char' of 'test' to SeaTunnel data type.]",
                exception.getCause().getMessage());
    }
}
