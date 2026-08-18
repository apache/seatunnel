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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HugeGraphTypeConverterTest {

    @Test
    void byteMapsToTinyint() {
        assertEquals(
                BasicType.BYTE_TYPE,
                HugeGraphTypeConverter.toSeaTunnelType(DataType.BYTE, Cardinality.SINGLE, "flag"));
    }

    @Test
    void objectMapsToStringSoItDoesNotBlockTheRead() {
        // A cold OBJECT column must be readable (as its string form) instead of throwing and
        // blocking the whole label read.
        assertEquals(
                BasicType.STRING_TYPE,
                HugeGraphTypeConverter.toSeaTunnelType(
                        DataType.OBJECT, Cardinality.SINGLE, "meta"));
    }

    @Test
    void byteListMapsToArrayOfTinyint() {
        SeaTunnelDataType<?> type =
                HugeGraphTypeConverter.toSeaTunnelType(DataType.BYTE, Cardinality.LIST, "flags");
        assertEquals(ArrayType.of(BasicType.BYTE_TYPE), type);
    }

    @Test
    void unsupportedCombinationErrorNamesTheProperty() {
        // The error must identify the offending column so a failure is easy to locate.
        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () ->
                                HugeGraphTypeConverter.toSeaTunnelType(
                                        DataType.BLOB, Cardinality.LIST, "avatar"));
        assertTrue(
                ex.getMessage().contains("avatar"),
                "Error message should name the property: " + ex.getMessage());
    }
}
