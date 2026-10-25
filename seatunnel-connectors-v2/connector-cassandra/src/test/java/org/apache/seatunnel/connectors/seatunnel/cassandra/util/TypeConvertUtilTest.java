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

package org.apache.seatunnel.connectors.seatunnel.cassandra.util;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TypeConvertUtilTest {

    @Test
    void testBuildSeaTunnelRowKeepsNullTimestamp() {
        Row row = mock(Row.class);
        ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        ColumnDefinition columnDefinition = mock(ColumnDefinition.class);

        when(row.size()).thenReturn(1);
        when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
        when(columnDefinitions.get(0)).thenReturn(columnDefinition);
        when(columnDefinition.getType()).thenReturn(DataTypes.TIMESTAMP);
        when(row.getInstant(0)).thenReturn(null);

        SeaTunnelRow seaTunnelRow = TypeConvertUtil.buildSeaTunnelRow(row);

        Assertions.assertNull(seaTunnelRow.getField(0));
    }
}
