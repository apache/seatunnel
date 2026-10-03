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

package org.apache.seatunnel.connectors.seatunnel.starrocks.serialize;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies format-specific restrictions enforced by the StarRocks base conversion path.
 *
 * <p>The CSV serializer exercises this path directly for every non-null field.
 */
public class StarRocksBaseSerializerTest {

    /**
     * Verifies that native JSON columns cannot silently use the CSV conversion path.
     *
     * <p>StarRocks requires JSON Stream Load for columns containing JSON-formatted values.
     */
    @Test
    public void rejectNativeJsonForCsvConversion() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"json"}, new SeaTunnelDataType[] {BasicType.JSON_TYPE});
        StarRocksCsvSerializer serializer = new StarRocksCsvSerializer("\\t", rowType, false);

        StarRocksConnectorException exception =
                Assertions.assertThrows(
                        StarRocksConnectorException.class,
                        () -> serializer.serialize(new SeaTunnelRow(new Object[] {"{\"id\":1}"})));
        Assertions.assertTrue(exception.getMessage().contains("require JSON Stream Load format"));
    }
}
