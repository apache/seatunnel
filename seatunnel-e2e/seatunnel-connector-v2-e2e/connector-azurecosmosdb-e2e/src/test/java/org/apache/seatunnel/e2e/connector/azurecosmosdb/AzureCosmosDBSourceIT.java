/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.azurecosmosdb;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;

public class AzureCosmosDBSourceIT extends AbstractAzureCosmosDBIT {

    @Test
    public void testBasicSourceRead() throws Exception {
        List<SeaTunnelRow> rows =
                readRows(BASIC_CONTAINER, "SELECT c.id, c.name, c.score FROM c", 100);
        rows.sort(Comparator.comparing(row -> String.valueOf(row.getField(0))));

        Assertions.assertEquals(2, rows.size());
        assertRow(rows.get(0), "1", "alpha", 10);
        assertRow(rows.get(1), "2", "beta", 20);
    }

    @Test
    public void testSourceQueryFilterAndProjection() throws Exception {
        List<SeaTunnelRow> rows =
                readRows(
                        FILTER_CONTAINER,
                        "SELECT c.id, c.name, c.score FROM c WHERE c.score > 10",
                        100);
        rows.sort(Comparator.comparing(row -> String.valueOf(row.getField(0))));

        Assertions.assertEquals(2, rows.size());
        assertRow(rows.get(0), "2", "high-score", 30);
        assertRow(rows.get(1), "3", "higher-score", 40);
    }

    @Test
    public void testSourcePaginationWithMaxItemCount() throws Exception {
        List<SeaTunnelRow> rows =
                readRows(PAGINATION_CONTAINER, "SELECT c.id, c.name, c.score FROM c", 1);
        rows.sort(Comparator.comparing(row -> String.valueOf(row.getField(0))));

        Assertions.assertEquals(3, rows.size());
        assertRow(rows.get(0), "1", "page-one", 10);
        assertRow(rows.get(1), "2", "page-two", 20);
        assertRow(rows.get(2), "3", "page-three", 30);
    }

    private void assertRow(SeaTunnelRow row, String id, String name, int score) {
        Assertions.assertEquals(id, row.getField(0));
        Assertions.assertEquals(name, row.getField(1));
        Assertions.assertEquals(score, row.getField(2));
    }
}
