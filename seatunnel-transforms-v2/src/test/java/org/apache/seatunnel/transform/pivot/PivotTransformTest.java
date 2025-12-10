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

package org.apache.seatunnel.transform.pivot;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for PivotTransform.
 *
 * <p>These tests verify:
 *
 * <ul>
 *   <li>Basic pivot functionality
 *   <li>State snapshot and restore
 *   <li>Flush behavior
 *   <li>Output schema generation
 * </ul>
 */
public class PivotTransformTest {

    private CatalogTable inputTable;
    private PivotTransform transform;

    @BeforeEach
    public void setUp() {
        // Create input table schema: id, type, value
        SeaTunnelRowType inputRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "type", "value"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });

        inputTable = CatalogTableUtil.getCatalogTable("test", inputRowType);

        // Create transform configuration
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(PivotTransformConfig.GROUP_BY_KEYS.key(), Arrays.asList("id"));
        configMap.put(PivotTransformConfig.PIVOT_COLUMN.key(), "type");
        configMap.put(PivotTransformConfig.VALUE_COLUMN.key(), "value");
        configMap.put(PivotTransformConfig.PIVOT_VALUES.key(), Arrays.asList("A", "B", "C"));

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        transform = new PivotTransform(inputTable, config);
        transform.open();
    }

    @Test
    public void testOutputSchema() {
        CatalogTable outputTable = transform.getProducedCatalogTable();
        SeaTunnelRowType outputRowType = outputTable.getSeaTunnelRowType();

        // Should have: id + A + B + C = 4 columns
        Assertions.assertEquals(4, outputRowType.getTotalFields());

        String[] fieldNames = outputRowType.getFieldNames();
        Assertions.assertEquals("id", fieldNames[0]);
        Assertions.assertEquals("A", fieldNames[1]);
        Assertions.assertEquals("B", fieldNames[2]);
        Assertions.assertEquals("C", fieldNames[3]);
    }

    @Test
    public void testBasicPivot() {
        // Collect rows for group id=1
        transform.collect(new SeaTunnelRow(new Object[] {1, "A", 100}));
        transform.collect(new SeaTunnelRow(new Object[] {1, "B", 200}));

        // Collect rows for group id=2
        transform.collect(new SeaTunnelRow(new Object[] {2, "A", 150}));
        transform.collect(new SeaTunnelRow(new Object[] {2, "C", 300}));

        // Flush and get results
        List<SeaTunnelRow> results = transform.flush();

        Assertions.assertEquals(2, results.size());

        // Find row with id=1
        SeaTunnelRow row1 = findRowById(results, 1);
        Assertions.assertNotNull(row1);
        Assertions.assertEquals(1, row1.getField(0)); // id
        Assertions.assertEquals(100, row1.getField(1)); // A
        Assertions.assertEquals(200, row1.getField(2)); // B
        Assertions.assertNull(row1.getField(3)); // C (not set)

        // Find row with id=2
        SeaTunnelRow row2 = findRowById(results, 2);
        Assertions.assertNotNull(row2);
        Assertions.assertEquals(2, row2.getField(0)); // id
        Assertions.assertEquals(150, row2.getField(1)); // A
        Assertions.assertNull(row2.getField(2)); // B (not set)
        Assertions.assertEquals(300, row2.getField(3)); // C
    }

    @Test
    public void testBufferState() {
        // Initially, buffer should be empty
        Assertions.assertFalse(transform.hasBufferedData());
        Assertions.assertEquals(0, transform.getBufferSize());

        // Collect some rows
        transform.collect(new SeaTunnelRow(new Object[] {1, "A", 100}));
        transform.collect(new SeaTunnelRow(new Object[] {2, "B", 200}));

        // Now buffer should have data
        Assertions.assertTrue(transform.hasBufferedData());
        Assertions.assertEquals(2, transform.getBufferSize());

        // Flush
        transform.flush();

        // Buffer should be empty again
        Assertions.assertFalse(transform.hasBufferedData());
        Assertions.assertEquals(0, transform.getBufferSize());
    }

    @Test
    public void testSnapshotAndRestore() throws Exception {
        // Collect some rows
        transform.collect(new SeaTunnelRow(new Object[] {1, "A", 100}));
        transform.collect(new SeaTunnelRow(new Object[] {1, "B", 200}));

        // Snapshot state
        List<PivotGroupState> states = transform.snapshotState(1L);
        Assertions.assertEquals(1, states.size());

        // Create new transform and restore state
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(PivotTransformConfig.GROUP_BY_KEYS.key(), Arrays.asList("id"));
        configMap.put(PivotTransformConfig.PIVOT_COLUMN.key(), "type");
        configMap.put(PivotTransformConfig.VALUE_COLUMN.key(), "value");
        configMap.put(PivotTransformConfig.PIVOT_VALUES.key(), Arrays.asList("A", "B", "C"));

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        PivotTransform newTransform = new PivotTransform(inputTable, config);
        newTransform.open();
        newTransform.restoreState(states);

        // New transform should have the restored data
        Assertions.assertTrue(newTransform.hasBufferedData());
        Assertions.assertEquals(1, newTransform.getBufferSize());

        // Flush and verify data
        List<SeaTunnelRow> results = newTransform.flush();
        Assertions.assertEquals(1, results.size());

        SeaTunnelRow row = results.get(0);
        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertEquals(100, row.getField(1)); // A
        Assertions.assertEquals(200, row.getField(2)); // B
    }

    @Test
    public void testStateSerializer() throws Exception {
        // Create a state
        Map<String, Object> pivotedValues = new HashMap<>();
        pivotedValues.put("A", 100);
        pivotedValues.put("B", 200);

        PivotGroupState originalState =
                new PivotGroupState("1", pivotedValues, new Object[] {1}, "test_table");

        // Serialize and deserialize
        PivotStateSerializer serializer = new PivotStateSerializer();
        byte[] bytes = serializer.serialize(originalState);
        PivotGroupState restoredState = serializer.deserialize(bytes);

        // Verify
        Assertions.assertEquals(originalState.getGroupKey(), restoredState.getGroupKey());
        Assertions.assertEquals(
                originalState.getPivotedValues().get("A"),
                restoredState.getPivotedValues().get("A"));
        Assertions.assertEquals(
                originalState.getPivotedValues().get("B"),
                restoredState.getPivotedValues().get("B"));
    }

    @Test
    public void testMultipleGroupByKeys() {
        // Create transform with multiple group by keys
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(PivotTransformConfig.GROUP_BY_KEYS.key(), Arrays.asList("id", "name"));
        configMap.put(PivotTransformConfig.PIVOT_COLUMN.key(), "type");
        configMap.put(PivotTransformConfig.VALUE_COLUMN.key(), "value");
        configMap.put(PivotTransformConfig.PIVOT_VALUES.key(), Arrays.asList("A", "B"));

        SeaTunnelRowType multiKeyRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "type", "value"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE
                        });
        CatalogTable multiKeyTable = CatalogTableUtil.getCatalogTable("test", multiKeyRowType);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        PivotTransform multiKeyTransform = new PivotTransform(multiKeyTable, config);
        multiKeyTransform.open();

        // Collect rows
        multiKeyTransform.collect(new SeaTunnelRow(new Object[] {1, "John", "A", 100}));
        multiKeyTransform.collect(new SeaTunnelRow(new Object[] {1, "John", "B", 200}));
        multiKeyTransform.collect(new SeaTunnelRow(new Object[] {1, "Jane", "A", 150}));

        // Flush and verify
        List<SeaTunnelRow> results = multiKeyTransform.flush();
        Assertions.assertEquals(2, results.size());

        // Verify output schema has id + name + A + B = 4 columns
        CatalogTable outputTable = multiKeyTransform.getProducedCatalogTable();
        Assertions.assertEquals(4, outputTable.getSeaTunnelRowType().getTotalFields());
    }

    @Test
    public void testIgnoreUnknownPivotValues() {
        // Collect a row with unknown pivot value
        transform.collect(new SeaTunnelRow(new Object[] {1, "A", 100}));
        transform.collect(new SeaTunnelRow(new Object[] {1, "D", 999})); // D is not in pivot_values
        transform.collect(new SeaTunnelRow(new Object[] {1, "B", 200}));

        // Flush and verify
        List<SeaTunnelRow> results = transform.flush();
        Assertions.assertEquals(1, results.size());

        SeaTunnelRow row = results.get(0);
        Assertions.assertEquals(100, row.getField(1)); // A
        Assertions.assertEquals(200, row.getField(2)); // B
        Assertions.assertNull(row.getField(3)); // C (not set, D was ignored)
    }

    @Test
    public void testPluginName() {
        Assertions.assertEquals("Pivot", transform.getPluginName());
    }

    private SeaTunnelRow findRowById(List<SeaTunnelRow> rows, int id) {
        for (SeaTunnelRow row : rows) {
            if (id == (Integer) row.getField(0)) {
                return row;
            }
        }
        return null;
    }
}
