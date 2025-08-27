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

package org.apache.seatunnel.transform.sql.zeta;

import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.VectorUtils;
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.SQLEngineFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class VectorFunctionTest {

    @Test
    public void testCosineDistanceFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"vector_float1", "vector_float2"},
                        new SeaTunnelDataType[] {
                            VectorType.VECTOR_FLOAT_TYPE, VectorType.VECTOR_SPARSE_FLOAT_TYPE
                        });
        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 3.0f}),
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 3.0f})
                        });

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select COSINE_DISTANCE(vector_float1, vector_float2) as cosineDistance from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Assertions.assertEquals(0.0, f1Object);
    }

    @Test
    public void testL1DistanceFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"vector_float1", "vector_float2"},
                        new SeaTunnelDataType[] {
                            VectorType.VECTOR_FLOAT_TYPE, VectorType.VECTOR_FLOAT_TYPE
                        });
        HashMap<Integer, Float> sparseVector = Maps.newHashMap();
        sparseVector.put(0, 1.0f);
        sparseVector.put(1, 2.0f);
        sparseVector.put(2, 3.0f);
        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Float[] {2.0f, 4.0f, 6.0f}), sparseVector
                        });

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select L1_DISTANCE(vector_float1, vector_float2) as l1Distance from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Assertions.assertEquals(6.0, f1Object);
    }

    @Test
    public void testL2DistanceFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"vector_float1", "vector_float2"},
                        new SeaTunnelDataType[] {
                            VectorType.VECTOR_FLOAT_TYPE, VectorType.VECTOR_FLOAT_TYPE
                        });

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Float[] {2.0f, 4.0f, 4.0f}),
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 2.0f})
                        });

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select L2_DISTANCE(vector_float1, vector_float2) as l2Distance from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Assertions.assertEquals(3.0, f1Object);
    }

    @Test
    public void testVectorNormFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"vector_float1", "vector_float2"},
                        new SeaTunnelDataType[] {
                            VectorType.VECTOR_FLOAT_TYPE, VectorType.VECTOR_FLOAT_TYPE
                        });

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 2.0f}),
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 3.0f})
                        });

        sqlEngine.init(
                "test", null, rowType, "select VECTOR_NORM(vector_float1) as norm from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Assertions.assertEquals(3.0, f1Object);
    }

    @Test
    public void testVectorDimsFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"vector_float1"},
                        new SeaTunnelDataType[] {VectorType.VECTOR_FLOAT_TYPE});

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 3.0f}),
                        });

        sqlEngine.init("test", null, rowType, "select VECTOR_DIMS(vector_float1) as dim from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Assertions.assertEquals(3, f1Object);
    }

    @Test
    public void testInnerProductFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"vector_float1", "vector_float2"},
                        new SeaTunnelDataType[] {
                            VectorType.VECTOR_FLOAT_TYPE, VectorType.VECTOR_FLOAT_TYPE
                        });

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 3.0f}),
                            VectorUtils.toByteBuffer(new Float[] {7.0f, 8.0f, 9.0f})
                        });

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select INNER_PRODUCT(vector_float1, vector_float2) as innerProduct from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Assertions.assertEquals(50.0, f1Object);
    }
}
