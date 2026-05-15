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
package org.apache.seatunnel.api.table.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class ConstraintKeyTest {

    @Test
    void vectorIndexKeyAcceptsVectorIndexColumns() {
        VectorIndex vectorIndex = new VectorIndex(null, "vec", "HNSW", "L2");
        ConstraintKey constraintKey =
                ConstraintKey.of(
                        ConstraintKey.ConstraintType.VECTOR_INDEX_KEY,
                        "vec_index",
                        Collections.singletonList(vectorIndex));

        Assertions.assertEquals(
                ConstraintKey.ConstraintType.VECTOR_INDEX_KEY, constraintKey.getConstraintType());
        Assertions.assertEquals("vec_index", constraintKey.getConstraintName());
        Assertions.assertEquals(1, constraintKey.getColumnNames().size());
        Assertions.assertTrue(constraintKey.getColumnNames().get(0) instanceof VectorIndex);
    }

    @Test
    void vectorIndexKeyRejectsNonVectorIndexColumns() {
        ConstraintKey.ConstraintKeyColumn nonVectorColumn =
                ConstraintKey.ConstraintKeyColumn.of("id", ConstraintKey.ColumnSortType.ASC);

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                ConstraintKey.of(
                                        ConstraintKey.ConstraintType.VECTOR_INDEX_KEY,
                                        "vec_index",
                                        Collections.singletonList(nonVectorColumn)));
        Assertions.assertTrue(
                exception.getMessage().contains("VECTOR_INDEX_KEY requires VectorIndex columns"));
    }

    @Test
    void vectorIndexKeyAllowsNullColumnList() {
        ConstraintKey constraintKey =
                ConstraintKey.of(ConstraintKey.ConstraintType.VECTOR_INDEX_KEY, "vec_index", null);
        Assertions.assertNotNull(constraintKey.getColumnNames());
        Assertions.assertTrue(constraintKey.getColumnNames().isEmpty());
    }

    @Test
    void copyVectorIndexKeyCopiesVectorIndexColumns() {
        VectorIndex v0 = new VectorIndex("idx0", "vec0", "HNSW", "L2");
        VectorIndex v1 =
                new VectorIndex(
                        "idx1",
                        "vec1",
                        (VectorIndex.IndexType) null,
                        (VectorIndex.MetricType) null);
        List<ConstraintKey.ConstraintKeyColumn> columns = Arrays.asList(v0, v1);
        ConstraintKey original =
                ConstraintKey.of(
                        ConstraintKey.ConstraintType.VECTOR_INDEX_KEY, "vec_index", columns);

        ConstraintKey copied = original.copy();

        Assertions.assertNotSame(original, copied);
        Assertions.assertEquals(original.getConstraintType(), copied.getConstraintType());
        Assertions.assertEquals(original.getConstraintName(), copied.getConstraintName());
        Assertions.assertEquals(original.getColumnNames().size(), copied.getColumnNames().size());

        Assertions.assertTrue(copied.getColumnNames().get(0) instanceof VectorIndex);
        Assertions.assertTrue(copied.getColumnNames().get(1) instanceof VectorIndex);

        VectorIndex copied0 = (VectorIndex) copied.getColumnNames().get(0);
        VectorIndex copied1 = (VectorIndex) copied.getColumnNames().get(1);

        Assertions.assertNotSame(v0, copied0);
        Assertions.assertNotSame(v1, copied1);

        Assertions.assertEquals("idx0", copied0.getIndexName());
        Assertions.assertEquals("vec0", copied0.getColumnName());
        Assertions.assertEquals(VectorIndex.IndexType.HNSW, copied0.getIndexType());
        Assertions.assertEquals(VectorIndex.MetricType.L2, copied0.getMetricType());

        Assertions.assertEquals("idx1", copied1.getIndexName());
        Assertions.assertEquals("vec1", copied1.getColumnName());
        Assertions.assertNull(copied1.getIndexType());
        Assertions.assertNull(copied1.getMetricType());
    }
}
