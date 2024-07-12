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

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

/** Vector Database need special Index on its vector field. */
@EqualsAndHashCode(callSuper = true)
@Getter
public class VectorIndex extends ConstraintKey.ConstraintKeyColumn implements Serializable {

    /** Vector index name */
    private final String indexName;

    /** Vector indexType, such as IVF_FLAT, HNSW, DISKANN */
    private final IndexType indexType;

    /** Vector index metricType, such as L2, IP, COSINE */
    private final MetricType metricType;

    public VectorIndex(String indexName, String columnName, String indexType, String metricType) {
        super(columnName, null);
        this.indexName = indexName;
        this.indexType = IndexType.of(indexType);
        this.metricType = MetricType.of(metricType);
    }

    public VectorIndex(
            String indexName, String columnName, IndexType indexType, MetricType metricType) {
        super(columnName, null);
        this.indexName = indexName;
        this.indexType = indexType;
        this.metricType = metricType;
    }

    @Override
    public ConstraintKey.ConstraintKeyColumn copy() {
        return new VectorIndex(indexName, getColumnName(), indexType, metricType);
    }

    public enum IndexType {
        FLAT,
        IVF_FLAT,
        IVF_SQ8,
        IVF_PQ,
        HNSW,
        DISKANN,
        AUTOINDEX,
        SCANN,

        // GPU indexes only for float vectors
        GPU_IVF_FLAT,
        GPU_IVF_PQ,
        GPU_BRUTE_FORCE,
        GPU_CAGRA,

        // Only supported for binary vectors
        BIN_FLAT,
        BIN_IVF_FLAT,

        // Only for varchar type field
        TRIE,
        // Only for scalar type field
        STL_SORT, // only for numeric type field
        INVERTED, // works for all scalar fields except JSON type field

        // Only for sparse vectors
        SPARSE_INVERTED_INDEX,
        SPARSE_WAND,
        ;

        public static IndexType of(String name) {
            return valueOf(name.toUpperCase());
        }
    }

    public enum MetricType {
        // Only for float vectors
        L2,
        IP,
        COSINE,

        // Only for binary vectors
        HAMMING,
        JACCARD,
        ;

        public static MetricType of(String name) {
            return valueOf(name.toUpperCase());
        }
    }
}
