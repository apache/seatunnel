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

package org.apache.seatunnel.api.table.factory;

import org.apache.seatunnel.api.table.connector.TableSink;

import java.util.Collections;
import java.util.List;

/**
 * This is an SPI interface, used to create {@link TableSink}. Each plugin need to have it own
 * implementation.
 *
 * @param <IN> row type
 * @param <StateT> state type
 * @param <CommitInfoT> commit info type
 * @param <AggregatedCommitInfoT> aggregated commit info type
 */
public interface TableSinkFactory<IN, StateT, CommitInfoT, AggregatedCommitInfoT> extends Factory {

    /**
     * We will never use this method now. So gave a default implement and return null.
     *
     * @param context TableFactoryContext
     * @return return the sink created by this factory
     */
    default TableSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> createSink(
            TableSinkFactoryContext context) {
        throw new UnsupportedOperationException(
                "The Factory has not been implemented and the deprecated Plugin will be used.");
    }

    /**
     * Validates sink connectivity and schema compatibility for {@code --dry-run=connect} without
     * creating sink writers or writing records.
     *
     * <p>The upstream schema is available through {@link
     * TableSinkFactoryContext#getCatalogTable()}. Connector implementations can use this hook for
     * metadata-level checks such as credentials, permissions, target existence, target
     * createability, and field/type compatibility.
     *
     * @param context sink factory context with upstream catalog table and resolved options
     * @throws Exception when connectivity or schema validation fails
     */
    default void validateConnectionForDryRun(TableSinkFactoryContext context) throws Exception {}

    @Deprecated
    default List<String> excludeTablePlaceholderReplaceKeys() {
        return Collections.emptyList();
    }
}
