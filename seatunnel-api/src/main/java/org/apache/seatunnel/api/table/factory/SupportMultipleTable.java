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

import org.apache.seatunnel.api.table.catalog.CatalogTable;

import java.util.List;

/**
 * Used to declare that the connector can handle data from multiple tables.
 * <p> The expansion of the {@link TableSourceFactory}.
 */
public interface SupportMultipleTable {

    /**
     * A connector can pick tables and return the accepted and remaining tables.
     */
    Result applyTables(TableFactoryContext context);

    final class Result {
        private final List<CatalogTable> acceptedTables;
        private final List<CatalogTable> remainingTables;

        private Result(
                List<CatalogTable> acceptedTables,
                List<CatalogTable> remainingTables) {
            this.acceptedTables = acceptedTables;
            this.remainingTables = remainingTables;
        }

        public static Result of(
                List<CatalogTable> acceptedTables,
                List<CatalogTable> remainingTables) {
            return new Result(acceptedTables, remainingTables);
        }

        public List<CatalogTable> getAcceptedTables() {
            return acceptedTables;
        }

        public List<CatalogTable> getRemainingTables() {
            return remainingTables;
        }
    }
}
