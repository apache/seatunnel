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

package org.apache.seatunnel.api.env;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;

/**
 * Multiple parsing modes for converting multi-{@link CatalogTable} retrieved through the {@link
 * Catalog} into DAG.
 */
public enum ParsingMode {
    /**
     * Each table is processed using a separate Source and Sink.
     *
     * <pre>
     * customer -> source(customer) -> sink(customer)
     * product  -> source(product)  -> sink(product)
     * stock    -> source(stock)    -> sink(stock)
     * </pre>
     */
    SINGLENESS,
    /**
     * Use a Source and Sink to process sharding-table.
     *
     * <pre>
     * customer1
     * customer2 --> customer\\d+ --> source(customer\\d+) -> sink(customer)
     * customer3
     * </pre>
     */
    SHARDING,
    /**
     * Multiple tables are processed using a single source, each table using a separate sink.
     *
     * <pre>
     * customer                   -> sink(customer)
     * product   --> source(.*)   -> sink(product)
     * stock                      -> sink(stock)
     * </pre>
     */
    @Deprecated
    MULTIPLEX
}
