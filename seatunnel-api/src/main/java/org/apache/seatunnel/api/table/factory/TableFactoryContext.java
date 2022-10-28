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
import java.util.Map;

public class TableFactoryContext {

    private final List<CatalogTable> catalogTables;
    private final Map<String, String> options;
    private final ClassLoader classLoader;

    public TableFactoryContext(
            List<CatalogTable> catalogTables,
            Map<String, String> options,
            ClassLoader classLoader) {
        this.catalogTables = catalogTables;
        this.options = options;
        this.classLoader = classLoader;
    }

    public ClassLoader getClassLoader() {
        return this.classLoader;
    }

    /**
     * Returns a list of tables that need to be processed.
     *
     * <p> By default, return only single table.
     *
     * <p> If you need multiple tables, implement {@link SupportMultipleTable}.
     */
    public List<CatalogTable> getCatalogTables() {
        return catalogTables;
    }

    public Map<String, String> getOptions() {
        return this.options;
    }
}
