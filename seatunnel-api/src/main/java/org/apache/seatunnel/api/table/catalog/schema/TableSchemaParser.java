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

package org.apache.seatunnel.api.table.catalog.schema;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;

import java.util.List;

public interface TableSchemaParser<T> {

    /**
     * Parse schema config to TableSchema
     *
     * @param schemaConfig schema config
     * @return TableSchema
     */
    TableSchema parse(T schemaConfig);

    @Deprecated
    interface FieldParser<T> {

        /**
         * Parse field config to List<Column>
         *
         * @param schemaConfig schema config
         * @return List<Column> column list
         */
        List<Column> parse(T schemaConfig);
    }

    interface ColumnParser<T> {

        /**
         * Parse column config to List<Column>
         *
         * @param schemaConfig schema config
         * @return List<Column> column list
         */
        List<Column> parse(T schemaConfig);
    }

    interface ConstraintKeyParser<T> {

        /**
         * Parse constraint key config to ConstraintKey
         *
         * @param schemaConfig schema config
         * @return List<ConstraintKey> constraint key list
         */
        List<ConstraintKey> parse(T schemaConfig);
    }

    interface PrimaryKeyParser<T> {

        /**
         * Parse primary key config to PrimaryKey
         *
         * @param schemaConfig schema config
         * @return PrimaryKey
         */
        PrimaryKey parse(T schemaConfig);
    }
}
