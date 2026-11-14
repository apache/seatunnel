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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public interface SQLEngine {
    void init(
            String inputTableName,
            String catalogTableName,
            SeaTunnelRowType inputRowType,
            String sql);

    SeaTunnelRowType typeMapping(List<String> inputColumnsMapping);

    List<SeaTunnelRow> transformBySQL(SeaTunnelRow inputRow, SeaTunnelRowType outputRowType);

    /**
     * Input column names the query text references outside of star projections: select item
     * expressions, the WHERE clause and lateral view generator arguments. Schema-change translation
     * uses it to fail fast when such a column is dropped or renamed. An empty set means the engine
     * cannot tell; the transform then relies on candidate evaluation to reject an incompatible
     * change.
     *
     * @return referenced input column names, or an empty set when unknown
     */
    default Set<String> referencedInputColumns() {
        return Collections.emptySet();
    }

    /**
     * Describes every output column in produced-column order: star expansion, direct reference,
     * expression or lateral view alias, and the input columns it depends on. The list must have
     * exactly one entry per column returned by {@link #typeMapping(List)}. An empty list means the
     * engine cannot describe its output; schema-change translation then fails explicitly instead of
     * guessing.
     *
     * @return output slot descriptors, or an empty list when unknown
     */
    default List<SQLOutputSlot> describeOutputSlots() {
        return Collections.emptyList();
    }

    /**
     * Checks that the WHERE clause is still type compatible with the input row type this engine was
     * initialised with. Called during schema-change candidate evaluation only, never at planning
     * time or on the row path.
     *
     * @throws IllegalArgumentException when a comparison can no longer be evaluated at row time
     */
    default void validateFilterTypes() {}

    default void close() {}
}
