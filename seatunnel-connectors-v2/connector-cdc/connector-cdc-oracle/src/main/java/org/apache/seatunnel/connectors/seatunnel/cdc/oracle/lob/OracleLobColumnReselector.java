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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.lob;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Reads LOB column values for one Oracle row identified by its primary key. */
public interface OracleLobColumnReselector extends Serializable, AutoCloseable {

    /**
     * Re-selects the requested LOB columns.
     *
     * <p>Implementations should query {@code AS OF SCN commitScn} when that SCN is usable, and fall
     * back to the current row when flashback is unavailable. Returned values are materialized
     * {@link String} or {@code byte[]} values, or null when the column is SQL NULL. Live JDBC LOB
     * locators must not be returned.
     *
     * @param commitScn Debezium {@code source.commit_scn}, which may be null
     * @return a result whose {@code found} flag is false when no row matches the key
     */
    ReselectResult reselect(
            String schema,
            String table,
            List<String> lobColumns,
            List<String> primaryKeyColumns,
            List<Object> primaryKeyValues,
            String commitScn)
            throws SQLException;

    @Override
    void close();

    /** Outcome of one primary-key LOB lookup. */
    final class ReselectResult implements Serializable {
        private static final long serialVersionUID = 1L;

        private final boolean found;
        private final Map<String, Object> values;

        private ReselectResult(boolean found, Map<String, Object> values) {
            this.found = found;
            this.values = values;
        }

        public static ReselectResult notFound() {
            return new ReselectResult(false, Collections.<String, Object>emptyMap());
        }

        public static ReselectResult found(Map<String, Object> values) {
            return new ReselectResult(true, values);
        }

        public boolean isFound() {
            return found;
        }

        public Map<String, Object> getValues() {
            return values;
        }
    }
}
