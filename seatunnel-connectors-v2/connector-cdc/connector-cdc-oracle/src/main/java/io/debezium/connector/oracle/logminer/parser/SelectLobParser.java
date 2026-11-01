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

package io.debezium.connector.oracle.logminer.parser;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.relational.Table;

/**
 * Simple text-based parser implementation for Oracle LogMiner SEL_LOB_LOCATOR Redo SQL.
 *
 * @author Chris Cranford
 */
@NotThreadSafe
public class SelectLobParser extends PreambleSingleColumnReconstructedSelectParser {

    private static final String BEGIN = "BEGIN";

    private static final String BLOB_LOCATOR = "loc_b";
    private static final String BLOB_BUFFER = "buf_b";

    private boolean binary;

    public SelectLobParser() {
        super(BEGIN);
    }

    public boolean isBinary() {
        return binary;
    }

    @Override
    protected void reset(Table table) {
        super.reset(table);
        this.binary = false;
    }

    @Override
    protected int parseIntoClause(String sql, int index) {
        if (sql.indexOf(BLOB_LOCATOR, index) == index || sql.indexOf(BLOB_BUFFER, index) == index) {
            binary = true;
        }
        return sql.indexOf(" ", index) + 1;
    }

    @Override
    protected LogMinerDmlEntry createDmlEntryForColumnValues(Object[] columnValues) {
        return LogMinerDmlEntryImpl.forLobLocator(columnValues);
    }
}
