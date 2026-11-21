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

import io.debezium.relational.Table;
import io.debezium.text.ParsingException;

/**
 * Basic contract for parsers that can decode a single column's preamble sequence.
 *
 * <p>Official (Debezium 3.7.1.Final): {@code
 * debezium-connector-oracle/.../parser/SingleColumnSqlRedoPreambleParser.java}.
 *
 * @author Chris Cranford
 */
public interface SingleColumnSqlRedoPreambleParser {
    /**
     * Parses the redo SQL. Must be called before fetching state from the parser.
     *
     * @param sql the redo SQL to be parsed, can be {@code null}
     * @param table the table the redo SQL is related to, should not be {@code null}
     * @return the preamble parse data
     * @throws ParsingException if the parser failed to parse the redo SQL
     */
    LogMinerDmlEntry parse(String sql, Table table) throws ParsingException;

    /**
     * Get the schema name of the parsed statement.
     *
     * @return the schema name
     */
    String getSchemaName();

    /**
     * Get the table name of the parsed statement.
     *
     * @return the table name
     */
    String getTableName();

    /**
     * Get the name of the parsed column.
     *
     * @return the column name
     */
    String getColumnName();
}
