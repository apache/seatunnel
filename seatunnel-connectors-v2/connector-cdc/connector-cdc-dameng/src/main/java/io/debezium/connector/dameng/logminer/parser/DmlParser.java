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

package io.debezium.connector.dameng.logminer.parser;

import io.debezium.relational.Table;

/** Contract for a DML parser for LogMiner. */
public interface DmlParser {
    /**
     * Parse a DML SQL string from the LogMiner event stream.
     *
     * @param sql the sql statement
     * @param table the table the sql statement is for
     * @param txId the current transaction id the sql is part of.
     * @return the parsed sql as a DML entry or {@code null} if the SQL couldn't be parsed.
     * @throws DmlParserException thrown if a parse exception is detected.
     */
    LogMinerDmlEntry parse(String sql, Table table, String txId);

    LogMinerDmlEntry parseInsert(String sql, Table table, String txId);

    LogMinerDmlEntry parseUpdate(String sql, Table table, String txId);

    LogMinerDmlEntry parseDelete(String sql, Table table, String txId);
}
