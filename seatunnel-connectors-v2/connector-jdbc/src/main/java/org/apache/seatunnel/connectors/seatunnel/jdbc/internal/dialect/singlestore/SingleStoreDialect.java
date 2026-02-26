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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.singlestore;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlDialect;

/**
 * JDBC dialect for SingleStore (formerly MemSQL), a high-performance real-time analytical database.
 * SingleStore is MySQL-compatible, so this dialect extends {@link MysqlDialect} and reuses MySQL
 * type mapping and row conversion.
 */
public class SingleStoreDialect extends MysqlDialect {

    public SingleStoreDialect() {}

    public SingleStoreDialect(String fieldIde) {
        super(fieldIde);
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.SINGLESTORE;
    }
}
