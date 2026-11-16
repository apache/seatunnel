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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.goldendb;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlDialect;

import com.google.auto.service.AutoService;

/** Selects the MySQL-compatible dialect for GoldenDB JDBC connections. */
@AutoService(JdbcDialectFactory.class)
public class GoldenDbDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectFactoryName() {
        return "GoldenDB";
    }

    @Override
    public boolean acceptsURL(String url) {
        // Preserve the generic dialect used by existing GoldenDB URL configurations.
        return false;
    }

    @Override
    public JdbcDialect create() {
        return new MysqlDialect();
    }

    @Override
    public JdbcDialect create(String compatibleMode, String fieldIde) {
        return new MysqlDialect(fieldIde);
    }
}
