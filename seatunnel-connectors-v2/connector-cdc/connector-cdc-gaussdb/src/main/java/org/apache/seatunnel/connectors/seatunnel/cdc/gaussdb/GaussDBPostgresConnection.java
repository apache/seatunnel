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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * PostgreSQL connection adapter for GaussDB's fixed PostgreSQL 9.2 compatibility version.
 *
 * <p>Debezium requires PostgreSQL 9.4 because that is where PostgreSQL introduced logical
 * replication. GaussDB provides its own logical replication implementation but intentionally
 * reports PostgreSQL 9.2.4 for ecosystem compatibility. This adapter changes only the metadata
 * observed by Debezium's inherited connection-startup validation. All subsequent database calls use
 * the original JDBC connection and its real metadata.
 */
final class GaussDBPostgresConnection extends PostgresConnection {

    /** Minimum major version accepted by Debezium's PostgreSQL startup validation. */
    private static final int DEBEZIUM_MINIMUM_MAJOR_VERSION = 9;

    /** Minimum minor version accepted by Debezium's PostgreSQL startup validation. */
    private static final int DEBEZIUM_MINIMUM_MINOR_VERSION = 4;

    /** Guards the reentrant connection lookup made by Debezium's initial operation. */
    private boolean establishingConnection;

    /** Creates a GaussDB connection with the standard PostgreSQL value converters. */
    GaussDBPostgresConnection(
            JdbcConfiguration config,
            PostgresValueConverterBuilder valueConverterBuilder,
            String connectionUsage) {
        super(config, valueConverterBuilder, connectionUsage);
    }

    /** Supplies compatibility metadata only while Debezium executes its initial version check. */
    @Override
    public synchronized Connection connection(boolean executeOnConnect) throws SQLException {
        if (establishingConnection) {
            return wrapVersionValidationConnection(super.connection(false));
        }
        establishingConnection = true;
        try {
            return super.connection(executeOnConnect);
        } finally {
            establishingConnection = false;
        }
    }

    /** Wraps statements so their connection exposes the validation-only metadata view. */
    private Connection wrapVersionValidationConnection(Connection delegate) {
        return (Connection)
                Proxy.newProxyInstance(
                        GaussDBPostgresConnection.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, arguments) -> {
                            if (isNoArgMethod(method, "getMetaData")) {
                                return wrapVersionValidationMetadata(delegate.getMetaData());
                            }
                            Object result = invoke(delegate, method, arguments);
                            if (result instanceof Statement) {
                                return wrapVersionValidationStatement(
                                        (Statement) result, (Connection) proxy);
                            }
                            return result;
                        });
    }

    /** Makes Statement.getConnection retain the validation-only connection view. */
    private Statement wrapVersionValidationStatement(Statement delegate, Connection connection) {
        return (Statement)
                Proxy.newProxyInstance(
                        GaussDBPostgresConnection.class.getClassLoader(),
                        new Class<?>[] {Statement.class},
                        (proxy, method, arguments) -> {
                            if (isNoArgMethod(method, "getConnection")) {
                                return connection;
                            }
                            return invoke(delegate, method, arguments);
                        });
    }

    /** Reports the logical-replication baseline expected by Debezium's version validator. */
    private DatabaseMetaData wrapVersionValidationMetadata(DatabaseMetaData delegate) {
        return (DatabaseMetaData)
                Proxy.newProxyInstance(
                        GaussDBPostgresConnection.class.getClassLoader(),
                        new Class<?>[] {DatabaseMetaData.class},
                        (proxy, method, arguments) -> {
                            if (isNoArgMethod(method, "getDatabaseMajorVersion")) {
                                return DEBEZIUM_MINIMUM_MAJOR_VERSION;
                            }
                            if (isNoArgMethod(method, "getDatabaseMinorVersion")) {
                                return DEBEZIUM_MINIMUM_MINOR_VERSION;
                            }
                            return invoke(delegate, method, arguments);
                        });
    }

    /** Returns whether a reflected JDBC method has the expected no-argument signature. */
    private boolean isNoArgMethod(Method method, String name) {
        return method.getName().equals(name) && method.getParameterCount() == 0;
    }

    /** Invokes a JDBC delegate while preserving its original checked exception. */
    private Object invoke(Object delegate, Method method, Object[] arguments) throws Throwable {
        try {
            return method.invoke(delegate, arguments);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
