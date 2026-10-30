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
package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/**
 * Adapts bulk copy through the installed SQL Server driver's own classloader.
 *
 * <p>The JDBC connector does not package vendor drivers. Keeping vendor types out of this class's
 * signatures lets unrelated JDBC jobs load the connector without Microsoft classes on their path.
 */
final class SqlserverBulkCopyProxy implements AutoCloseable {
    private static final String DRIVER_PACKAGE = "com.microsoft.sqlserver.jdbc.";
    private final Object bulkCopy;
    private final Class<?> bulkDataType;
    private final ClassLoader driverClassLoader;

    SqlserverBulkCopyProxy(Connection connection) throws SQLException {
        try {
            Class<?> connectionType = loadConnectionType(connection);
            driverClassLoader = connectionType.getClassLoader();
            Connection driverConnection =
                    connectionType.isInstance(connection)
                            ? connection
                            : connection.unwrap(connectionType.asSubclass(Connection.class));
            Class<?> bulkCopyType =
                    Class.forName(DRIVER_PACKAGE + "SQLServerBulkCopy", true, driverClassLoader);
            bulkDataType =
                    Class.forName(DRIVER_PACKAGE + "ISQLServerBulkData", true, driverClassLoader);
            bulkCopy = bulkCopyType.getConstructor(Connection.class).newInstance(driverConnection);
        } catch (ReflectiveOperationException e) {
            throw failure(e);
        }
    }

    void setDestinationTableName(String name) throws SQLException {
        invoke("setDestinationTableName", new Class<?>[] {String.class}, name);
    }

    void addColumnMapping(int ordinal, String name) throws SQLException {
        invoke("addColumnMapping", new Class<?>[] {int.class, String.class}, ordinal, name);
    }

    /**
     * Configures the driver's options to retain ordinary INSERT value semantics.
     *
     * <p>The JDBC writer owns commit and rollback; bulk copy must use its existing transaction.
     */
    void configure(int batchSize) throws SQLException {
        try {
            Class<?> optionsType =
                    Class.forName(
                            DRIVER_PACKAGE + "SQLServerBulkCopyOptions", true, driverClassLoader);
            Object options = optionsType.getConstructor().newInstance();
            optionsType
                    .getMethod("setUseInternalTransaction", boolean.class)
                    .invoke(options, false);
            optionsType.getMethod("setKeepIdentity", boolean.class).invoke(options, true);
            optionsType.getMethod("setKeepNulls", boolean.class).invoke(options, true);
            optionsType.getMethod("setCheckConstraints", boolean.class).invoke(options, true);
            optionsType.getMethod("setFireTriggers", boolean.class).invoke(options, true);
            optionsType.getMethod("setBatchSize", int.class).invoke(options, batchSize);
            invoke("setBulkCopyOptions", new Class<?>[] {optionsType}, options);
        } catch (ReflectiveOperationException e) {
            throw failure(e);
        }
    }

    /**
     * Supplies row data through an interface defined by the installed driver's classloader.
     *
     * <p>The proxy avoids binding this connector to a second copy of the vendor interface.
     */
    void writeToServer(BulkData data) throws SQLException {
        Object driverData =
                Proxy.newProxyInstance(
                        driverClassLoader,
                        new Class<?>[] {bulkDataType},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "getColumnOrdinals":
                                    return data.getColumnOrdinals();
                                case "getColumnName":
                                    return data.getColumnName((Integer) args[0]);
                                case "getColumnType":
                                    return data.getColumnType((Integer) args[0]);
                                case "getPrecision":
                                    return data.getPrecision((Integer) args[0]);
                                case "getScale":
                                    return data.getScale((Integer) args[0]);
                                case "getRowData":
                                    return data.getRowData();
                                case "next":
                                    return data.next();
                                case "toString":
                                    return "SeaTunnel SQL Server bulk data";
                                case "hashCode":
                                    return System.identityHashCode(proxy);
                                case "equals":
                                    return proxy == args[0];
                                default:
                                    throw new UnsupportedOperationException(
                                            "Unsupported SQL Server bulk data method: "
                                                    + method.getName());
                            }
                        });
        invoke("writeToServer", new Class<?>[] {bulkDataType}, driverData);
    }

    @Override
    public void close() throws SQLException {
        invoke("close", new Class<?>[0]);
    }

    private void invoke(String name, Class<?>[] types, Object... arguments) throws SQLException {
        try {
            Method method = bulkCopy.getClass().getMethod(name, types);
            method.invoke(bulkCopy, arguments);
        } catch (ReflectiveOperationException e) {
            throw failure(e);
        }
    }

    /**
     * Finds vendor types in the connection's loader, falling back to the task context loader.
     *
     * <p>This also supports wrapped connections without binding optional driver types to the
     * connector's loader.
     */
    private static Class<?> loadConnectionType(Connection connection)
            throws ClassNotFoundException {
        try {
            return Class.forName(
                    DRIVER_PACKAGE + "SQLServerConnection",
                    false,
                    connection.getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            return Class.forName(
                    DRIVER_PACKAGE + "SQLServerConnection",
                    false,
                    Thread.currentThread().getContextClassLoader());
        }
    }

    /**
     * Preserves vendor SQLExceptions so the JDBC writer can classify their SQLState for retries.
     *
     * <p>Reflection failures instead report an unsupported installed-driver contract with the
     * original cause; wrapping a vendor SQLException would obscure the reconnect decision.
     */
    private static SQLException failure(ReflectiveOperationException error) {
        Throwable cause = error instanceof InvocationTargetException ? error.getCause() : error;
        if (cause instanceof SQLException) {
            return (SQLException) cause;
        }
        return new SQLException(
                "Unable to access SQL Server bulk copy through the installed JDBC driver.",
                "0A000",
                cause);
    }

    /**
     * Exposes the row and metadata values requested by the vendor's bulk-copy interface.
     *
     * <p>Only JDK types cross this boundary so unrelated JDBC jobs need no Microsoft driver.
     */
    interface BulkData {
        Set<Integer> getColumnOrdinals();

        String getColumnName(int column);

        int getColumnType(int column);

        int getPrecision(int column);

        int getScale(int column);

        Object[] getRowData();

        boolean next();
    }
}
