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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

public class CopyManagerProxy {
    private static final Logger LOG = LoggerFactory.getLogger(CopyManagerProxy.class);
    Object connection;
    Object copyManager;
    Class<?> connectionClazz;
    Class<?> copyManagerClazz;
    Method getCopyAPIMethod;
    Method copyInMethod;

    Method copyOutWriterMethod;
    Method copyOutOutputStreamMethod;

    public CopyManagerProxy(Connection connection)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
                    SQLException {
        LOG.info("Proxy connection class: {}", connection.getClass().getName());
        this.connection = connection.unwrap(Connection.class);
        LOG.info("Proxy unwrap connection class: {}", this.connection.getClass().getName());
        if (Proxy.isProxyClass(this.connection.getClass())) {
            InvocationHandler handler = Proxy.getInvocationHandler(this.connection);
            this.connection = getConnectionFromInvocationHandler(handler);
            if (null == this.connection) {
                throw new InvocationTargetException(
                        new NullPointerException("Proxy Connection is null."));
            }
            LOG.info("Proxy connection class: {}", this.connection.getClass().getName());
            this.connectionClazz = this.connection.getClass();
        } else {
            this.connectionClazz = this.connection.getClass();
        }
        this.getCopyAPIMethod = this.connectionClazz.getMethod("getCopyAPI");
        this.copyManager = this.getCopyAPIMethod.invoke(this.connection);
        this.copyManagerClazz = this.copyManager.getClass();
        this.copyInMethod = this.copyManagerClazz.getMethod("copyIn", String.class, Reader.class);

        try {
            this.copyOutWriterMethod =
                    this.copyManagerClazz.getMethod("copyOut", String.class, Writer.class);
        } catch (NoSuchMethodException e) {
            LOG.info("copyOut(String, Writer) not found, will try OutputStream");
        }
        try {
            this.copyOutOutputStreamMethod =
                    this.copyManagerClazz.getMethod("copyOut", String.class, OutputStream.class);
        } catch (NoSuchMethodException e) {
            LOG.info("copyOut(String, OutputStream) not found");
        }
    }

    long doCopy(String sql, Reader reader)
            throws InvocationTargetException, IllegalAccessException {
        return (long) this.copyInMethod.invoke(this.copyManager, sql, reader);
    }

    private static Object getConnectionFromInvocationHandler(InvocationHandler handler)
            throws IllegalAccessException {
        Class<?> handlerClass = handler.getClass();
        LOG.info("InvocationHandler class: {}", handlerClass.getName());
        for (Field declaredField : handlerClass.getDeclaredFields()) {
            boolean tempAccessible = declaredField.isAccessible();
            if (!tempAccessible) {
                declaredField.setAccessible(true);
            }
            Object handlerObject = declaredField.get(handler);
            if (handlerObject instanceof Connection) {
                if (!tempAccessible) {
                    declaredField.setAccessible(tempAccessible);
                }
                return handlerObject;
            } else {
                if (!tempAccessible) {
                    declaredField.setAccessible(tempAccessible);
                }
            }
        }
        return null;
    }

    /**
     * Executes COPY TO STDOUT and streams the result into the provided OutputStream.
     *
     * <p>Use when the caller controls the destination sink (file, network, pipe) and wants true
     * streaming with minimal memory footprint. Requires the underlying driver to support
     * CopyManager.copyOut(String, OutputStream).
     *
     * <p>Difference vs {@link #copyOutAsStream(String)}:
     *
     * <ul>
     *   <li>Push-based API: data is pushed to the caller's {@code OutputStream}.
     *   <li>Streaming guarantee depends on the driver's OutputStream variant; there is no in-memory
     *       fallback here—if the method is missing, an exception is thrown.
     *   <li>Backpressure and buffering are controlled by the provided {@code OutputStream}.
     * </ul>
     *
     * @param sql COPY SQL built as "COPY (<select>) TO STDOUT WITH <options>"
     * @param outputStream destination stream to receive COPY data
     * @throws InvocationTargetException if the driver method invocation fails
     * @throws IllegalAccessException if reflective access fails
     */
    public void copyOut(String sql, OutputStream outputStream)
            throws InvocationTargetException, IllegalAccessException {
        if (this.copyOutOutputStreamMethod != null) {
            this.copyOutOutputStreamMethod.invoke(this.copyManager, sql, outputStream);
        } else {
            throw new InvocationTargetException(
                    new NoSuchMethodException(
                            "copyOut(String, OutputStream) method not found on CopyManager"));
        }
    }

    /**
     * Executes COPY TO STDOUT and returns the entire output as a byte array.
     *
     * <p>Convenient for small results or when the full payload is required at once. Internally
     * buffers the whole COPY output in memory.
     *
     * @param sql COPY SQL built as "COPY (<select>) TO STDOUT WITH <options>"
     * @return the COPY output as bytes (UTF-8 encoded if a Writer-based path is used)
     * @throws InvocationTargetException if the driver method invocation fails
     * @throws IllegalAccessException if reflective access fails
     * @throws IOException on I/O errors while buffering
     */
    public byte[] copyOutAsBytes(String sql)
            throws InvocationTargetException, IllegalAccessException, java.io.IOException {
        if (this.copyOutOutputStreamMethod != null) {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            this.copyOutOutputStreamMethod.invoke(this.copyManager, sql, baos);
            baos.flush();
            return baos.toByteArray();
        } else if (this.copyOutWriterMethod != null) {
            java.io.StringWriter writer = new java.io.StringWriter();
            this.copyOutWriterMethod.invoke(this.copyManager, sql, writer);
            writer.flush();
            return writer.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        } else {
            throw new InvocationTargetException(
                    new NoSuchMethodException("No copyOut method found on CopyManager"));
        }
    }

    /**
     * Executes COPY TO STDOUT and returns the entire output as a String.
     *
     * <p>Convenient for small results that are naturally textual (e.g., CSV). The whole output is
     * buffered in memory and returned as a UTF-8 string.
     *
     * @param sql COPY SQL built as "COPY (<select>) TO STDOUT WITH <options>"
     * @return the COPY output as a UTF-8 string
     * @throws InvocationTargetException if the driver method invocation fails
     * @throws IllegalAccessException if reflective access fails
     * @throws IOException on I/O errors while buffering
     */
    public String copyOutAsString(String sql)
            throws InvocationTargetException, IllegalAccessException, java.io.IOException {
        if (this.copyOutWriterMethod != null) {
            StringWriter writer = new StringWriter();
            this.copyOutWriterMethod.invoke(this.copyManager, sql, writer);
            writer.flush();
            return writer.toString();
        } else if (this.copyOutOutputStreamMethod != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            this.copyOutOutputStreamMethod.invoke(this.copyManager, sql, baos);
            baos.flush();
            return new String(baos.toByteArray(), StandardCharsets.UTF_8);
        } else {
            throw new InvocationTargetException(
                    new NoSuchMethodException("No copyOut method found on CopyManager"));
        }
    }

    /**
     * Executes COPY TO STDOUT and returns an InputStream for consuming the result.
     *
     * <p>Attempts to construct org.postgresql.copy.PGCopyInputStream for true streaming. If
     * unavailable, falls back to buffering the entire COPY output in memory and returning a
     * ByteArrayInputStream.
     *
     * <p>Difference vs {@link #copyOut(String, OutputStream)}:
     *
     * <ul>
     *   <li>Pull-based API: the caller reads from an {@code InputStream} at its own pace.
     *   <li>Streaming is best-effort: with PGCopyInputStream it is true streaming; otherwise a
     *       fallback may buffer the entire result in memory.
     *   <li>Simplifies integration when upstream expects an {@code InputStream}, but be aware of
     *       the memory implications in fallback mode.
     * </ul>
     *
     * @param sql COPY SQL built as "COPY (<select>) TO STDOUT WITH <options>"
     * @return an InputStream for reading the COPY output
     * @throws InvocationTargetException if the driver method invocation fails
     * @throws IllegalAccessException if reflective access fails
     * @throws IOException if stream creation or buffering fails
     */
    public InputStream copyOutAsStream(String sql)
            throws InvocationTargetException, IllegalAccessException, IOException {
        try {
            // Prefer PG JDBC's PGCopyInputStream for true streaming
            Class<?> pgConnInterface = Class.forName("org.postgresql.PGConnection");
            Class<?> pgCopyInputStreamClazz =
                    Class.forName("org.postgresql.copy.PGCopyInputStream");

            Object pgConn;
            if (pgConnInterface.isInstance(this.connection)) {
                pgConn = this.connection;
            } else {
                // Some drivers may expose BaseConnection; still try to pass it directly
                pgConn = this.connection;
            }

            try {
                java.lang.reflect.Constructor<?> ctor =
                        pgCopyInputStreamClazz.getConstructor(pgConnInterface, String.class);
                return (InputStream) ctor.newInstance(pgConn, sql);
            } catch (NoSuchMethodException e) {
                // Be compatible with different driver implementations (e.g., BaseConnection)
                java.lang.reflect.Constructor<?> ctor =
                        pgCopyInputStreamClazz.getConstructor(this.connectionClazz, String.class);
                return (InputStream) ctor.newInstance(pgConn, sql);
            }
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException e) {
            // When PGCopyInputStream is unavailable, fall back to in-memory buffering and return a
            // ByteArrayInputStream
            if (this.copyOutOutputStreamMethod != null) {
                java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                this.copyOutOutputStreamMethod.invoke(this.copyManager, sql, baos);
                baos.flush();
                return new java.io.ByteArrayInputStream(baos.toByteArray());
            } else if (this.copyOutWriterMethod != null) {
                java.io.StringWriter writer = new java.io.StringWriter();
                this.copyOutWriterMethod.invoke(this.copyManager, sql, writer);
                writer.flush();
                byte[] bytes = writer.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                return new java.io.ByteArrayInputStream(bytes);
            } else {
                throw new InvocationTargetException(
                        new NoSuchMethodException("No copyOut method found on CopyManager"),
                        e.getMessage());
            }
        }
    }
}
