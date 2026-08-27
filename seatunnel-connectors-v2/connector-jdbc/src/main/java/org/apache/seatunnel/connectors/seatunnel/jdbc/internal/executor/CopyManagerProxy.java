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

import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

class CopyManagerProxy {
    private static final Logger LOG = LoggerFactory.getLogger(CopyManagerProxy.class);
    Object connection;
    Object copyManager;
    Class<?> connectionClazz;
    Class<?> copyManagerClazz;
    Method getCopyAPIMethod;
    Method copyInMethod;

    CopyManagerProxy(Connection connection)
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
}
