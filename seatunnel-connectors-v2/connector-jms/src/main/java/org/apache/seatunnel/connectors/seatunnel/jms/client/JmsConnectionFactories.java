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

package org.apache.seatunnel.connectors.seatunnel.jms.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsBaseOptions.CONNECTION_FACTORY_CLASS;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsBaseOptions.CONNECTION_FACTORY_PROPERTIES;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsBaseOptions.JNDI_CONNECTION_FACTORY_NAME;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsBaseOptions.JNDI_INITIAL_CONTEXT_FACTORY;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsBaseOptions.JNDI_PROPERTIES;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsBaseOptions.JNDI_PROVIDER_URL;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsBaseOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsBaseOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode.CREATE_CONNECTION_FACTORY_FAILED;

/**
 * Creates the provider's {@link ConnectionFactory} from the connector options. Property and JNDI
 * values may hold credentials, so errors name the key but never the value, and never carry a
 * provider exception whose message could repeat the value.
 */
public final class JmsConnectionFactories {

    /** Setter parameter types in the order they are tried. */
    private static final List<Class<?>> SETTER_TYPES =
            Arrays.asList(
                    String.class,
                    int.class,
                    Integer.class,
                    long.class,
                    Long.class,
                    boolean.class,
                    Boolean.class);

    private JmsConnectionFactories() {}

    /** Creates the connection factory configured by the class or the JNDI options. */
    public static ConnectionFactory create(ReadonlyConfig config) {
        if (config.getOptional(CONNECTION_FACTORY_CLASS).isPresent()) {
            return fromClass(
                    config.get(CONNECTION_FACTORY_CLASS),
                    config.getOptional(CONNECTION_FACTORY_PROPERTIES)
                            .orElse(Collections.emptyMap()));
        }
        return fromJndi(config);
    }

    /** Opens a connection with the configured credentials, if any. */
    public static Connection createConnection(ConnectionFactory factory, ReadonlyConfig config)
            throws JMSException {
        if (config.getOptional(USERNAME).isPresent()) {
            return factory.createConnection(
                    config.get(USERNAME), config.getOptional(PASSWORD).orElse(null));
        }
        return factory.createConnection();
    }

    static ConnectionFactory fromClass(String className, Map<String, String> properties) {
        Class<?> factoryClass = loadClass(className);
        if (!ConnectionFactory.class.isAssignableFrom(factoryClass)) {
            throw new JmsConnectorException(
                    CREATE_CONNECTION_FACTORY_FAILED,
                    "Class " + className + " does not implement javax.jms.ConnectionFactory");
        }
        Object factory;
        try {
            factory = factoryClass.getConstructor().newInstance();
        } catch (InvocationTargetException e) {
            throw new JmsConnectorException(
                    CREATE_CONNECTION_FACTORY_FAILED,
                    "The constructor of "
                            + className
                            + " failed with "
                            + e.getTargetException().getClass().getName());
        } catch (ReflectiveOperationException e) {
            throw new JmsConnectorException(
                    CREATE_CONNECTION_FACTORY_FAILED,
                    "Class " + className + " has no public no-argument constructor");
        }
        // A fixed order keeps the result independent of how the config map is ordered.
        for (Map.Entry<String, String> property : new TreeMap<>(properties).entrySet()) {
            applyProperty(factory, property.getKey(), property.getValue());
        }
        return (ConnectionFactory) factory;
    }

    private static Class<?> loadClass(String className) {
        ClassLoader connectorLoader = JmsConnectionFactories.class.getClassLoader();
        try {
            return Class.forName(className, true, connectorLoader);
        } catch (ClassNotFoundException | LinkageError first) {
            ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
            if (contextLoader != null && contextLoader != connectorLoader) {
                try {
                    return Class.forName(className, true, contextLoader);
                } catch (ClassNotFoundException | LinkageError ignored) {
                    // Report the first failure below.
                }
            }
            throw new JmsConnectorException(
                    CREATE_CONNECTION_FACTORY_FAILED,
                    "Could not load connection factory class "
                            + className
                            + " ("
                            + first
                            + "). Put the JMS provider's client jars in "
                            + "${SEATUNNEL_HOME}/plugins/connector-jms/");
        }
    }

    /**
     * Applies one property. Every part of a dotted key but the last names a getter, and the last
     * names a setter with one String, int, long or boolean parameter (or a boxed form).
     */
    static void applyProperty(Object factory, String key, String value) {
        String[] parts = key.split("\\.", -1);
        Object target = factory;
        for (int i = 0; i < parts.length - 1; i++) {
            Method getter = findGetter(target.getClass(), parts[i]);
            if (getter == null) {
                throw unknownProperty(factory, key);
            }
            Object next = invoke(getter, target, key);
            if (next == null) {
                throw new JmsConnectorException(
                        CREATE_CONNECTION_FACTORY_FAILED,
                        "Connection factory property "
                                + key
                                + ": "
                                + getter.getName()
                                + "() returned null");
            }
            target = next;
        }
        String name = parts[parts.length - 1];
        boolean found = false;
        for (Class<?> type : SETTER_TYPES) {
            Method setter = findSetter(target.getClass(), name, type);
            if (setter == null) {
                continue;
            }
            found = true;
            Object converted = convert(value, type);
            if (converted != null) {
                invoke(setter, target, key, converted);
                return;
            }
        }
        if (found) {
            throw new JmsConnectorException(
                    CREATE_CONNECTION_FACTORY_FAILED,
                    "The value of connection factory property "
                            + key
                            + " does not match the type of its setter");
        }
        throw unknownProperty(factory, key);
    }

    private static JmsConnectorException unknownProperty(Object factory, String key) {
        return new JmsConnectorException(
                CREATE_CONNECTION_FACTORY_FAILED,
                "Unknown connection factory property "
                        + key
                        + " for "
                        + factory.getClass().getName());
    }

    private static Method findGetter(Class<?> type, String name) {
        if (name.isEmpty()) {
            return null;
        }
        for (Method method : type.getMethods()) {
            if (method.getParameterCount() == 0
                    && method.getReturnType() != void.class
                    && (method.getName().equals("get" + capitalize(name))
                            || method.getName().equals("is" + capitalize(name)))) {
                return method;
            }
        }
        return null;
    }

    private static Method findSetter(Class<?> type, String name, Class<?> parameterType) {
        if (name.isEmpty()) {
            return null;
        }
        for (Method method : type.getMethods()) {
            if (method.getParameterCount() == 1
                    && method.getParameterTypes()[0] == parameterType
                    && method.getName().equals("set" + capitalize(name))) {
                return method;
            }
        }
        return null;
    }

    /** Returns null when the value cannot be converted to the type. */
    private static Object convert(String value, Class<?> type) {
        if (type == String.class) {
            return value;
        }
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        try {
            if (type == int.class || type == Integer.class) {
                return Integer.valueOf(trimmed);
            }
            if (type == long.class || type == Long.class) {
                return Long.valueOf(trimmed);
            }
        } catch (NumberFormatException e) {
            return null;
        }
        if ("true".equalsIgnoreCase(trimmed)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(trimmed)) {
            return Boolean.FALSE;
        }
        return null;
    }

    private static Object invoke(Method method, Object target, String key, Object... args) {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw new JmsConnectorException(
                    CREATE_CONNECTION_FACTORY_FAILED,
                    "Applying connection factory property "
                            + key
                            + " failed with "
                            + e.getTargetException().getClass().getName());
        } catch (IllegalAccessException e) {
            throw new JmsConnectorException(
                    CREATE_CONNECTION_FACTORY_FAILED,
                    "Connection factory property " + key + " is not accessible");
        }
    }

    private static String capitalize(String name) {
        return Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    static ConnectionFactory fromJndi(ReadonlyConfig config) {
        String name = config.get(JNDI_CONNECTION_FACTORY_NAME);
        Hashtable<String, Object> environment = new Hashtable<>();
        config.getOptional(JNDI_PROPERTIES).ifPresent(environment::putAll);
        environment.put(Context.INITIAL_CONTEXT_FACTORY, config.get(JNDI_INITIAL_CONTEXT_FACTORY));
        config.getOptional(JNDI_PROVIDER_URL)
                .ifPresent(url -> environment.put(Context.PROVIDER_URL, url));
        // JNDI loads the initial context factory through the context class loader.
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(JmsConnectionFactories.class.getClassLoader());
        InitialContext context = null;
        try {
            context = new InitialContext(environment);
            Object found = context.lookup(name);
            if (!(found instanceof ConnectionFactory)) {
                throw new JmsConnectorException(
                        CREATE_CONNECTION_FACTORY_FAILED,
                        "JNDI name "
                                + name
                                + " is bound to "
                                + (found == null ? "null" : found.getClass().getName())
                                + ", not to a javax.jms.ConnectionFactory");
            }
            return (ConnectionFactory) found;
        } catch (NamingException e) {
            throw new JmsConnectorException(
                    CREATE_CONNECTION_FACTORY_FAILED,
                    "JNDI lookup of connection factory "
                            + name
                            + " failed with "
                            + e.getClass().getName());
        } finally {
            thread.setContextClassLoader(previous);
            if (context != null) {
                try {
                    context.close();
                } catch (NamingException ignored) {
                    // The factory has been obtained; a failed close does not affect it.
                }
            }
        }
    }
}
