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

package org.apache.seatunnel.connectors.seatunnel.jms.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

/** Options that obtain a JMS {@code ConnectionFactory} and open a connection. */
public class JmsBaseOptions {

    public static final Option<String> CONNECTION_FACTORY_CLASS =
            Options.key("connection_factory_class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Fully qualified name of the provider's javax.jms.ConnectionFactory "
                                    + "implementation. It is created with its no-argument "
                                    + "constructor. Exclusive with jndi_connection_factory_name.");

    public static final Option<Map<String, String>> CONNECTION_FACTORY_PROPERTIES =
            Options.key("connection_factory_properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Properties applied to the connection factory through its public "
                                    + "setters, for example brokerURL. A dotted key such as "
                                    + "prefetchPolicy.queuePrefetch calls the getters for the "
                                    + "leading parts and the setter for the last part.");

    public static final Option<String> JNDI_INITIAL_CONTEXT_FACTORY =
            Options.key("jndi_initial_context_factory")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "JNDI initial context factory class, required with "
                                    + "jndi_connection_factory_name.");

    public static final Option<String> JNDI_PROVIDER_URL =
            Options.key("jndi_provider_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JNDI provider URL.");

    public static final Option<String> JNDI_CONNECTION_FACTORY_NAME =
            Options.key("jndi_connection_factory_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "JNDI name of the connection factory to look up. Exclusive with "
                                    + "connection_factory_class.");

    public static final Option<Map<String, String>> JNDI_PROPERTIES =
            Options.key("jndi_properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Additional JNDI environment properties.");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "User name passed to createConnection(username, password) together "
                                    + "with password.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password passed to createConnection(username, password) together "
                                    + "with username.");

    public static final Option<String> DESTINATION =
            Options.key("destination")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the queue, passed to Session.createQueue. Its syntax is "
                                    + "defined by the provider.");
}
