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

package org.apache.seatunnel.connectors.seatunnel.jms.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.MessageFormat;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.api.configuration.util.Conditions.greaterThan;
import static org.apache.seatunnel.api.configuration.util.Conditions.notBlank;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.CONNECTION_FACTORY_CLASS;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.CONNECTION_FACTORY_PROPERTIES;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.DESTINATION;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.JNDI_CONNECTION_FACTORY_NAME;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.JNDI_INITIAL_CONTEXT_FACTORY;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.JNDI_PROPERTIES;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.JNDI_PROVIDER_URL;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.MAX_IN_FLIGHT_MESSAGES;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.MESSAGE_SELECTOR;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.USERNAME;

/** Factory of the JMS queue source. */
@AutoService(Factory.class)
public class JmsSourceFactory implements TableSourceFactory {

    static final String IDENTIFIER = "Jms";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .exclusive(CONNECTION_FACTORY_CLASS, JNDI_CONNECTION_FACTORY_NAME)
                .required(DESTINATION, notBlank(DESTINATION))
                .required(SCHEMA)
                .optional(
                        CONNECTION_FACTORY_PROPERTIES,
                        JNDI_INITIAL_CONTEXT_FACTORY,
                        JNDI_PROVIDER_URL,
                        JNDI_PROPERTIES)
                .bundled(USERNAME, PASSWORD)
                .optional(MESSAGE_SELECTOR, FORMAT)
                .optional(FIELD_DELIMITER, notBlank(FIELD_DELIMITER))
                .optional(MAX_IN_FLIGHT_MESSAGES, greaterThan(MAX_IN_FLIGHT_MESSAGES, 0))
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        validate(config);
        CatalogTable table = CatalogTableUtil.buildWithConfig(config);
        DeserializationSchema<SeaTunnelRow> deserializer = createDeserializer(config, table);
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>) new JmsSource(config, table, deserializer);
    }

    static DeserializationSchema<SeaTunnelRow> createDeserializer(
            ReadonlyConfig config, CatalogTable table) {
        return config.get(FORMAT) == MessageFormat.JSON
                ? new JsonDeserializationSchema(table, false, false)
                : TextDeserializationSchema.builder()
                        .seaTunnelRowType(table.getSeaTunnelRowType())
                        .delimiter(config.get(FIELD_DELIMITER))
                        .build();
    }

    /**
     * Validates the options before any connection is opened, including for callers that create the
     * source directly. Options of the connection factory mode that is not used are rejected rather
     * than ignored.
     */
    static void validate(ReadonlyConfig config) {
        ConfigValidator.of(config).validate(new JmsSourceFactory().optionRule());
        if (config.getOptional(CONNECTION_FACTORY_CLASS).isPresent()) {
            if (config.getOptional(JNDI_INITIAL_CONTEXT_FACTORY).isPresent()
                    || config.getOptional(JNDI_PROVIDER_URL).isPresent()
                    || config.getOptional(JNDI_PROPERTIES).isPresent()) {
                throw new IllegalArgumentException(
                        "The jndi_* options apply only with jndi_connection_factory_name, "
                                + "not with connection_factory_class");
            }
        } else {
            if (config.getOptional(CONNECTION_FACTORY_PROPERTIES).isPresent()) {
                throw new IllegalArgumentException(
                        "connection_factory_properties applies only with "
                                + "connection_factory_class");
            }
            if (!config.getOptional(JNDI_INITIAL_CONTEXT_FACTORY).isPresent()) {
                throw new IllegalArgumentException(
                        "jndi_initial_context_factory is required with "
                                + "jndi_connection_factory_name");
            }
        }
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return JmsSource.class;
    }
}
