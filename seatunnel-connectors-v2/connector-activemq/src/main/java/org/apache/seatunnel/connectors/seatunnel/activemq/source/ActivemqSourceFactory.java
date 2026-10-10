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

package org.apache.seatunnel.connectors.seatunnel.activemq.source;

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
import org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSourceOptions.MessageFormat;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.seatunnel.api.configuration.util.Conditions.greaterThan;
import static org.apache.seatunnel.api.configuration.util.Conditions.notBlank;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.QUEUE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.URI;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSourceOptions.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSourceOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSourceOptions.MAX_IN_FLIGHT_MESSAGES;

/** Factory for the streaming, checkpoint-aware ActiveMQ Classic queue source. */
@AutoService(Factory.class)
public class ActivemqSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "ActiveMQ";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URI, notBlank(URI))
                .required(QUEUE_NAME, notBlank(QUEUE_NAME))
                .required(SCHEMA)
                .bundled(USERNAME, PASSWORD)
                .optional(FORMAT)
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
        DeserializationSchema<SeaTunnelRow> deserializer =
                config.get(FORMAT) == MessageFormat.JSON
                        ? new JsonDeserializationSchema(table, false, false)
                        : TextDeserializationSchema.builder()
                                .seaTunnelRowType(table.getSeaTunnelRowType())
                                .delimiter(config.get(FIELD_DELIMITER))
                                .build();
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new ActivemqSource(config, table, deserializer);
    }

    /** Validate before opening connections, including callers that construct a source directly. */
    static void validate(ReadonlyConfig config) {
        ConfigValidator.of(config).validate(new ActivemqSourceFactory().optionRule());
        try {
            URI endpoint = new URI(config.get(URI));
            if (!("tcp".equals(endpoint.getScheme()) || "ssl".equals(endpoint.getScheme()))
                    || endpoint.getHost() == null
                    || endpoint.getPort() <= 0
                    || endpoint.getPort() > 65535
                    || endpoint.getUserInfo() != null
                    || endpoint.getQuery() != null
                    || endpoint.getFragment() != null
                    || (endpoint.getPath() != null && !endpoint.getPath().isEmpty())) {
                throw new IllegalArgumentException(
                        "ActiveMQ source uri must be tcp://host:port or ssl://host:port; "
                                + "use username/password options for credentials. "
                                + "Composite transports and URI options are not supported.");
            }
        } catch (URISyntaxException e) {
            // Do not include the supplied URI: it may contain credentials.
            throw new IllegalArgumentException("Invalid ActiveMQ source uri");
        }
        String queue = config.get(QUEUE_NAME);
        if (queue.contains(",")
                || queue.contains("?")
                || queue.contains(">")
                || queue.contains("*")
                || queue.contains("://")) {
            throw new IllegalArgumentException(
                    "ActiveMQ source queue_name must name one literal queue without destination options");
        }
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ActivemqSource.class;
    }
}
