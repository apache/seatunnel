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

package org.apache.seatunnel.api.datasource.gravitino;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.datasource.DataSourceMapper;
import org.apache.seatunnel.api.datasource.DataSourceProvider;
import org.apache.seatunnel.api.metalake.gravitino.GravitinoClient;

import com.google.auto.service.AutoService;

import java.util.Collection;
import java.util.Collections;

/**
 * Gravitino implementation of {@link DataSourceProvider}.
 *
 * <p>This provider integrates with Apache Gravitino for centralized data source metadata
 * management.
 *
 * <p>Configuration (from seatunnel.yaml under seatunnel.engine.datasource):
 *
 * <pre>
 * datasource:
 *   enabled: true
 *   kind: gravitino
 *   uri: <a href="http://localhost:8090">...</a>          # Gravitino server URI
 *   metalake: seatunnel                 # Metalake name
 * </pre>
 */
@AutoService(DataSourceProvider.class)
public class GravitinoDataSourceProvider implements DataSourceProvider {

    private String uri;
    private String metalake;
    private GravitinoClient client;

    private static final String METALAKE_API_PATH = "/api/metalakes/";
    private static final String CATALOGS_PATH = "/catalogs/";

    public static final Option<String> URI =
            Options.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Gravitino server URI, e.g., http://localhost:8090");

    public static final Option<String> METALAKE =
            Options.key("metalake")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Gravitino metalake name to use for data source metadata");

    @Override
    public String kind() {
        return "gravitino";
    }

    @Override
    public void init(Config config) {
        // Extract Gravitino-specific configuration
        String uri = config.getString(URI.key());
        String metalake = config.getString(METALAKE.key());
        // Validate required parameters
        if (uri == null || uri.isEmpty()) {
            throw new IllegalArgumentException(
                    "Gravitino URI is required. Please configure 'seatunnel.engine.datasource.uri' in seatunnel.yaml");
        }
        if (metalake == null || metalake.isEmpty()) {
            throw new IllegalArgumentException(
                    "Gravitino metalake is required. Please configure 'seatunnel.engine.datasource.metalake' in seatunnel.yaml");
        }
        this.uri = uri;
        this.metalake = metalake;
        this.client = new GravitinoClient();
    }

    @Override
    public Collection<DataSourceMapper> dataSourceMappers() {
        return Collections.singletonList(
                new GravitinoJdbcDataSourceMapper(buildMetalakeUrl(), client));
    }

    /**
     * Builds the metalake URL for Gravitino API calls.
     *
     * @return complete metalake URL
     */
    private String buildMetalakeUrl() {
        String baseUri = uri.endsWith("/") ? uri : uri + "/";
        return baseUri + METALAKE_API_PATH + metalake + CATALOGS_PATH;
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
