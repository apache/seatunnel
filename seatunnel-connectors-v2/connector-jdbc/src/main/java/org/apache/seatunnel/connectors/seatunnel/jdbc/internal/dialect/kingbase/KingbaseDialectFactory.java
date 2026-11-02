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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

import java.sql.Connection;

/** Factory for {@link KingbaseDialect}. */
@AutoService(JdbcDialectFactory.class)
public class KingbaseDialectFactory implements JdbcDialectFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KingbaseDialectFactory.class);

    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.KINGBASE;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:kingbase8:");
    }

    @Override
    public JdbcDialect create() {
        return new KingbaseDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new KingbaseDialect(compatibleMode, fieldIde);
    }

    @Override
    public JdbcDialect create(
            String compatibleMode, String fieldIde, JdbcConnectionConfig jdbcConnectionConfig) {
        String detectedCompatibleMode = compatibleMode;
        if (detectedCompatibleMode == null && jdbcConnectionConfig != null) {
            detectedCompatibleMode = detectCompatibleMode(jdbcConnectionConfig);
        }
        return new KingbaseDialect(detectedCompatibleMode, fieldIde);
    }

    private String detectCompatibleMode(JdbcConnectionConfig config) {
        SimpleJdbcConnectionProvider provider = new SimpleJdbcConnectionProvider(config);
        try {
            Connection connection = provider.getOrEstablishConnection();
            if (connection instanceof com.kingbase8.jdbc.KbConnection) {
                String level = ((com.kingbase8.jdbc.KbConnection) connection).getCompatibleLevel();
                return level;
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to detect KingbaseES compatible mode from connection, fallback to default.",
                    e);
        } finally {
            provider.closeConnection();
        }
        return null;
    }
}
