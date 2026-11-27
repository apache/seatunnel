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

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

/**
 * Factory for {@link KingbaseDialect}.
 *
 * <p>The KingbaseES compatibility mode is taken exclusively from the explicit {@code
 * compatible_mode} option. This factory never opens a connection during planning: an absent mode
 * deterministically selects the native Kingbase (PostgreSQL-compatible) behavior, which is the
 * pre-existing behavior of this connector.
 */
@AutoService(JdbcDialectFactory.class)
public class KingbaseDialectFactory implements JdbcDialectFactory {

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
}
