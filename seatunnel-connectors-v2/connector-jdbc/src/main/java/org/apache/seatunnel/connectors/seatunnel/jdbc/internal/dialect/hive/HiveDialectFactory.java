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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hive;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.inceptor.InceptorDialect;

import com.google.auto.service.AutoService;

/** Factory for {@link HiveDialect}. */
@AutoService(JdbcDialectFactory.class)
public class HiveDialectFactory implements JdbcDialectFactory {

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:hive2:");
    }

    @Override
    public JdbcDialect create() {
        throw new UnsupportedOperationException(
                "Can't create JdbcDialect without compatible mode for Hive");
    }

    @Override
    public JdbcDialect create(String compatibleMode, String fieldId) {
        if ("inceptor".equals(compatibleMode)) {
            return new InceptorDialect();
        }
        return new HiveDialect();
    }
}
