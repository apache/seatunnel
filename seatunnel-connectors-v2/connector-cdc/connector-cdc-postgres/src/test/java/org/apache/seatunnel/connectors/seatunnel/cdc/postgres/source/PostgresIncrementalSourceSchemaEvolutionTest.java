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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class PostgresIncrementalSourceSchemaEvolutionTest {

    @Test
    void shouldNotCreateSchemaChangeResolverWhenDisabled() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                SourceOptions.SCHEMA_CHANGES_ENABLED.key(), false));

        Assertions.assertNull(PostgresIncrementalSource.createSchemaChangeResolver(config));
    }

    @Test
    void shouldCreateSchemaChangeResolverWhenEnabled() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(SourceOptions.SCHEMA_CHANGES_ENABLED.key(), true));

        Assertions.assertNotNull(PostgresIncrementalSource.createSchemaChangeResolver(config));
    }
}
