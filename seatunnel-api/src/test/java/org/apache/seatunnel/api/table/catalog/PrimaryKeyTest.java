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

package org.apache.seatunnel.api.table.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Verifies primary key column copies remain mutable for runtime schema event serialization. This
 * prevents Flink Kryo from populating the immutable caller-owned collection.
 */
class PrimaryKeyTest {

    /**
     * Ensures immutable caller-owned columns do not become the primary key's backing collection.
     */
    @Test
    void copiesImmutableColumnNames() {
        List<String> columnNames = Collections.unmodifiableList(Arrays.asList("id"));

        PrimaryKey primaryKey = PrimaryKey.of("pk", columnNames, true);

        Assertions.assertEquals(columnNames, primaryKey.getColumnNames());
        primaryKey.getColumnNames().add("name");
        Assertions.assertEquals(Collections.singletonList("id"), columnNames);
        Assertions.assertEquals(Arrays.asList("id", "name"), primaryKey.getColumnNames());
    }
}
