/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class AbstractJdbcCatalogTest {

    @ParameterizedTest
    @ValueSource(strings = {"jdbc:mysql://localhost:5432/", "jdbc:mysql://localhost:5432", "jdbc:mysql://localhost:5432/", "jdbc:postgresql://localhost:5432"})
    public void testValidateJdbcBaseUrl(String baseUrl) {
        Assertions.assertDoesNotThrow(() -> AbstractJdbcCatalog.validateJdbcUrlWithoutDatabase(baseUrl));
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> AbstractJdbcCatalog.validateJdbcUrlWithDatabase(baseUrl));
    }

    @ParameterizedTest
    @ValueSource(strings = {"jdbc:mysql://localhost:5432/db", "jdbc:postgresql://localhost:5432/db"})
    public void testValidateJdbcDefault(String defaultUrl) {
        Assertions.assertDoesNotThrow(() -> AbstractJdbcCatalog.validateJdbcUrlWithDatabase(defaultUrl));
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> AbstractJdbcCatalog.validateJdbcUrlWithoutDatabase(defaultUrl));
    }

    @ParameterizedTest
    @CsvSource({"jdbc:mysql://localhost:5432/db, jdbc:mysql://localhost:5432/, db",
        "jdbc:postgresql://localhost:5432/db, jdbc:postgresql://localhost:5432/, db"})
    public void testSplitDefaultUrl(String defaultUrl, String expectedUrl, String expectedDatabase) {
        Assertions.assertArrayEquals(new String[] {expectedUrl, expectedDatabase}, AbstractJdbcCatalog.splitDefaultUrl(defaultUrl));
    }
}
