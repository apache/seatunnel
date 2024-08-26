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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils;

import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class CatalogUtilsTest {

    @Test
    void testPrimaryKeysNameWithOutSpecialChar() throws SQLException {
        Optional<PrimaryKey> primaryKey =
                CatalogUtils.getPrimaryKey(new TestDatabaseMetaData(), TablePath.of("test.test"));
        Assertions.assertEquals("testfdawe_", primaryKey.get().getPrimaryKey());
    }

    @Test
    void testConstraintKeysNameWithOutSpecialChar() throws SQLException {
        List<ConstraintKey> constraintKeys =
                CatalogUtils.getConstraintKeys(
                        new TestDatabaseMetaData(), TablePath.of("test.test"));
        Assertions.assertEquals("testfdawe_", constraintKeys.get(0).getConstraintName());
    }
}
