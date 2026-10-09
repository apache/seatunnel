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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Driver;

/**
 * Verifies the URL-acceptance behavior of the actual PostgreSQL and OpenGauss JDBC drivers that
 * this module ships, rather than synthetic in-package driver stubs.
 *
 * <p>The point of this test is to pin down the real root-cause premise: with the pinned driver
 * versions ({@code postgresql-42.4.3}, {@code opengauss-jdbc-5.1.0-og}), the OpenGauss driver
 * registers as {@code org.opengauss.Driver} and only accepts {@code jdbc:opengauss:}/{@code
 * jdbc:dws:iam:} URLs, while PostgreSQL only accepts {@code jdbc:postgresql:} URLs. Neither driver
 * accepts the other's URL prefix, so the URL-based disambiguation is what actually selects the
 * correct driver. If a future driver upgrade reintroduces a genuine class-name/URL collision, these
 * assertions will fail and force a re-check of the fix's premise.
 */
class RealJdbcDriverAcceptsUrlTest {

    @Test
    void testPostgresDriverOnlyAcceptsPostgresUrl() throws Exception {
        Driver driver = new org.postgresql.Driver();
        Assertions.assertTrue(driver.acceptsURL("jdbc:postgresql://localhost:5432/test"));
        Assertions.assertFalse(driver.acceptsURL("jdbc:opengauss://localhost:5432/test"));
    }

    @Test
    void testOpenGaussDriverOnlyAcceptsOpenGaussUrl() throws Exception {
        Driver driver = new org.opengauss.Driver();
        Assertions.assertTrue(driver.acceptsURL("jdbc:opengauss://localhost:5432/test"));
        Assertions.assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432/test"));
    }

    @Test
    void testDriverClassNamesDiffer() {
        // Documents the premise the original issue was based on. If this ever fails (i.e. both
        // share org.postgresql.Driver), the class-name collision is real and the fallback path is
        // load-bearing again.
        Assertions.assertNotEquals(
                org.postgresql.Driver.class.getName(), org.opengauss.Driver.class.getName());
    }
}
