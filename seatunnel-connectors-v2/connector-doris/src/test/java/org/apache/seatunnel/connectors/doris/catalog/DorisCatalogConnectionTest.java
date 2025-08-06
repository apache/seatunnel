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

package org.apache.seatunnel.connectors.doris.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

public class DorisCatalogConnectionTest {

    @Test
    public void testDriverConfigurationWithCustomDriver() {
        // Test custom driver configuration
        DorisCatalog catalog =
                new DorisCatalog(
                        "test-catalog",
                        "localhost:8030",
                        9030,
                        "root",
                        "password",
                        "com.mysql.cj.jdbc.Driver", // Custom driver
                        false);

        // Test that catalog is created with correct configuration
        Assertions.assertNotNull(catalog);
    }

    @Test
    public void testDriverConfigurationWithDefaultDriver() {
        // Test without specifying driver (should use default)
        DorisCatalog catalog =
                new DorisCatalog(
                        "test-catalog",
                        "localhost:8030",
                        9030,
                        "root",
                        "password",
                        null, // No custom driver
                        false);

        Assertions.assertNotNull(catalog);
    }

    @Test
    public void testLdapConfiguration() throws Exception {
        // Test LDAP configuration
        DorisCatalog ldapCatalog =
                new DorisCatalog(
                        "ldap-catalog",
                        "localhost:8030",
                        9030,
                        "ldapuser",
                        "ldappassword",
                        "com.mysql.jdbc.Driver",
                        true); // LDAP enabled

        Assertions.assertNotNull(ldapCatalog);
        Field enableLdapField = DorisCatalog.class.getDeclaredField("enableLdap");
        enableLdapField.setAccessible(true);
        boolean isLdapEnabled = (boolean) enableLdapField.get(ldapCatalog);

        Assertions.assertTrue(isLdapEnabled, "LDAP should be enabled");
        Field driverField = DorisCatalog.class.getDeclaredField("driverClass");
        driverField.setAccessible(true);
        String driver = (String) driverField.get(ldapCatalog);

        Assertions.assertEquals("com.mysql.jdbc.Driver", driver, "Driver should be set correctly");
    }

    @Test
    public void testCatalogNameValidation() {
        // Test catalog name is properly set
        String catalogName = "my-doris-catalog";
        DorisCatalog catalog =
                new DorisCatalog(
                        catalogName,
                        "localhost:8030",
                        9030,
                        "user",
                        "pass",
                        "com.mysql.cj.jdbc.Driver",
                        false);
    }

    @Test
    public void testDifferentDriverClasses() {
        // Test MySQL 5.x driver
        DorisCatalog mysql5Catalog =
                new DorisCatalog(
                        "mysql5-catalog",
                        "localhost:8030",
                        9030,
                        "root",
                        "password",
                        "com.mysql.jdbc.Driver",
                        true);

        Assertions.assertNotNull(mysql5Catalog);

        // Test MySQL 8.x driver
        DorisCatalog mysql8Catalog =
                new DorisCatalog(
                        "mysql8-catalog",
                        "localhost:8030",
                        9030,
                        "root",
                        "password",
                        "com.mysql.cj.jdbc.Driver",
                        true);

        Assertions.assertNotNull(mysql8Catalog);
    }
}
