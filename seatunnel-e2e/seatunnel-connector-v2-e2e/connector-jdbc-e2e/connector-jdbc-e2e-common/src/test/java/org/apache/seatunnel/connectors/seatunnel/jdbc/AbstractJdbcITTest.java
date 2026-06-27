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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AbstractJdbcITTest {

    @Test
    void shouldAddMavenCentralFallbackForRepo1Urls() {
        String driverUrl =
                "https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3-standalone.jar";

        String command = AbstractJdbcIT.buildDriverDownloadCommand(driverUrl);

        Assertions.assertTrue(command.contains(driverUrl));
        Assertions.assertTrue(
                command.contains(
                        "https://repo.maven.apache.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3-standalone.jar"));
        Assertions.assertTrue(command.contains("curl --fail --location --retry 5 --retry-delay 2"));
        Assertions.assertTrue(
                command.contains(
                        "wget --tries=5 --waitretry=2 --retry-connrefused --no-check-certificate"));
    }

    @Test
    void shouldKeepSingleDownloadAttemptForNonRepo1Urls() {
        String driverUrl =
                "https://repo.maven.apache.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar";

        String command = AbstractJdbcIT.buildDriverDownloadCommand(driverUrl);

        Assertions.assertFalse(command.contains(" || "));
    }
}
