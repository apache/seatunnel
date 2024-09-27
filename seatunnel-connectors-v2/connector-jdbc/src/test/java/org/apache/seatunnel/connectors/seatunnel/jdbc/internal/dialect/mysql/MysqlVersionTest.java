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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MysqlVersionTest {

    @Test
    public void testMysqlVersionParse() {
        Assertions.assertEquals(MySqlVersion.V_5_5, MySqlVersion.parse("5.5.0"));
        Assertions.assertEquals(MySqlVersion.V_5_5, MySqlVersion.parse("5.5.1"));
        Assertions.assertEquals(MySqlVersion.V_5_5, MySqlVersion.parse("5.5.12"));

        Assertions.assertEquals(MySqlVersion.V_5_6, MySqlVersion.parse("5.6.0"));
        Assertions.assertEquals(MySqlVersion.V_5_6, MySqlVersion.parse("5.6.1"));
        Assertions.assertEquals(MySqlVersion.V_5_6, MySqlVersion.parse("5.6.12"));

        Assertions.assertEquals(MySqlVersion.V_5_7, MySqlVersion.parse("5.7.0"));
        Assertions.assertEquals(MySqlVersion.V_5_7, MySqlVersion.parse("5.7.1"));
        Assertions.assertEquals(MySqlVersion.V_5_7, MySqlVersion.parse("5.7.12"));

        Assertions.assertEquals(MySqlVersion.V_8, MySqlVersion.parse("8.0.0"));
        Assertions.assertEquals(MySqlVersion.V_8, MySqlVersion.parse("8.0.1"));
        Assertions.assertEquals(MySqlVersion.V_8, MySqlVersion.parse("8.0.12"));

        Assertions.assertEquals(MySqlVersion.V_8_1, MySqlVersion.parse("8.1.0"));
        Assertions.assertEquals(MySqlVersion.V_8_1, MySqlVersion.parse("8.1.4"));
        Assertions.assertEquals(MySqlVersion.V_8_1, MySqlVersion.parse("8.1.14"));

        Assertions.assertEquals(MySqlVersion.V_8_2, MySqlVersion.parse("8.2.0"));
        Assertions.assertEquals(MySqlVersion.V_8_2, MySqlVersion.parse("8.2.4"));
        Assertions.assertEquals(MySqlVersion.V_8_2, MySqlVersion.parse("8.2.14"));

        Assertions.assertEquals(MySqlVersion.V_8_3, MySqlVersion.parse("8.3.0"));
        Assertions.assertEquals(MySqlVersion.V_8_3, MySqlVersion.parse("8.3.4"));
        Assertions.assertEquals(MySqlVersion.V_8_3, MySqlVersion.parse("8.3.14"));

        Assertions.assertEquals(MySqlVersion.V_8_4, MySqlVersion.parse("8.4.0"));
        Assertions.assertEquals(MySqlVersion.V_8_4, MySqlVersion.parse("8.4.4"));
        Assertions.assertEquals(MySqlVersion.V_8_4, MySqlVersion.parse("8.4.14"));
    }
}
