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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;

/** MySQL-CDC smoke test on MySQL 5.6 with GTIDs enabled, the first version that has them. */
public class Mysql5_6CDCIT extends AbstractMysqlCDCSmokeITBase {

    public Mysql5_6CDCIT() {
        super(
                MySqlVersion.V5_6,
                "docker/server-gtids/my5-6.cnf",
                "mysql_cdc_e2e_5_6",
                "/mysqlcdc_to_mysql_5_6_smoke.conf");
    }
}
