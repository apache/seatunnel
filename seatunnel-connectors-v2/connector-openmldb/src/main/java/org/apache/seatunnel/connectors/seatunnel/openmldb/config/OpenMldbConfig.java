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

package org.apache.seatunnel.connectors.seatunnel.openmldb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class OpenMldbConfig {
    private static final int DEFAULT_SESSION_TIMEOUT = 10000;
    private static final int DEFAULT_REQUEST_TIMEOUT = 60000;
    public static final Option<String> ZK_HOST = Options.key("zk_host")
            .stringType()
            .noDefaultValue()
            .withDescription("Zookeeper server host");
    public static final Option<String> ZK_PATH = Options.key("zk_path")
            .stringType()
            .noDefaultValue()
            .withDescription("Zookeeper server path of OpenMldb cluster");
    public static final Option<String> HOST = Options.key("host")
            .stringType()
            .noDefaultValue()
            .withDescription("OpenMldb host");
    public static final Option<Integer> PORT = Options.key("port")
            .intType()
            .noDefaultValue()
            .withDescription("OpenMldb port");
    public static final Option<Integer> SESSION_TIMEOUT = Options.key("session_timeout")
            .intType()
            .defaultValue(DEFAULT_SESSION_TIMEOUT)
            .withDescription("OpenMldb session timeout");
    public static final Option<Integer> REQUEST_TIMEOUT = Options.key("request_timeout")
            .intType()
            .defaultValue(DEFAULT_REQUEST_TIMEOUT)
            .withDescription("OpenMldb request timeout");
    public static final Option<Boolean> CLUSTER_MODE = Options.key("cluster_mode")
            .booleanType()
            .noDefaultValue()
            .withDescription("Whether cluster mode is enabled");
    public static final Option<String> SQL = Options.key("sql")
            .stringType()
            .noDefaultValue()
            .withDescription("Sql statement");
    public static final Option<String> DATABASE = Options.key("database")
            .stringType()
            .noDefaultValue()
            .withDescription("The database you want to access");
}
