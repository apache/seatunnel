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

package org.apache.seatunnel.edge.agent.starter.command;

public class EdgeAgentEnvConstants {

    public static final String ENV_EDGE_AGENT_CONFIG = "EDGE_AGENT_CONFIG";

    public static final String ENV_EDGE_AGENT_SQLITE_PATH = "EDGE_AGENT_SQLITE_PATH";

    public static final String ENV_EDGE_AGENT_PID_FILE = "EDGE_AGENT_PID_FILE";

    public static final String PROP_AGENT_HOME = "edge.agent.home";

    public static final String DEFAULT_PID_FILE_NAME = "edge-agent.pid";

    public static final String DEFAULT_SQLITE_RELATIVE_PATH = "data/wal.db";
}
