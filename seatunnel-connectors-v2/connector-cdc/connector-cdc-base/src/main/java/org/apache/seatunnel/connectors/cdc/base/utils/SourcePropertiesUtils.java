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
package org.apache.seatunnel.connectors.cdc.base.utils;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * When a subclass inherits the {@link JdbcSourceConfigFactory} class to implement the create method,
 *   * First obtain the public Properties configuration through this class
 */
public class SourcePropertiesUtils {

    public static Properties getProperties(JdbcSourceConfigFactory sourceConfigFactory,Integer subtaskId){
        Map<String, Object> sourceConfig = sourceConfigFactory.getSourceConfig();
        Properties props = new Properties();

        // database Basic information
        props.setProperty("database.hostname", checkNotNull(String.valueOf(sourceConfig.get("hostname"))));
        props.setProperty("database.user", checkNotNull(String.valueOf(sourceConfig.get("username"))));
        props.setProperty("database.password", checkNotNull(String.valueOf(sourceConfig.get("password"))));
        props.setProperty("database.port", checkNotNull(String.valueOf(sourceConfig.get("port"))));

        // database history
        props.setProperty("database.history", EmbeddedDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtaskId);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));

        // TODO Not yet supported
        props.setProperty("include.schema.changes", String.valueOf(false));

        if (sourceConfig.get("databaseList") != null){
            props.setProperty("database.include.list",
                    String.join(",",(List<String>) sourceConfig.get("databaseList")));
        }

        return props;
    }


}
