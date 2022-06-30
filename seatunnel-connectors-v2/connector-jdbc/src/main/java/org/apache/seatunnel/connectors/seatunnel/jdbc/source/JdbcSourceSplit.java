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

<<<<<<<< HEAD:seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/source/JdbcSourceSplit.java
package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JdbcSourceSplit implements SourceSplit {

    Object[] parameterValues;
    Integer splitId;

    @Override
    public String splitId() {
        return splitId.toString();
    }
========
package org.apache.seatunnel.core.base.command;

import org.apache.seatunnel.apis.base.command.CommandArgs;

/**
 * Command interface.
 *
 * @param <T> args type
 */
@FunctionalInterface
public interface Command<T extends CommandArgs> {

    /**
     * Execute command
     */
    void execute();

>>>>>>>> dev:seatunnel-core/seatunnel-core-base/src/main/java/org/apache/seatunnel/core/base/command/Command.java
}
