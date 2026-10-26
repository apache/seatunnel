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

package org.apache.seatunnel.edge.agent.starter.wal.sqlite;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.edge.agent.starter.config.AgentRuntimeConfig;
import org.apache.seatunnel.edge.agent.starter.config.EdgeAgentRuntimeOptionRules;
import org.apache.seatunnel.edge.agent.starter.wal.WalStore;
import org.apache.seatunnel.edge.agent.starter.wal.WalStoreFactory;

import com.google.auto.service.AutoService;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

@AutoService(Factory.class)
public class SqliteWalStoreFactory implements WalStoreFactory {

    @Override
    public String factoryIdentifier() {
        return "sqlite";
    }

    @Override
    public OptionRule optionRule() {
        return EdgeAgentRuntimeOptionRules.queueRule();
    }

    @Override
    public WalStore create(AgentRuntimeConfig config, Path workDir) throws Exception {
        Path sqlitePath = resolveSqlitePath(config.getSqlitePath(), workDir);
        return new SqliteWalStore(sqlitePath);
    }

    private static Path resolveSqlitePath(String sqlitePath, Path workingDirectory) {
        Objects.requireNonNull(sqlitePath, "sqlitePath");
        Path path = Paths.get(sqlitePath);
        if (path.isAbsolute()) {
            return path;
        }
        return Objects.requireNonNull(workingDirectory, "workingDirectory").resolve(path);
    }
}
