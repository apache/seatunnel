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

package org.apache.seatunnel.connectors.seatunnel.neo4j.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkQueryInfo;

import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class Neo4jSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private final Neo4jSinkQueryInfo neo4jSinkQueryInfo;
    private final transient Driver driver;
    private final transient Session session;

    public Neo4jSinkWriter(Neo4jSinkQueryInfo neo4jSinkQueryInfo) {
        this.neo4jSinkQueryInfo = neo4jSinkQueryInfo;
        this.driver = this.neo4jSinkQueryInfo.getDriverBuilder().build();
        this.session = driver.session(SessionConfig.forDatabase(neo4jSinkQueryInfo.getDriverBuilder().getDatabase()));
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        final Map<String, Object> queryParamPosition = neo4jSinkQueryInfo.getQueryParamPosition().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> element.getField((Integer) e.getValue())));
        final Query query = new Query(neo4jSinkQueryInfo.getQuery(), queryParamPosition);
        session.writeTransaction(tx -> {
            tx.run(query);
            return null;
        });
    }

    @Override
    public Optional<Void> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {

    }

    @Override
    public void close() throws IOException {
        session.close();
        driver.close();
    }
}
