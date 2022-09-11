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

package org.apache.seatunnel.connectors.seatunnel.neo4j.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig;
import org.neo4j.driver.*;

import java.io.IOException;

public class Neo4jSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private final SingleSplitReaderContext context;
    private final Neo4jSourceConfig config;
    private final Driver driver;
    private Session session;


    public Neo4jSourceReader(SingleSplitReaderContext context, Neo4jSourceConfig config) {
        this.context = context;
        this.config = config;
        this.driver = config.getDriverBuilder().build();
    }

    @Override
    public void open() throws Exception {
        this.session = driver.session(SessionConfig.forDatabase(config.getDriverBuilder().getDatabase()));
    }

    @Override
    public void close() throws IOException {
        session.close();
        driver.close();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        final Query query = new Query(config.getQuery());
        session.readTransaction(tx -> {
            final Result result = tx.run(query);
            result.stream()
                    .forEach(row -> {
                        final Object[] values = row.values().stream().map(Value::asObject).toArray();
                        output.collect(new SeaTunnelRow(values));
                    });
            return null;
        });
        this.context.signalNoMoreElement();
    }

    /** TODO : type mapping
     *  The Neo4j mapping for common types is as follows:
     * TypeSystem.NULL() - null
     * TypeSystem.LIST() - List
     * TypeSystem.MAP() - Map
     * TypeSystem.BOOLEAN() - Boolean
     * TypeSystem.INTEGER() - Long
     * TypeSystem.FLOAT() - Double
     * TypeSystem.STRING() - String
     * TypeSystem.BYTES() - byte[]
     * TypeSystem.DATE() - LocalDate
     * TypeSystem.TIME() - OffsetTime
     * TypeSystem.LOCAL_TIME() - LocalTime
     * TypeSystem.DATE_TIME() - ZonedDateTime
     * TypeSystem.LOCAL_DATE_TIME() - LocalDateTime
     * TypeSystem.DURATION() - IsoDuration
     * TypeSystem.POINT() - Point
     * TypeSystem.NODE() - Node
     * TypeSystem.RELATIONSHIP() - Relationship
     * TypeSystem.PATH() - Path
     * Note that the types in TypeSystem refers to the Neo4j type system where TypeSystem.INTEGER() and TypeSystem.FLOAT() are both 64-bit precision. This is why these types return java Long and Double, respectively.
     */

}
