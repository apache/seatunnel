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

package org.apache.seatunnel.connectors.seatunnel.cassandra.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cassandra.client.CassandraClient;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraParameters;
import org.apache.seatunnel.connectors.seatunnel.cassandra.util.TypeConvertUtil;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class CassandraSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final CassandraParameters cassandraParameters;
    private final SingleSplitReaderContext readerContext;
    private CqlSession session;

    CassandraSourceReader(
            CassandraParameters cassandraParameters, SingleSplitReaderContext readerContext) {
        this.cassandraParameters = cassandraParameters;
        this.readerContext = readerContext;
    }

    @Override
    public void open() throws Exception {
        session =
                CassandraClient.getCqlSessionBuilder(
                                cassandraParameters.getHost(),
                                cassandraParameters.getKeyspace(),
                                cassandraParameters.getUsername(),
                                cassandraParameters.getPassword(),
                                cassandraParameters.getDatacenter())
                        .build();
    }

    @Override
    public void close() throws IOException {
        if (session != null) {
            session.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        try {
            ResultSet resultSet =
                    session.execute(
                            CassandraClient.createSimpleStatement(
                                    cassandraParameters.getCql(),
                                    cassandraParameters.getConsistencyLevel()));
            resultSet.forEach(row -> output.collect(TypeConvertUtil.buildSeaTunnelRow(row)));
        } finally {
            this.readerContext.signalNoMoreElement();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
