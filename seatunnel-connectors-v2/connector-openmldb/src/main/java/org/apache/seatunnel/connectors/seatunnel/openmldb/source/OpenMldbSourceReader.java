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

package org.apache.seatunnel.connectors.seatunnel.openmldb.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbParameters;

import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.ResultSet;

@Slf4j
public class OpenMldbSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final SqlClusterExecutor sqlClusterExecutor;
    private final OpenMldbParameters openMldbParameters;
    private final SeaTunnelRowType seaTunnelRowType;

    public OpenMldbSourceReader(SqlClusterExecutor sqlClusterExecutor,
                                OpenMldbParameters openMldbParameters,
                                SeaTunnelRowType seaTunnelRowType) {
        this.sqlClusterExecutor = sqlClusterExecutor;
        this.openMldbParameters = openMldbParameters;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public void open() throws Exception {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        sqlClusterExecutor.close();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        int totalFields = seaTunnelRowType.getTotalFields();
        Object[] objects = new Object[totalFields];
        try (ResultSet resultSet = sqlClusterExecutor.executeSQL(openMldbParameters.getDatabase(),
                openMldbParameters.getSql())) {
            while (resultSet.next()) {
                for (int i = 0; i < totalFields; i++) {
                    objects[i] = resultSet.getObject(i);
                }
                output.collect(new SeaTunnelRow(objects));
            }
        }
    }
}
