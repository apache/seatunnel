/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.druid.sink;

import lombok.NonNull;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.druid.client.DruidOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.druid.config.DruidSinkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DruidSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>{
    private static final Logger LOGGER = LoggerFactory.getLogger(DruidSinkWriter.class);
    private static final long serialVersionUID = -7210857670269773005L;
    private SeaTunnelRowType seaTunnelRowType;
    private DruidOutputFormat druidOutputFormat;

    public DruidSinkWriter(@NonNull SeaTunnelRowType seaTunnelRowType,
                          @NonNull DruidSinkOptions druidSinkOptions) {
        this.seaTunnelRowType = seaTunnelRowType;
        druidOutputFormat = new DruidOutputFormat(druidSinkOptions.getCoordinatorURL(),
                druidSinkOptions.getDatasource(),
                druidSinkOptions.getTimestampColumn(),
                druidSinkOptions.getTimestampFormat(),
                druidSinkOptions.getTimestampMissingValue(),
                druidSinkOptions.getColumns()
                );
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        druidOutputFormat.write(element);
    }

    @Override
    public void close() throws IOException {
        druidOutputFormat.closeOutputFormat();
    }
}
