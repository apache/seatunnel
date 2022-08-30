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
package org.apache.seatunnel.connectors.seatunnel.druid.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.seatunnel.connectors.seatunnel.druid.client.DruidInputFormat;
import java.io.IOException;

public class DruidSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DruidSourceReader.class);

    private final SingleSplitReaderContext context;
    private final DruidInputFormat druidInputFormat;

    public DruidSourceReader(SingleSplitReaderContext context,DruidInputFormat druidInputFormat) {
        this.context = context;
        this.druidInputFormat = druidInputFormat;
    }

    @Override
    public void open() throws Exception {
        druidInputFormat.openInputFormat();
    }

    @Override
    public void close() throws IOException {
        druidInputFormat.closeInputFormat();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        while (!druidInputFormat.reachedEnd()) {
            SeaTunnelRow seaTunnelRow = druidInputFormat.nextRecord();
            output.collect(seaTunnelRow);
        }
        druidInputFormat.closeInputFormat();
        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            LOGGER.info("Closed the bounded Druid source");
            context.signalNoMoreElement();
        }
    }
}