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

package org.apache.seatunnel.connectors.seatunnel.doris.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class DorisSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DorisSourceReader.class);

    private final SingleSplitReaderContext context;

    private final String[] names = {"Wenjun", "Fanjia", "Zongwen", "CalvinKirs"};
    private final int[] ages = {11, 22, 33, 44};

    public DorisSourceReader(SingleSplitReaderContext context) {
        this.context = context;
    }

    @Override
    public void open() {
        // nothing
    }

    @Override
    public void close() {
        // nothing
    }

    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws InterruptedException {
        // Generate a random number of rows to emit.
        Random random = ThreadLocalRandom.current();
        int size = random.nextInt(10) + 1;
        for (int i = 0; i < size; i++) {
            int randomIndex = random.nextInt(names.length);
            SeaTunnelRow seaTunnelRow = new SeaTunnelRow(new Object[]{names[randomIndex], ages[randomIndex], System.currentTimeMillis()});
            output.collect(seaTunnelRow);
        }
        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            LOGGER.info("Closed the bounded fake source");
            context.signalNoMoreElement();
        }

    }
}
