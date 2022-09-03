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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ScrollResult;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.SourceIndexInfo;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ElasticsearchSourceReader implements SourceReader<SeaTunnelRow, ElasticsearchSourceSplit> {

    protected static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSourceReader.class);

    Context context;

    private Config pluginConfig;

    private EsRestClient esRestClient;

    Deque<ElasticsearchSourceSplit> splits = new LinkedList<>();
    boolean noMoreSplit;

    public ElasticsearchSourceReader(Context context, Config pluginConfig) {
        this.context = context;
        this.pluginConfig = pluginConfig;
    }

    @Override
    public void open() throws Exception {
        esRestClient = EsRestClient.createInstance(this.pluginConfig);
    }

    @Override
    public void close() throws IOException {
        esRestClient.close();
    }


    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        ElasticsearchSourceSplit split = splits.poll();
        if (null != split) {
            SourceIndexInfo sourceIndexInfo = split.getSourceIndexInfo();

            ScrollResult scrollResult = esRestClient.searchByScroll(sourceIndexInfo.getIndex(), sourceIndexInfo.getSource(), sourceIndexInfo.getScrollTime(), sourceIndexInfo.getScrollSize());
            outputFromScrollResult(scrollResult, sourceIndexInfo.getSource(), output);
            while (scrollResult.getDocs() != null && scrollResult.getDocs().size() > 0) {
                scrollResult = esRestClient.searchWithScrollId(scrollResult.getScrollId(), sourceIndexInfo.getScrollTime());
                outputFromScrollResult(scrollResult, sourceIndexInfo.getSource(), output);
            }
        } else if (noMoreSplit) {
            // signal to the source that we have reached the end of the data.
            LOG.info("Closed the bounded ELasticsearch source");
            context.signalNoMoreElement();
        } else {
            Thread.sleep(1000L);
        }
    }

    private void outputFromScrollResult(ScrollResult scrollResult, List<String> source, Collector<SeaTunnelRow> output) {
        int sourceSize = source.size();
        for (Map<String, Object> doc : scrollResult.getDocs()) {
            SeaTunnelRow seaTunnelRow = new SeaTunnelRow(sourceSize);
            for (int i = 0; i < sourceSize; i++) {
                Object value = doc.get(source.get(i));
                seaTunnelRow.setField(i, String.valueOf(value));
            }
            output.collect(seaTunnelRow);
        }
    }

    @Override
    public List<ElasticsearchSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splits);
    }

    @Override
    public void addSplits(List<ElasticsearchSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
