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

package org.apache.seatunnel.connectors.seatunnel.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.PutRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Datahub write class
 */
public class DataHubWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(DataHubWriter.class);
    private DatahubClient dataHubClient;
    private String project;
    private String topic;
    private SeaTunnelRowType seaTunnelRowType;
    private static final int DATAHUB_CONNECT_MAX_TIMEOUT = 10000;
    public DataHubWriter(SeaTunnelRowType seaTunnelRowType, String endpoint, String accessId, String accessKey, String project, String topic) {
        this.dataHubClient = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(new DatahubConfig(endpoint,
                                new AliyunAccount(accessId, accessKey), true))
                .setHttpConfig(new HttpConfig().setCompressType(HttpConfig.CompressType.LZ4)
                .setConnTimeout(DATAHUB_CONNECT_MAX_TIMEOUT))
                .build();
        this.seaTunnelRowType = seaTunnelRowType;
        this.project = project;
        this.topic = topic;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        Object[] fields = element.getFields();
        List<RecordEntry> recordEntries = new ArrayList<>();
        RecordSchema recordSchema = dataHubClient.getTopic(project, topic).getRecordSchema();
        for (int i = 0; i < fieldNames.length; i++) {
            TupleRecordData data = new TupleRecordData(recordSchema);
            data.setField(fieldNames[i], fields[i]);
            RecordEntry recordEntry = new RecordEntry();
            recordEntry.setRecordData(data);
            recordEntries.add(recordEntry);
        }
        try {
            PutRecordsResult result = dataHubClient.putRecords(project, topic, recordEntries);
            LOG.info("putRecordResult:" + result.toString());
        }  catch (DatahubClientException e) {
            LOG.error("requestId:" + e.getRequestId() + "\tmessage:" + e.getErrorMessage());
        }
    }

    @Override
    public void close() throws IOException {

    }

}

