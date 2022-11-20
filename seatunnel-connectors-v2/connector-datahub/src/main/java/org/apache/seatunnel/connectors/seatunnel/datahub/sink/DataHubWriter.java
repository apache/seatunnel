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

package org.apache.seatunnel.connectors.seatunnel.datahub.sink;

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
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * DataHub write class
 */
@Slf4j
public class DataHubWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final DatahubClient dataHubClient;
    private final String project;
    private final String topic;
    private final Integer retryTimes;
    private final SeaTunnelRowType seaTunnelRowType;

    public DataHubWriter(SeaTunnelRowType seaTunnelRowType, String endpoint, String accessId, String accessKey, String project, String topic, Integer timeout, Integer retryTimes) {
        this.dataHubClient = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(new DatahubConfig(endpoint,
                        new AliyunAccount(accessId, accessKey), true))
                .setHttpConfig(new HttpConfig().setCompressType(HttpConfig.CompressType.LZ4)
                        .setConnTimeout(timeout))
                .build();
        this.seaTunnelRowType = seaTunnelRowType;
        this.project = project;
        this.topic = topic;
        this.retryTimes = retryTimes;
    }

    @Override
    public void write(SeaTunnelRow element)  {
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
            int failedRecordCount = result.getFailedRecordCount();
            if (failedRecordCount > 0) {
                log.info("begin to retry for putting failed record");
                if (retry(result.getFailedRecords(), retryTimes, project, topic)) {
                    log.info("retry putting record success");
                } else {
                    log.info("retry putting record failed");
                }
            } else {
                log.info("put record success");
            }
        } catch (DatahubClientException e) {
            log.error("requestId:" + e.getRequestId() + "\tmessage:" + e.getErrorMessage());
        }
    }

    @Override
    public void close() throws IOException {
        //the client does not need to be closed
    }

    private boolean retry(List<RecordEntry> records, int retryNums, String project, String topic) {
        boolean success = false;
        while (retryNums != 0) {
            retryNums = retryNums - 1;
            PutRecordsResult recordsResult = dataHubClient.putRecords(project, topic, records);
            if (recordsResult.getFailedRecordCount() > 0) {
                retry(recordsResult.getFailedRecords(), retryNums, project, topic);
            }
            success = true;
            break;
        }
        return success;
    }
}

