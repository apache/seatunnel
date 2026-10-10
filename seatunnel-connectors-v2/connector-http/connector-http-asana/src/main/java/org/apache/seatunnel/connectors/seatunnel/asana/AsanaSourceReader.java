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

package org.apache.seatunnel.connectors.seatunnel.asana;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class AsanaSourceReader extends HttpSourceReader {

    public AsanaSourceReader(
            HttpParameter httpParameter, SingleSplitReaderContext readerContext,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            JsonField jsonField, String contentField, PageInfo pageInfo) {
        super(httpParameter, readerContext, deserializationSchema, jsonField, contentField, pageInfo);
    }

    @Override
    protected HttpResponse executeRequest() throws Exception {
        int maxRetries = httpParameter.getRetry();
        long waitMillis = httpParameter.getRetryBackoffMultiplierMillis();
        long maxWaitMillis = httpParameter.getRetryBackoffMaxMillis();
        int attempt = 0;
        while (true) {
            HttpResponse response = super.executeRequest();
            int code = response.getCode();
            boolean retryable = code == 429 || (code >= 500 && code < 600);
            if (!retryable || attempt >= maxRetries) {
                return response;
            }
            attempt++;
            log.warn("Asana returned HTTP {}; retry {}/{} in {} ms",
                    code, attempt, maxRetries, waitMillis);
            Thread.sleep(waitMillis);
            waitMillis = Math.min(waitMillis * 2, maxWaitMillis);
        }
    }
}
