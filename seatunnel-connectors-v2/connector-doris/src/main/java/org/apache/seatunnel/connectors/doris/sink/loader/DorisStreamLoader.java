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

package org.apache.seatunnel.connectors.doris.sink.loader;

import static org.apache.seatunnel.connectors.doris.common.DorisConstants.DORIS_SUCCESS_STATUS;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.doris.common.DorisConstants;
import org.apache.seatunnel.connectors.doris.common.DorisOptions;
import org.apache.seatunnel.connectors.doris.common.HttpClient;
import org.apache.seatunnel.connectors.doris.model.response.ResponseBody;
import org.apache.seatunnel.connectors.doris.sink.DorisLoader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Stream load
 * Created 2022/8/01
 */
public class DorisStreamLoader implements DorisLoader<String> {
    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoader.class);

    private final DorisOptions dorisOptions;
    private final String loadUrl;
    private final List<Header> defaultHeaders;
    private final HttpClient httpClient;

    public DorisStreamLoader(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
        this.loadUrl = buildLoadUrl(dorisOptions);
        this.defaultHeaders = buildDefaultHeader(dorisOptions);
        this.httpClient = new HttpClient(dorisOptions);
    }

    private List<Header> buildDefaultHeader(DorisOptions dorisOptions) {
        List<Header> headers = Lists.newArrayList();
        if (Objects.nonNull(dorisOptions.getParameters())) {
            Properties properties = dorisOptions.getParameters();
            headers = properties.keySet()
                .stream()
                .map(key -> new BasicHeader((String) key, properties.getProperty((String) key)))
                .collect(Collectors.toList());
        }

        addDefaultHeaders(headers);
        return headers;
    }

    private void addDefaultHeaders(List<Header> headers) {
        String username = dorisOptions.getUsername();
        String password = dorisOptions.getPassword();
        String token = Base64.getEncoder().encodeToString(String.format("%s:%s", username, password)
            .getBytes(StandardCharsets.UTF_8));
        headers.add(new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + token));
        headers.add(new BasicHeader(HttpHeaders.EXPECT, "100-continue"));
        headers.add(new BasicHeader(DorisConstants.DORIS_STRIP_OUTER_ARRAY_OPTION, DorisConstants.DORIS_STRIP_OUTER_ARRAY_OPTION_VALUE));
        headers.add(new BasicHeader(DorisConstants.DORIS_FORMAT_OPTION, DorisConstants.DORIS_FORMAT_OPTION_VALUE));
    }

    @Override
    public int load(String request, String label) {
        RequestBuilder requestBuilder = RequestBuilder
            .put(loadUrl)
            .setCharset(Charset.defaultCharset())
            .setEntity(new StringEntity(request, ContentType.TEXT_PLAIN));

        defaultHeaders.forEach(requestBuilder::addHeader);
        requestBuilder.addHeader(new BasicHeader(DorisConstants.DORIS_LABEL_OPTION, label));
        HttpUriRequest putRequest = requestBuilder.build();

        String entity = httpClient.executeAndGetEntity(putRequest, true);
        ResponseBody responseBody = JsonUtils.parseObject(entity, new TypeReference<ResponseBody>() {
        });
        if (Objects.isNull(responseBody)) {
            throw new RuntimeException("Empty response.");

        } else {
            if (!DORIS_SUCCESS_STATUS.contains(responseBody.getStatus())
                || responseBody.getNumberTotalRows() != responseBody.getNumberLoadedRows()) {
                String errMsg = String.format("stream load error: %s, see more in %s",
                    responseBody.getMessage(), responseBody.getErrorURL());
                throw new RuntimeException(errMsg);
            }
        }
        return responseBody.getTxnId();
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    private static String buildLoadUrl(DorisOptions dorisOptions) {
        String address = dorisOptions.getFeAddresses();
        String database = dorisOptions.getDatabaseName();
        String table = dorisOptions.getTableName();
        return String.format(DorisConstants.LOAD_URL_TEMPLATE, address, database, table);
    }
}
