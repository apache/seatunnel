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

package org.apache.seatunnel.scheduler.dolphinscheduler.utils;

import static org.apache.seatunnel.server.common.SeatunnelErrorEnum.HTTP_REQUEST_FAILED;
import static com.google.common.base.Preconditions.checkState;

import org.apache.seatunnel.server.common.SeatunnelException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Connection;
import org.jsoup.Jsoup;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class HttpUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @SuppressWarnings("MagicNumber")
    public static Map<String, String> createParamMap(Object... elements) {
        if (elements == null || elements.length == 0 || elements.length % 2 == 1) {
            throw new IllegalArgumentException("params length must be even!");
        }
        Map<String, String> paramMap = Maps.newHashMapWithExpectedSize(elements.length / 2);
        for (int i = 0; i < elements.length / 2; i++) {
            Object key = elements[2 * i];
            Object value = elements[2 * i + 1];
            if (key == null) {
                continue;
            }

            paramMap.put(key.toString(), value == null ? "" : value.toString());
        }

        return paramMap;
    }

    public static Builder builder() {
        return new Builder();
    }

    @SuppressWarnings("MagicNumber")
    public static class Builder {
        private Connection connection;
        private String url;
        private Optional<Map<String, String>> data = Optional.empty();
        private Optional<String> requestBody = Optional.empty();
        private Optional<Integer> maxBodySize = Optional.of(2147483647);
        private Optional<String> postDataCharset = Optional.of("UTF-8");
        private Optional<Boolean> ignoreContentType = Optional.of(true);
        private Optional<Integer> timeout = Optional.of(30000);
        private Optional<Map<String, String>> headers;
        private Optional<Connection.Method> method = Optional.empty();

        private Optional<Type> type;

        public Builder() {
            HashMap<String, String> defaultMap = Maps.newHashMap();
            defaultMap.put("Content-Type", "application/json; charset=UTF-8");
            headers = Optional.of(defaultMap);
        }

        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder withData(Map<String, String> data) {
            this.data = Optional.ofNullable(data);
            return this;
        }

        public Builder withRequestBody(String requestBody) {
            this.requestBody = Optional.ofNullable(requestBody);
            return this;
        }

        public Builder withMaxBodySize(int maxBodySize) {
            this.maxBodySize = Optional.of(maxBodySize);
            return this;
        }

        public Builder withPostDataCharset(String postDataCharset) {
            this.postDataCharset = Optional.ofNullable(postDataCharset);
            return this;
        }

        public Builder withIgnoreContentType(boolean ignoreContentType) {
            this.ignoreContentType = Optional.of(ignoreContentType);
            return this;
        }

        public Builder withTimeout(int timeout) {
            this.timeout = Optional.of(timeout);
            return this;
        }

        public Builder withHeaders(Map<String, String> headers) {
            this.headers = Optional.ofNullable(headers);
            return this;
        }

        public Builder withMethod(Connection.Method method) {
            this.method = Optional.ofNullable(method);
            return this;
        }

        public Builder withToken(String tokenKey, String tokenValue) {
            this.headers.ifPresent(map -> map.put(tokenKey, tokenValue));
            return this;
        }

        private Connection build() {
            checkState(!Strings.isNullOrEmpty(url), "request url is empty");
            connection = Jsoup.connect(url);
            data.ifPresent(connection::data);
            requestBody.ifPresent(connection::requestBody);
            maxBodySize.ifPresent(connection::maxBodySize);
            postDataCharset.ifPresent(connection::postDataCharset);
            ignoreContentType.ifPresent(connection::ignoreContentType);
            timeout.ifPresent(connection::timeout);
            headers.ifPresent(connection::headers);
            method.ifPresent(connection::method);

            return connection;
        }

        public <T> T execute(Class<T> type) {
            this.build();
            try {
                Connection.Response response = connection.execute();
                return MAPPER.readValue(response.body(), type);
            } catch (IOException e) {
                log.error("Request url {} failed", this.url, e);
                throw new SeatunnelException(HTTP_REQUEST_FAILED, this.url);
            }
        }

        public void execute() throws IOException {
            this.build();
            try {
                connection.execute();
            } catch (IOException e) {
                log.error("Request url {} failed", this.url, e);
                throw new SeatunnelException(HTTP_REQUEST_FAILED, this.url);
            }
        }
    }
}
