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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.utils.LogUtil;

import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Slf4j
public class BaseLogService extends BaseService {

    public BaseLogService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BASIC_PREFIX = "Basic ";

    /** Get configuration log path */
    public String getLogPath() {
        try {
            return LogUtil.getLogPath();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            log.error("Get log path error,{}", ExceptionUtils.getMessage(e));
            return null;
        }
    }

    /**
     * Send a simple HTTP GET request.
     *
     * @param urlString url
     * @return the response body as a string, or {@code null} if the request failed
     */
    protected String sendGet(String urlString) {
        return sendGet(urlString, null, null);
    }

    /**
     * Send GET request (optionally with Basic Auth)
     *
     * @param urlString url
     * @param user username, nullable
     * @param pass password, nullable
     * @return the response body as a string, or {@code null} if the request failed
     */
    protected String sendGet(String urlString, String user, String pass) {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(urlString).openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);

            // Basic Auth
            if (user != null && pass != null) {
                String auth = user + ":" + pass;
                String token =
                        Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
                connection.setRequestProperty(AUTHORIZATION_HEADER, BASIC_PREFIX + token);
            }

            connection.connect();

            int code = connection.getResponseCode();
            if (code == HttpURLConnection.HTTP_OK) {
                return readResponseBody(connection.getInputStream());
            } else {
                log.warn("GET {} -> HTTP {}", urlString, code);
                drainErrorStream(connection);
            }
        } catch (IOException e) {
            log.error("Send GET failed: url={}, err={}", urlString, ExceptionUtils.getMessage(e));
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        return null;
    }

    private String readResponseBody(InputStream is) throws IOException {
        try (InputStream input = is;
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {

            byte[] buf = new byte[4096];
            int len;
            while ((len = input.read(buf)) != -1) {
                output.write(buf, 0, len);
            }
            return output.toString(StandardCharsets.UTF_8.name());
        }
    }

    private void drainErrorStream(HttpURLConnection connection) throws IOException {
        try (InputStream err = connection.getErrorStream()) {
            if (err != null) {
                byte[] buffer = new byte[1024];
                while (err.read(buffer) != -1) {
                    // discard
                }
            }
        }
    }

    public String getLogParam(String uri, String contextPath) {
        uri = uri.substring(uri.indexOf(contextPath) + contextPath.length());
        uri = StringUtil.stripTrailingSlash(uri).substring(1);
        int indexEnd = uri.indexOf('/');
        if (indexEnd != -1) {
            return uri.substring(indexEnd + 1);
        }
        return "";
    }

    protected String buildLogLink(String href, String name) {
        return "<li><a href=\"" + href + "\">" + name + "</a></li>\n";
    }

    protected String buildWebSiteContent(StringBuffer logLink) {
        return "<html><head><title>Seatunnel log</title></head>\n"
                + "<body>\n"
                + " <h2>Seatunnel log</h2>\n"
                + " <ul>\n"
                + logLink.toString()
                + " </ul>\n"
                + "</body></html>";
    }
}
