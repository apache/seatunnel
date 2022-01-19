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

package org.apache.seatunnel.flink.sink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * doris streamLoad
 */
public class DorisStreamLoad implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DorisStreamLoad.class);
    private static final List<String> DORIS_SUCCESS_STATUS = Arrays.asList("Success", "Publish Timeout");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load?";

    private final String loadUrlStr;
    private final String authEncoding;
    private final Properties streamLoadProp;

    public DorisStreamLoad(String hostPort, String db, String tbl, String user, String passwd, Properties streamLoadProp) {
        this.loadUrlStr = String.format(LOAD_URL_PATTERN, hostPort, db, tbl);
        this.authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        this.streamLoadProp = streamLoadProp;
    }

    public Properties getStreamLoadProp() {
        return streamLoadProp;
    }

    private HttpURLConnection getConnection(String urlStr, String label) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        conn.addRequestProperty("Expect", "100-continue");
        conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        conn.addRequestProperty("label", label);
        for (Map.Entry<Object, Object> entry : streamLoadProp.entrySet()) {
            conn.addRequestProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        conn.setDoOutput(true);
        conn.setDoInput(true);
        return conn;
    }

    public void load(String value) {
        LoadResponse loadResponse = loadBatch(value);
        LOGGER.info("Streamload Response:{}", loadResponse);
        if (loadResponse.status != HttpResponseStatus.OK.code()) {
            throw new RuntimeException("stream load error: " + loadResponse.respContent);
        } else {
            try {
                RespContent respContent = OBJECT_MAPPER.readValue(loadResponse.respContent, RespContent.class);
                if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())
                        || respContent.getNumberTotalRows() != respContent.getNumberLoadedRows()) {
                    String errMsg = String.format("stream load error: %s, see more in %s", respContent.getMessage(), respContent.getErrorURL());
                    throw new RuntimeException(errMsg);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private LoadResponse loadBatch(String data) {
        String formatDate = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String label = String.format("flink_sink_%s_%s", formatDate,
                UUID.randomUUID().toString().replaceAll("-", ""));

        HttpURLConnection feConn = null;
        HttpURLConnection beConn = null;
        try {
            // build request and send to fe
            feConn = getConnection(loadUrlStr, label);
            int status = feConn.getResponseCode();
            // fe send back http response code TEMPORARY_REDIRECT 307 and new be location
            if (status != HttpResponseStatus.TEMPORARY_REDIRECT.code()) {
                throw new Exception("status is not TEMPORARY_REDIRECT 307, status: " + status);
            }
            String location = feConn.getHeaderField("Location");
            if (location == null) {
                throw new Exception("redirect location is null");
            }
            // build request and send to new be location
            beConn = getConnection(location, label);
            // send data to be
            BufferedOutputStream bos = new BufferedOutputStream(beConn.getOutputStream());
            bos.write(data.getBytes());
            bos.close();

            // get respond
            status = beConn.getResponseCode();
            String respMsg = beConn.getResponseMessage();
            InputStream stream = (InputStream) beConn.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(stream));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            return new LoadResponse(status, respMsg, response.toString());
        } catch (Exception e) {
            String err = "failed to stream load data with label:" + label;
            LOGGER.warn(err, e);
            throw new RuntimeException("stream load error: " + err);
        } finally {
            if (feConn != null) {
                feConn.disconnect();
            }
            if (beConn != null) {
                beConn.disconnect();
            }
        }
    }

    public static class LoadResponse {
        public int status;
        public String respMsg;
        public String respContent;

        public LoadResponse(int status, String respMsg, String respContent) {
            this.status = status;
            this.respMsg = respMsg;
            this.respContent = respContent;
        }

        @Override
        public String toString() {
            return "status: " + status +
                    ", resp msg: " + respMsg +
                    ", resp content: " + respContent;
        }
    }
}
