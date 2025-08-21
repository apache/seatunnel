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

package org.apache.seatunnel.api.metalake;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;

public class GravitinoClient implements MetalakeClient {
    private final String metalakeUrl;

    public GravitinoClient(String metalakeUrl) {
        this.metalakeUrl = metalakeUrl;
    }

    @Override
    public String getType() {
        return "gravitino";
    }

    @Override
    public JsonNode getMetaInfo(String sourceId) throws IOException {
        OkHttpClient client = new OkHttpClient().newBuilder().build();
        MediaType mediaType = MediaType.parse("text/plain");
        RequestBody body = RequestBody.create(mediaType, "");
        Request request =
                new Request.Builder()
                        .url(this.metalakeUrl + sourceId)
                        .method("GET", body)
                        .addHeader("Accept", "application/vnd.gravitino.v1+json")
                        // .addHeader("Authorization", "Bearer <TOKEN>")
                        .build();
        Response response = client.newCall(request).execute();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(response.body().byteStream());
        JsonNode propertiesNode = rootNode.get("properties");
        return propertiesNode;
    }
}
