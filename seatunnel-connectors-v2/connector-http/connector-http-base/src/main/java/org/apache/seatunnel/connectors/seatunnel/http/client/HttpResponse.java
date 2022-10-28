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

package org.apache.seatunnel.connectors.seatunnel.http.client;

import org.apache.http.HttpStatus;

import java.io.Serializable;

public class HttpResponse implements Serializable {

    private static final long serialVersionUID = 2168152194164783950L;

    public static final int STATUS_OK = HttpStatus.SC_OK;
    /**
     * response status code
     */
    private int code;

    /**
     * response body
     */
    private String content;

    public HttpResponse() {
    }

    public HttpResponse(int code) {
        this.code = code;
    }

    public HttpResponse(String content) {
        this.content = content;
    }

    public HttpResponse(int code, String content) {
        this.code = code;
        this.content = content;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "HttpClientResult [code=" + code + ", content=" + content + "]";
    }

}
