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

package org.apache.seatunnel.connectors.seatunnel.easysearch.dto;

/** the response of bulk EZS by http request */
public class BulkResponse {

    private boolean errors;
    private int took;
    private String response;

    public BulkResponse() {}

    public BulkResponse(boolean errors, int took, String response) {
        this.errors = errors;
        this.took = took;
        this.response = response;
    }

    public boolean isErrors() {
        return errors;
    }

    public void setErrors(boolean errors) {
        this.errors = errors;
    }

    public int getTook() {
        return took;
    }

    public void setTook(int took) {
        this.took = took;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }
}
