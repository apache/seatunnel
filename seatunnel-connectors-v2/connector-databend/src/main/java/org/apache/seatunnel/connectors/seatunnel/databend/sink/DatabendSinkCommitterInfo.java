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

package org.apache.seatunnel.connectors.seatunnel.databend.sink;

import java.io.Serializable;

public class DatabendSinkCommitterInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    // CDC related fields
    private String rawTableName;
    private String streamName;

    public DatabendSinkCommitterInfo() {
        // Default constructor
    }

    public DatabendSinkCommitterInfo(String rawTableName, String streamName) {
        this.rawTableName = rawTableName;
        this.streamName = streamName;
    }

    public String getRawTableName() {
        return rawTableName;
    }

    public void setRawTableName(String rawTableName) {
        this.rawTableName = rawTableName;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    @Override
    public String toString() {
        return "DatabendSinkCommitterInfo{"
                + "rawTableName='"
                + rawTableName
                + '\''
                + ", streamName='"
                + streamName
                + '\''
                + '}';
    }
}
