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

package org.apache.seatunnel.connectors.doris.client;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DorisFlushTuple {
    private String label;
    private Long bytes = 0L;
    private List<byte[]> rows;
    private boolean eof;

    public DorisFlushTuple(String label) {
        this.label = label;
        this.rows = new ArrayList<>();
    }

    public DorisFlushTuple(String label, Long bytes, List<byte[]> rows) {
        this.label = label;
        this.bytes = bytes;
        this.rows = rows;
    }

    public DorisFlushTuple asEOF() {
        eof = true;
        return this;
    }

    public void addToBuffer(byte[] bts) {
        rows.add(bts);
        bytes += bts.length;
    }
}
