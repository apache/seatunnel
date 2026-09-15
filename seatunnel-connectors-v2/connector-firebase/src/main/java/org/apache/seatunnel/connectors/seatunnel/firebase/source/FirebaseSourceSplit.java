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

package org.apache.seatunnel.connectors.seatunnel.firebase.source;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.Getter;

import java.util.Collections;
import java.util.List;

public class FirebaseSourceSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String splitId;
    @Getter private final String path;
    @Getter private final List<String> keys; // Sub-keys to fetch if using key-based splitting

    public FirebaseSourceSplit(String splitId, String path) {
        this.splitId = splitId;
        this.path = path;
        this.keys = Collections.emptyList();
    }

    public FirebaseSourceSplit(String splitId, String path, List<String> keys) {
        this.splitId = splitId;
        this.path = path;
        this.keys = keys;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return splitId + " " + path + " keys : " + keys.toString();
    }
}
