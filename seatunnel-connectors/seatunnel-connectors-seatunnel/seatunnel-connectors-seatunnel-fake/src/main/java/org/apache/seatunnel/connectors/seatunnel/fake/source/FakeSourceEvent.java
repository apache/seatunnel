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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.api.source.SourceEvent;

public class FakeSourceEvent implements SourceEvent {

    private final String name;
    private final int age;
    private final long timestamp;

    public FakeSourceEvent(String name, int age, long timestamp) {
        this.name = name;
        this.age = age;
        this.timestamp = timestamp;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
