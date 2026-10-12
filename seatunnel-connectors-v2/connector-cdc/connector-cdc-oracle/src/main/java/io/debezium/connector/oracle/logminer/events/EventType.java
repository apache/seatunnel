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

package io.debezium.connector.oracle.logminer.events;

/**
 * Represents all supported event types that are loaded from Oracle LogMiner.
 *
 * @author Chris Cranford
 */
public enum EventType {
    INSERT(1),
    DELETE(2),
    UPDATE(3),
    DDL(5),
    START(6),
    COMMIT(7),
    SELECT_LOB_LOCATOR(9),
    LOB_WRITE(10),
    LOB_TRIM(11),
    LOB_ERASE(29),
    MISSING_SCN(34),
    ROLLBACK(36),
    XML_BEGIN(68),
    XML_WRITE(70),
    XML_END(71),
    UNSUPPORTED(255);

    private static EventType[] types = new EventType[256];

    static {
        for (EventType option : EventType.values()) {
            types[option.getValue()] = option;
        }
    }

    private int value;

    EventType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    /**
     * Resolve an EventType from a numeric event type operation code.
     *
     * @param value the operation code
     * @return the event type, will be {@link #UNSUPPORTED} if the code is not supported.
     */
    public static EventType from(int value) {
        return value < types.length ? types[value] : EventType.UNSUPPORTED;
    }
}
