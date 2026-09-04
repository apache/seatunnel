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

package org.apache.seatunnel.connectors.seatunnel.snmp.source;

final class SnmpRecord {

    private final String oid;
    private final String value;
    private final String valueType;

    SnmpRecord(String oid, String value, String valueType) {
        this.oid = oid;
        this.value = value;
        this.valueType = valueType;
    }

    String getOid() {
        return oid;
    }

    String getValue() {
        return value;
    }

    String getValueType() {
        return valueType;
    }
}
