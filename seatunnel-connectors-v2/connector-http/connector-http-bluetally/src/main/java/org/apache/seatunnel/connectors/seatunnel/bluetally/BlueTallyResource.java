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
package org.apache.seatunnel.connectors.seatunnel.bluetally;

/**
 * Identifies the two supported BlueTally v1 list operations.
 *
 * <p>Each resource has its own offset, ordering checks and provider record identities.
 */
enum BlueTallyResource {
    ASSETS("assets"),
    EMPLOYEES("employees");

    final String path;

    BlueTallyResource(String path) {
        this.path = path;
    }

    static BlueTallyResource from(String name) {
        for (BlueTallyResource known : values()) {
            if (known.path.equals(name)) {
                return known;
            }
        }
        throw BlueTallyConfig.invalid("resources", "expected assets or employees");
    }
}
