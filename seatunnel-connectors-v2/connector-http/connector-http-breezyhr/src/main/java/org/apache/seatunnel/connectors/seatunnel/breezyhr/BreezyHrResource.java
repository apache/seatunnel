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
package org.apache.seatunnel.connectors.seatunnel.breezyhr;

/**
 * Company list operations supported by the Breezy HR v3 source.
 *
 * <p>The candidates operation has a position parent while pipelines are a single keyed map.
 */
enum BreezyHrResource {
    POSITIONS("positions"),
    CANDIDATES("candidates"),
    PIPELINES("pipelines");

    final String id;

    BreezyHrResource(String id) {
        this.id = id;
    }

    static BreezyHrResource from(String text) {
        for (BreezyHrResource resource : values()) {
            if (resource.id.equals(text)) {
                return resource;
            }
        }
        throw BreezyHrConfig.invalid("resources", "unknown resource name");
    }
}
