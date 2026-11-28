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
package org.apache.seatunnel.connectors.seatunnel.aviationstack;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The Aviationstack directory routes this source can fetch.
 *
 * <p>Catalog selection is resolved before constructing requests so options cannot supply arbitrary
 * paths or leak the query credential to a different endpoint.
 */
enum AviationstackResource {
    AIRPORTS("airports"),
    AIRLINES("airlines"),
    AIRPLANES("airplanes"),
    AIRCRAFT_TYPES("aircraft_types"),
    CITIES("cities"),
    COUNTRIES("countries"),
    TAXES("taxes");

    final String name;

    AviationstackResource(String name) {
        this.name = name;
    }

    static AviationstackResource from(String name) {
        return Arrays.stream(values())
                .filter(resource -> resource.name.equals(name))
                .findFirst()
                .orElseThrow(
                        () ->
                                AviationstackConfig.invalid(
                                        "resources", "contains an unknown catalog"));
    }

    static List<String> names() {
        return Arrays.stream(values()).map(resource -> resource.name).collect(Collectors.toList());
    }
}
