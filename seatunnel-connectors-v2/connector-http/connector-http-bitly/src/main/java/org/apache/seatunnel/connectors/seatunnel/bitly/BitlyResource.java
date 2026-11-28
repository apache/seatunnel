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
package org.apache.seatunnel.connectors.seatunnel.bitly;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Public names of the Bitly resources that the source can emit.
 *
 * <p>Discovery of organization, group and link IDs is independent of emission settings.
 */
enum BitlyResource {
    BSDS("bsds"),
    CAMPAIGNS("campaigns"),
    CHANNELS("channels"),
    ORGANIZATIONS("organizations"),
    ORGANIZATION_COUNTS("organization_shorten_counts"),
    GROUPS("groups"),
    GROUP_PREFERENCES("group_preferences"),
    GROUP_COUNTS("group_shorten_counts"),
    BITLINKS("bitlinks"),
    CLICKS("bitlink_clicks"),
    QR_CODES("qr_codes");

    final String name;

    BitlyResource(String name) {
        this.name = name;
    }

    static List<String> names() {
        return Arrays.stream(values())
                .map(value -> value.name)
                .collect(Collectors.toUnmodifiableList());
    }

    static BitlyResource from(String name) {
        for (BitlyResource value : values()) {
            if (value.name.equals(name)) {
                return value;
            }
        }
        throw BitlyConfig.invalid("resources", "select only documented Bitly resources");
    }
}
