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

package org.apache.seatunnel.connectors.seatunnel.timeplus.util;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.timeplus.proton.client.ProtonCredentials;
import com.timeplus.proton.client.ProtonNode;
import com.timeplus.proton.client.ProtonProtocol;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TimeplusUtil {

    public static List<ProtonNode> createNodes(
            String nodeAddress,
            String database,
            String serverTimeZone,
            String username,
            String password,
            Map<String, String> options) {
        return Arrays.stream(nodeAddress.split(","))
                .map(
                        address -> {
                            String[] nodeAndPort = address.split(":", 2);
                            ProtonNode.Builder builder =
                                    ProtonNode.builder()
                                            .host(nodeAndPort[0])
                                            .port(
                                                    ProtonProtocol.HTTP,
                                                    Integer.parseInt(nodeAndPort[1]))
                                            .database(database)
                                            .timeZone(serverTimeZone);
                            if (MapUtils.isNotEmpty(options)) {
                                for (Map.Entry<String, String> entry : options.entrySet()) {
                                    // TODO: need new version of proton-java to support addOption
                                    // builder=builder.addOption(entry.getKey(), entry.getValue());
                                }
                            }

                            if (StringUtils.isNotEmpty(username)
                                    && StringUtils.isNotEmpty(password)) {
                                builder =
                                        builder.credentials(
                                                ProtonCredentials.fromUserAndPassword(
                                                        username, password));
                            }

                            return builder.build();
                        })
                .collect(Collectors.toList());
    }
}
