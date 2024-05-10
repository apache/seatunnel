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

package org.apache.seatunnel.connectors.doris.rest.models;

import org.apache.seatunnel.connectors.doris.rest.RestService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class RestServiceTest {

    @Test
    void testRandomEndpoint() {

        List<String> list =
                Arrays.asList(
                        "fe_host1:fe_http_port1",
                        "fe_host2:fe_http_port2",
                        "fe_host3:fe_http_port3",
                        "fe_host4:fe_http_port4",
                        "fe_host5:fe_http_port5");

        boolean hasDifferentAddress = false;
        for (int i = 0; i < 5; i++) {
            Set<String> addresses =
                    list.stream()
                            .map(address -> RestService.randomEndpoint(String.join(",", list), log))
                            .collect(Collectors.toSet());
            hasDifferentAddress = addresses.size() > 1;
        }
        Assertions.assertTrue(hasDifferentAddress);
    }
}
