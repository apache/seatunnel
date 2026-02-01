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

package org.apache.seatunnel.trace.collector.http;

import com.sun.net.httpserver.HttpExchange;

final class TraceAuth {
    private static final String TOKEN_HEADER = "X-Seatunnel-Token";

    private final String token;

    TraceAuth(String token) {
        this.token = token;
    }

    boolean isEnabled() {
        return token != null && !token.isEmpty();
    }

    boolean isAuthorized(HttpExchange exchange) {
        if (!isEnabled()) {
            return true;
        }
        String got = exchange.getRequestHeaders().getFirst(TOKEN_HEADER);
        return token.equals(got);
    }
}
