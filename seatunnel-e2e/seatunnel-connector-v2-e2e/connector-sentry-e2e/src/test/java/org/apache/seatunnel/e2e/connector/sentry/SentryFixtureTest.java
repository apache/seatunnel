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

package org.apache.seatunnel.e2e.connector.sentry;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockserver.configuration.Configuration;
import org.mockserver.logging.MockServerLogger;
import org.mockserver.matchers.HttpRequestPropertiesMatcher;

class SentryFixtureTest {
    @Test
    void firstPageMatcherExcludesCursorRequests() {
        HttpRequestPropertiesMatcher matcher =
                new HttpRequestPropertiesMatcher(new Configuration(), new MockServerLogger());
        matcher.apply(SentryIT.firstRequest());
        Assertions.assertTrue(matcher.matches(null, SentryIT.request()));
        Assertions.assertFalse(
                matcher.matches(
                        null, SentryIT.request().withQueryStringParameter("cursor", "0:2:0")));
    }
}
