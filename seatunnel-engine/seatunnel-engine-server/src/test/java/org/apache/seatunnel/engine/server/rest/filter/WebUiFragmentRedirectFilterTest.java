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

package org.apache.seatunnel.engine.server.rest.filter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The Web UI route lives in the URL fragment, so a client that percent-encoded a whole URL sends it
 * as part of the request path and no static resource matches it. The mapping below has to restore
 * the route for that shape only; every other request must stay untouched.
 */
class WebUiFragmentRedirectFilterTest {

    @Test
    void shouldMapEncodedFragmentToTheWebUiRoute() {
        Assertions.assertEquals(
                "/#/overview",
                WebUiFragmentRedirectFilter.redirectLocationFor("GET", "/%23/overview", "", null));
    }

    @Test
    void shouldKeepTheDeepLinkRouteAndItsQueryString() {
        Assertions.assertEquals(
                "/#/jobs/1001?tab=metrics",
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "GET", "/%23/jobs/1001", "", "tab=metrics"));
    }

    @Test
    void shouldKeepTheConfiguredContextPath() {
        Assertions.assertEquals(
                "/seatunnel/#/overview",
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "GET", "/seatunnel/%23/overview", "/seatunnel", null));
    }

    @Test
    void shouldKeepAnEscapedRouteAndItsQuery() {
        Assertions.assertEquals(
                "/#/%E4%B8%AD%E6%96%87/jobs?tab=metrics&pretty=true",
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "GET", "/%23/%E4%B8%AD%E6%96%87/jobs", "", "tab=metrics&pretty=true"));
        Assertions.assertEquals(
                "/#/overview?from=chat%20client",
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "GET", "/%23/overview", "", "from=chat%20client"));
    }

    @Test
    void shouldRefuseRoutesThatAreNotUriReferences() {
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "GET", "/%23/<script>alert(1)</script>", "", null));
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "GET", "/%23/overview", "", "x=%0d%0aInjected:%20yes"));
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "GET", "/%23/overview", "", "x=100%"));
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "GET", "/%23/overview", "", "x=%2"));
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "GET", "/%23/overview", "", "x=%zz"));
        // Non-ASCII digits are digits for Character.digit, but not for a URI escape.
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "GET", "/%23/overview", "", "x=%\u0663\u0663"));
    }

    @Test
    void shouldMapAnEncodedFragmentWithoutRouteToTheWebUiRoot() {
        Assertions.assertEquals(
                "/", WebUiFragmentRedirectFilter.redirectLocationFor("GET", "/%23", "", null));
        Assertions.assertEquals(
                "/?from=chat",
                WebUiFragmentRedirectFilter.redirectLocationFor("GET", "/%23", "", "from=chat"));
    }

    @Test
    void shouldRedirectHeadButNotRequestsWithABody() {
        Assertions.assertEquals(
                "/#/overview",
                WebUiFragmentRedirectFilter.redirectLocationFor("HEAD", "/%23/overview", "", null));
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor("POST", "/%23/overview", "", null));
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "DELETE", "/%23/overview", "", null));
    }

    @Test
    void shouldLeaveRegularWebUiAndRestPathsAlone() {
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor("GET", "/", "", null));
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor("GET", "/index.html", "", null));
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor("GET", "/overview", "", null));
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "GET", "/jobs/checkpoints", "", "jobId=1"));
    }

    @Test
    void shouldNotRedirectEncodedFragmentOutsideTheContextPath() {
        Assertions.assertNull(
                WebUiFragmentRedirectFilter.redirectLocationFor(
                        "GET", "/%23/overview", "/seatunnel", null));
    }
}
