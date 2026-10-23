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

package org.apache.seatunnel.engine.server.rest.servlet;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Map;

public class PageBaseServlet extends BaseServlet {
    private static final int DEFAULT_ROWS = 10;

    private final String pageParam = "page";
    private final String rowsParam = "rows";

    public PageBaseServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    /**
     * Paginates an already-built array, writing {@code {"data": [], "total": n}} when a page was
     * requested and the bare array otherwise.
     *
     * <p>Endpoints that can slice at the source should do so and use {@link #writeJsonPage}
     * instead, because this method has already paid the cost of building every row before
     * discarding all but one page.
     */
    protected void writeJsonWithPagination(
            HttpServletRequest req, HttpServletResponse resp, JsonArray jsonArray)
            throws IOException {
        int total = jsonArray.size();

        PageParams pageParams = pageParams(req);
        if (pageParams == null) {
            writeJson(resp, jsonArray);
            return;
        }
        checkPageInRange(pageParams, total);

        int start = pageParams.getStart();
        JsonArray paginatedArray = new JsonArray();
        jsonArray
                .values()
                .subList(start, Math.min(start + pageParams.getRows(), total))
                .forEach(
                        t -> {
                            paginatedArray.add(t);
                        });
        writeJsonPage(resp, paginatedArray, total);
    }

    /**
     * A requested page. Absent when the caller sent no {@code page} parameter.
     *
     * <p>The start offset is computed and validated in {@link #pageParams(HttpServletRequest)}
     * rather than derived on demand, so that no accessor here can throw.
     */
    protected static final class PageParams {
        private final int rows;
        private final int start;

        private PageParams(int rows, int start) {
            this.rows = rows;
            this.start = start;
        }

        public int getRows() {
            return rows;
        }

        public int getStart() {
            return start;
        }
    }

    /**
     * Reads and validates the pagination parameters, or returns {@code null} when the request does
     * not ask for a page.
     *
     * <p>This is the single definition of pagination input handling. {@link
     * #writeJsonWithPagination} routes through it as well, so every paginated endpoint accepts and
     * rejects the same input.
     *
     * @throws IllegalArgumentException if the values are unparseable, non-positive, or describe an
     *     offset too large to address, all of which the servlet layer reports as a 400
     */
    protected PageParams pageParams(HttpServletRequest req) {
        Map<String, String> parameterMap = getParameterMap(req);
        if (parameterMap == null || !parameterMap.containsKey(pageParam)) {
            return null;
        }
        int page = positiveIntParam(parameterMap.get(pageParam), pageParam);
        String rowsValue = parameterMap.get(rowsParam);
        int rows = rowsValue != null ? positiveIntParam(rowsValue, rowsParam) : DEFAULT_ROWS;

        // widened before multiplying: page and rows are both caller controlled, and an int
        // overflow here can wrap to a small positive offset that passes checkPageInRange and
        // silently serves the wrong page
        long start = (long) (page - 1) * rows;
        if (start > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Page number exceeds total pages");
        }
        return new PageParams(rows, (int) start);
    }

    private int positiveIntParam(String value, String name) {
        int parsed;
        try {
            parsed = Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            // the JDK message quotes the raw input without naming the parameter
            throw new IllegalArgumentException(
                    "Parameter '" + name + "' must be an integer, but was: " + value);
        }
        if (parsed < 1) {
            throw new IllegalArgumentException("Parameter '" + name + "' must be greater than 0");
        }
        return parsed;
    }

    /**
     * Rejects a page that starts past the end of the result set.
     *
     * <p>A page starting exactly at {@code total} is allowed and yields an empty page. That is
     * deliberate: it is the behaviour the pre-existing pagination has always had, and changing it
     * would alter the response for callers that walk to the end of a listing.
     */
    protected void checkPageInRange(PageParams pageParams, int total) {
        if (pageParams.getStart() > total) {
            throw new IllegalArgumentException("Page number exceeds total pages");
        }
    }

    /** Writes the {@code {"data": [], "total": n}} envelope for an already-sliced page. */
    protected void writeJsonPage(HttpServletResponse resp, JsonArray pageData, int total)
            throws IOException {
        JsonObject paginatedObj = new JsonObject();
        paginatedObj.add("data", pageData);
        paginatedObj.add("total", total);
        writeJson(resp, paginatedObj);
    }
}
