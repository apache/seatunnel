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
    private final String pageParam = "page";
    private final String rowsParam = "rows";

    public PageBaseServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    protected void writeJsonWithPagination(
            HttpServletRequest req, HttpServletResponse resp, JsonArray jsonArray)
            throws IOException {
        int total = jsonArray.size();

        // fetch pagination params, if page exist, then paginate data，pagination data format like:
        // {"data": [], "total": 10}
        Map<String, String> parameterMap = getParameterMap(req);
        if (parameterMap != null && parameterMap.containsKey(pageParam)) {
            int page = Integer.parseInt(parameterMap.get(pageParam));
            int rows =
                    parameterMap.get(rowsParam) != null
                            ? Integer.parseInt(parameterMap.get(rowsParam))
                            : 10;
            int start = (page - 1) * rows;
            if (start > total || page < 1) {
                throw new IllegalArgumentException(
                        page < 1
                                ? "Page number must be greater than 0"
                                : "Page number exceeds total pages");
            }
            JsonArray paginatedArray = new JsonArray();
            jsonArray
                    .values()
                    .subList(start, Math.min(start + rows, total))
                    .forEach(
                            t -> {
                                paginatedArray.add(t);
                            });
            JsonObject paginatedObj = new JsonObject();
            paginatedObj.add("data", paginatedArray);
            paginatedObj.add("total", total);
            writeJson(resp, paginatedObj);
        } else {
            writeJson(resp, jsonArray);
        }
    }

    /** A requested page. Absent when the caller sent no {@code page} parameter. */
    protected static final class PageParams {
        private final int page;
        private final int rows;

        private PageParams(int page, int rows) {
            this.page = page;
            this.rows = rows;
        }

        public int getRows() {
            return rows;
        }

        public int getStart() {
            return (page - 1) * rows;
        }
    }

    /**
     * Reads the pagination parameters, or returns {@code null} when the request does not ask for a
     * page. For endpoints that can slice at the source, which pay per returned row rather than per
     * stored row and so cannot use {@link #writeJsonWithPagination}.
     */
    protected PageParams pageParams(HttpServletRequest req) {
        Map<String, String> parameterMap = getParameterMap(req);
        if (parameterMap == null || !parameterMap.containsKey(pageParam)) {
            return null;
        }
        int page = Integer.parseInt(parameterMap.get(pageParam));
        int rows =
                parameterMap.get(rowsParam) != null
                        ? Integer.parseInt(parameterMap.get(rowsParam))
                        : 10;
        if (page < 1) {
            throw new IllegalArgumentException("Page number must be greater than 0");
        }
        return new PageParams(page, rows);
    }

    /** Rejects a page that starts past the end of the result set. */
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
