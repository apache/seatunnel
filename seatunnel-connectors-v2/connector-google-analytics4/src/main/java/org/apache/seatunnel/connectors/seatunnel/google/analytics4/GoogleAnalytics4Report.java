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

package org.apache.seatunnel.connectors.seatunnel.google.analytics4;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

final class GoogleAnalytics4Report {
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private final GoogleAnalytics4Config config;
    private long expectedCount = -1;
    private List<String> previousKey;
    private String currency;
    private String timeZone;

    GoogleAnalytics4Report(GoogleAnalytics4Config config) {
        this.config = config;
    }

    byte[] request(long offset) throws IOException {
        ObjectNode request = MAPPER.createObjectNode();
        ArrayNode dimensions = request.putArray("dimensions");
        ArrayNode order = request.putArray("orderBys");
        for (String name : config.getDimensions()) {
            dimensions.addObject().put("name", name);
            order.addObject()
                    .putObject("dimension")
                    .put("dimensionName", name)
                    .put("orderType", "ALPHANUMERIC");
        }
        ArrayNode metrics = request.putArray("metrics");
        config.getMetrics().forEach(name -> metrics.addObject().put("name", name));
        request.putArray("dateRanges")
                .addObject()
                .put("startDate", config.getStartDate())
                .put("endDate", config.getEndDate());
        request.put("offset", Long.toString(offset));
        request.put("limit", Integer.toString(config.getPageSize()));
        request.put("keepEmptyRows", true);
        request.put("returnPropertyQuota", true);
        return MAPPER.writeValueAsBytes(request);
    }

    Page parse(byte[] bytes, long offset) throws IOException {
        JsonNode response;
        try {
            response = MAPPER.readTree(bytes);
        } catch (IOException | RuntimeException e) {
            throw failure("invalid JSON response");
        }
        if (response == null || !response.isObject() || response.has("error")) {
            throw failure("invalid report response");
        }
        validateMetadata(response.path("metadata"));
        headers(response.path("dimensionHeaders"), config.getDimensions(), false);
        headers(response.path("metricHeaders"), config.getMetrics(), true);
        // Protobuf JSON omits zero-valued scalar fields and empty repeated fields.
        long count = response.has("rowCount") ? count(response.get("rowCount"), "rowCount") : 0;
        if (count > config.getMaxRows() || (expectedCount >= 0 && expectedCount != count)) {
            throw failure("rowCount changed or exceeds max_report_rows; restart the entire report");
        }
        if (config.getDimensions().isEmpty() && count > 1) {
            throw failure("a report without dimensions must contain at most one row");
        }
        JsonNode rows = response.path("rows");
        if (!rows.isMissingNode() && !rows.isArray()) {
            throw failure("rows must be an array");
        }
        if (offset > count
                || rows.size() > config.getPageSize()
                || offset + rows.size() > count
                || (offset < count && rows.size() == 0)) {
            throw failure("inconsistent page length or rowCount; no page was skipped");
        }
        List<SeaTunnelRow> result = new ArrayList<>(rows.size());
        for (JsonNode row : rows) {
            JsonNode dims = row.path("dimensionValues");
            JsonNode metrics = row.path("metricValues");
            values(dims, config.getDimensions().size());
            values(metrics, config.getMetrics().size());
            Object[] fields =
                    new Object[config.getDimensions().size() + config.getMetrics().size()];
            List<String> key = new ArrayList<>();
            int field = 0;
            for (JsonNode dim : dims) {
                String value = text(dim.path("value"), "dimension value");
                key.add(value);
                fields[field++] = value;
            }
            if (previousKey != null && compare(previousKey, key) >= 0) {
                throw failure("duplicate or out-of-order dimension tuple; report may have changed");
            }
            for (int i = 0; i < config.getMetrics().size(); i++) {
                String value = text(metrics.get(i).path("value"), "metric value");
                try {
                    if ("TYPE_INTEGER".equals(config.getMetricTypes().get(i))) {
                        if (!value.matches("-?[0-9]+")) {
                            throw new NumberFormatException();
                        }
                        fields[field++] = Long.parseLong(value);
                    } else {
                        if (!value.matches(
                                "-?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?")) {
                            throw new NumberFormatException();
                        }
                        double number = Double.parseDouble(value);
                        if (!Double.isFinite(number)) {
                            throw new NumberFormatException();
                        }
                        fields[field++] = number;
                    }
                } catch (NumberFormatException e) {
                    throw failure("metric value is invalid or outside the configured numeric type");
                }
            }
            SeaTunnelRow output = new SeaTunnelRow(fields);
            output.setTableId(config.getTable().getTableId().toTablePath().toString());
            result.add(output);
            previousKey = key;
        }
        expectedCount = count;
        boolean finished = offset + result.size() == count;
        if (!finished) {
            checkQuota(response.path("propertyQuota"));
        }
        return new Page(result, finished);
    }

    private void headers(JsonNode headers, List<String> names, boolean metric) throws IOException {
        if ((names.isEmpty() && headers.isMissingNode())) {
            return;
        }
        if (!headers.isArray() || headers.size() != names.size()) {
            throw failure("response headers do not match configured schema");
        }
        for (int i = 0; i < names.size(); i++) {
            if (!names.get(i).equals(text(headers.get(i).path("name"), "header name"))
                    || (metric
                            && !config.getMetricTypes()
                                    .get(i)
                                    .equals(text(headers.get(i).path("type"), "metric type")))) {
                throw failure(
                        "response header name or metric type does not match configured schema");
            }
        }
    }

    private void validateMetadata(JsonNode metadata) throws IOException {
        if (metadata.isMissingNode()) {
            return;
        }
        if (!metadata.isObject()) {
            throw failure("invalid metadata");
        }
        for (String flag : new String[] {"dataLossFromOtherRow", "subjectToThresholding"}) {
            if (metadata.has(flag)
                    && (!metadata.get(flag).isBoolean() || metadata.get(flag).booleanValue())) {
                throw failure("report metadata indicates " + flag + "; report rejected");
            }
        }
        if (metadata.has("samplingMetadatas")
                && (!metadata.get("samplingMetadatas").isArray()
                        || metadata.get("samplingMetadatas").size() != 0)) {
            throw failure("sampled report is not supported");
        }
        JsonNode restrictions = metadata.path("schemaRestrictionResponse");
        if (!restrictions.isMissingNode()) {
            if (!restrictions.isObject()) {
                throw failure("invalid schemaRestrictionResponse");
            }
            JsonNode active = restrictions.path("activeMetricRestrictions");
            if (!active.isMissingNode() && (!active.isArray() || active.size() != 0)) {
                throw failure("report contains restricted metrics");
            }
        }
        if (metadata.has("emptyReason")
                && !text(metadata.get("emptyReason"), "emptyReason").isEmpty()) {
            throw failure(
                    "report metadata contains an emptyReason; check property access and reporting availability");
        }
        if (metadata.has("currencyCode")) {
            String current = text(metadata.get("currencyCode"), "currencyCode");
            if (currency != null && !currency.equals(current)) {
                throw failure("report currency changed");
            }
            currency = current;
        }
        if (metadata.has("timeZone")) {
            String current = text(metadata.get("timeZone"), "timeZone");
            if (timeZone != null && !timeZone.equals(current)) {
                throw failure("report timeZone changed");
            }
            timeZone = current;
        }
    }

    private static void checkQuota(JsonNode quota) throws IOException {
        if (quota.isMissingNode()) {
            return;
        }
        if (!quota.isObject()) {
            throw failure("invalid propertyQuota");
        }
        for (String bucket :
                new String[] {
                    "tokensPerDay",
                    "tokensPerHour",
                    "tokensPerProjectPerHour",
                    "serverErrorsPerProjectPerHour"
                }) {
            JsonNode status = quota.path(bucket);
            if (!status.isMissingNode()) {
                if (!status.isObject()) {
                    throw failure("invalid propertyQuota status");
                }
                // A present status with omitted remaining means protobuf's default zero.
                long remaining =
                        status.has("remaining")
                                ? count(status.get("remaining"), "quota remaining")
                                : 0;
                if (remaining == 0) {
                    throw failure(
                            "property quota exhausted before report completion; retry the job after quota reset");
                }
            }
        }
    }

    private static void values(JsonNode values, int width) throws IOException {
        if (width == 0 && values.isMissingNode()) {
            return;
        }
        if (!values.isArray() || values.size() != width) {
            throw failure("row width does not match configured schema");
        }
    }

    private static String text(JsonNode value, String field) throws IOException {
        if (!value.isTextual()) {
            throw failure("missing or invalid " + field);
        }
        return value.textValue();
    }

    private static long count(JsonNode value, String field) throws IOException {
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw failure("invalid " + field);
        }
        return value.longValue();
    }

    // Google ALPHANUMERIC compares Unicode code points, not UTF-16 code units.
    static int compare(List<String> left, List<String> right) {
        for (int i = 0; i < left.size(); i++) {
            String a = left.get(i);
            String b = right.get(i);
            int x = 0;
            int y = 0;
            while (x < a.length() && y < b.length()) {
                int aPoint = a.codePointAt(x);
                int bPoint = b.codePointAt(y);
                int cmp = Integer.compare(aPoint, bPoint);
                if (cmp != 0) {
                    return cmp;
                }
                x += Character.charCount(aPoint);
                y += Character.charCount(bPoint);
            }
            int cmp = Integer.compare(a.length() - x, b.length() - y);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    static IOException failure(String message) {
        // Never attach an HTTP/JSON/auth exception: it may contain credentials or report data.
        return new IOException("GoogleAnalytics4: " + message);
    }

    static final class Page {
        final List<SeaTunnelRow> rows;
        final boolean finished;

        Page(List<SeaTunnelRow> rows, boolean finished) {
            this.rows = rows;
            this.finished = finished;
        }
    }
}
