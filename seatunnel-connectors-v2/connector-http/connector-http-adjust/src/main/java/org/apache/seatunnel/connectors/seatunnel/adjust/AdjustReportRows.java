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

package org.apache.seatunnel.connectors.seatunnel.adjust;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustReportClient.problem;

/** Checks that an entire report day is complete before converting any output. */
final class AdjustReportRows {
    private static final int MAX_ROWS = 20000;
    private final AdjustReportPlan plan;
    private final String tableId;

    AdjustReportRows(AdjustReportPlan plan, String tableId) {
        this.plan = plan;
        this.tableId = tableId;
    }

    List<SeaTunnelRow> convert(LocalDate day, JsonNode response)
            throws AdjustReportClient.ReportFailure {
        if (response == null) {
            // HTTP 204 carries no report data.
            return Collections.emptyList();
        }
        JsonNode pagination = response.path("pagination");
        if (!absent(pagination) && (!pagination.isContainerNode() || !pagination.isEmpty())) {
            throw problem("unsupported pagination in a daily report");
        }
        JsonNode warnings = response.path("warnings");
        if (!absent(warnings) && (!warnings.isArray() || !warnings.isEmpty())) {
            throw problem("report contains warnings or an invalid warnings field");
        }
        JsonNode totals = response.path("totals");
        if (!absent(totals) && !totals.isObject()) {
            throw problem("totals must be an object");
        }
        if (!absent(response.path("error"))) {
            throw problem("success response contains an error");
        }
        JsonNode data = response.path("rows");
        if (!data.isArray() || data.size() > MAX_ROWS) {
            throw problem("rows must be an array with at most 20000 entries");
        }
        List<SeaTunnelRow> result = new ArrayList<>(data.size());
        Set<List<Object>> groups = new HashSet<>();
        for (JsonNode source : data) {
            if (!source.isObject() || !day.toString().equals(source.path("day").textValue())) {
                throw problem("row day does not match the requested day");
            }
            Object[] fields = new Object[1 + plan.dimensions.size() + plan.metrics.size()];
            fields[0] = day;
            int column = 1;
            for (String dimension : plan.dimensions) {
                fields[column++] = dimension(source.path(dimension), dimension);
            }
            if (!groups.add(Arrays.asList(Arrays.copyOfRange(fields, 1, column)))) {
                throw problem("report repeats a dimension group for one day");
            }
            for (AdjustReportPlan.Metric metric : plan.metrics) {
                fields[column++] = decimal(source.path(metric.name), metric);
            }
            SeaTunnelRow record = new SeaTunnelRow(fields);
            record.setTableId(tableId);
            result.add(record);
        }
        return result;
    }

    private static boolean absent(JsonNode value) {
        return value.isMissingNode() || value.isNull();
    }

    private static String dimension(JsonNode value, String name)
            throws AdjustReportClient.ReportFailure {
        if (absent(value)) {
            return null;
        }
        if (value.isTextual() || value.isIntegralNumber() || value.isBoolean()) {
            return value.asText();
        }
        throw problem("dimension " + name + " must be a scalar string or ID");
    }

    private static BigDecimal decimal(JsonNode value, AdjustReportPlan.Metric metric)
            throws AdjustReportClient.ReportFailure {
        if (absent(value)) {
            return null;
        }
        try {
            BigDecimal number;
            if (value.isNumber()) {
                number = value.decimalValue();
            } else if (value.isTextual() && value.textValue().length() <= 512) {
                number = new BigDecimal(value.textValue());
            } else {
                throw problem("metric " + metric.name + " must contain a decimal number");
            }
            BigDecimal normalized =
                    number.signum() == 0 ? BigDecimal.ZERO : number.stripTrailingZeros();
            // Check the exponent before scaling: a short literal can expand to billions of digits.
            if (normalized.scale() <= metric.scale
                    && (normalized.signum() == 0
                            || (long) normalized.precision() - normalized.scale()
                                    <= AdjustReportPlan.PRECISION - metric.scale)) {
                return normalized.setScale(metric.scale, RoundingMode.UNNECESSARY);
            }
        } catch (NumberFormatException | ArithmeticException ignored) {
            // Error messages omit metric values and provider contents.
        }
        throw problem(
                "metric " + metric.name + " does not fit the configured decimal without rounding");
    }
}
