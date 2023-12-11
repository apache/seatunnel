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

package org.apache.seatunnel.api.common.metrics;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public final class JobMetrics implements Serializable {

    private static final JobMetrics EMPTY = new JobMetrics(Collections.emptyMap());

    private static final Collector<Measurement, ?, Map<String, List<Measurement>>> COLLECTOR =
            Collectors.groupingBy(Measurement::metric);

    @Getter private Map<String, List<Measurement>> metrics; // metric name -> set of measurements

    JobMetrics() { // needed for deserialization
    }

    private JobMetrics(Map<String, List<Measurement>> metrics) {
        this.metrics = new HashMap<>(metrics);
    }

    /** Returns an empty {@link JobMetrics} object. */
    public static JobMetrics empty() {
        return EMPTY;
    }

    /** Builds a {@link JobMetrics} object based on a map of {@link Measurement}s. */
    public static JobMetrics of(Map<String, List<Measurement>> metrics) {
        return new JobMetrics(metrics);
    }

    public JobMetrics merge(JobMetrics jobMetrics) {
        if (jobMetrics == null) {
            return this;
        }
        Map<String, List<Measurement>> metricsMap = new HashMap<>();
        metrics.forEach((key, value) -> metricsMap.put(key, new ArrayList<>(value)));
        //// Because if a job is restarted, the running node might change, so we need to remove the
        // node information.
        Set<String> keysToExclude =
                new HashSet<>(Arrays.asList(MetricTags.MEMBER, MetricTags.ADDRESS));
        jobMetrics.metrics.forEach(
                (key, value) ->
                        metricsMap.merge(
                                key,
                                value,
                                (v1, v2) -> {
                                    List<Measurement> ms = new ArrayList<>(v2);
                                    for (Measurement m1 : v1) {
                                        if (v2.stream()
                                                .noneMatch(
                                                        m2 ->
                                                                areMapsEqualExcludingKeys(
                                                                        m2.getTags(),
                                                                        m1.getTags(),
                                                                        keysToExclude))) {
                                            ms.add(m1);
                                        }
                                    }
                                    return ms;
                                }));
        return new JobMetrics(metricsMap);
    }

    /**
     * Compares two Map objects excluding certain keys.
     *
     * @param map1 the first map
     * @param map2 the second map
     * @param keysToExclude the keys to be excluded during comparison
     * @return true if the maps are equal excluding the specific keys, false otherwise
     */
    public static boolean areMapsEqualExcludingKeys(
            Map<String, String> map1, Map<String, String> map2, Set<String> keysToExclude) {
        // Return false if either of the maps is null
        if (map1 == null || map2 == null) {
            return false;
        }

        // Return false if the sizes of the maps are different
        if (map1.size() != map2.size()) {
            return false;
        }

        // Create copies of the maps to avoid modifying the original maps
        Map<String, String> map1Copy = new HashMap<>(map1);
        Map<String, String> map2Copy = new HashMap<>(map2);

        // Remove specific keys from the copies
        for (String key : keysToExclude) {
            map1Copy.remove(key);
            map2Copy.remove(key);
        }

        // Return whether the copies are equal
        return map1Copy.equals(map2Copy);
    }

    /** Returns all metrics present. */
    public Set<String> metrics() {
        return Collections.unmodifiableSet(metrics.keySet());
    }

    /**
     * Returns all {@link Measurement}s associated with a given metric name.
     *
     * <p>For a list of job-specific metric names please see {@link MetricNames}.
     */
    public List<Measurement> get(String metricName) {
        Objects.requireNonNull(metricName);
        List<Measurement> measurements = metrics.get(metricName);
        return measurements == null ? Collections.emptyList() : measurements;
    }

    public JobMetrics filter(String tagName, String tagValue) {
        return filter(MeasurementPredicates.tagValueEquals(tagName, tagValue));
    }

    public JobMetrics filter(Predicate<Measurement> predicate) {
        Objects.requireNonNull(predicate, "predicate");

        Map<String, List<Measurement>> filteredMetrics =
                metrics.values().stream()
                        .flatMap(List::stream)
                        .filter(predicate)
                        .collect(COLLECTOR);
        return new JobMetrics(filteredMetrics);
    }

    @Override
    public int hashCode() {
        return metrics.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        return Objects.equals(metrics, ((JobMetrics) obj).metrics);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        metrics.entrySet().stream()
                .sorted(Comparator.comparing(Entry::getKey))
                .forEach(
                        mainEntry -> {
                            sb.append(mainEntry.getKey()).append(":\n");
                            mainEntry.getValue().stream()
                                    .collect(
                                            groupingBy(
                                                    m -> {
                                                        String vertex = m.tag(MetricTags.TASK_NAME);
                                                        return vertex == null ? "" : vertex;
                                                    }))
                                    .entrySet()
                                    .stream()
                                    .sorted(Comparator.comparing(Entry::getKey))
                                    .forEach(
                                            e -> {
                                                String vertexName = e.getKey();
                                                sb.append("  ").append(vertexName).append(":\n");
                                                e.getValue()
                                                        .forEach(
                                                                m ->
                                                                        sb.append("    ")
                                                                                .append(m)
                                                                                .append("\n"));
                                            });
                        });
        return sb.toString();
    }

    public String toJsonString() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this.metrics);
        } catch (JsonProcessingException e) {
            ObjectNode objectNode = objectMapper.createObjectNode();
            objectNode.put("err", "serialize JobMetrics err");
            return objectNode.toString();
        }
    }
}
