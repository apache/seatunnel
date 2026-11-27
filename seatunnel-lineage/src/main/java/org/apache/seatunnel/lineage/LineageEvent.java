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

package org.apache.seatunnel.lineage;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Immutable event contract shared by engines and lineage backends. */
public final class LineageEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UUID runId;
    private final ZonedDateTime eventTime;
    private final LineageEventType eventType;
    private final String jobNamespace;
    private final String jobName;
    private final String producer;
    private final String runFacet;
    private final Map<String, Object> runProperties;
    private final List<LineageDataset> inputs;
    private final List<LineageDataset> outputs;

    private LineageEvent(Builder builder) {
        this.runId = require(builder.runId, "runId");
        this.eventTime = require(builder.eventTime, "eventTime");
        this.eventType = require(builder.eventType, "eventType");
        this.jobNamespace = LineageValidation.requireText(builder.jobNamespace, "jobNamespace");
        this.jobName = LineageValidation.requireText(builder.jobName, "jobName");
        this.producer = LineageValidation.requireText(builder.producer, "producer");
        this.runFacet = LineageValidation.requireText(builder.runFacet, "runFacet");
        this.runProperties =
                Collections.unmodifiableMap(new LinkedHashMap<>(builder.runProperties));
        this.inputs = Collections.unmodifiableList(new ArrayList<>(builder.inputs));
        this.outputs = Collections.unmodifiableList(new ArrayList<>(builder.outputs));
    }

    /** Returns a builder for an immutable lineage event. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns a builder pre-populated with every field of this event. */
    private Builder toBuilder() {
        return builder()
                .runId(runId)
                .eventTime(eventTime)
                .eventType(eventType)
                .jobNamespace(jobNamespace)
                .jobName(jobName)
                .producer(producer)
                .runFacet(runFacet)
                .runProperties(runProperties)
                .inputs(inputs)
                .outputs(outputs);
    }

    /** Returns a copy with a different lifecycle event type and a fresh event time. */
    public LineageEvent withEventType(LineageEventType type) {
        return toBuilder()
                .eventTime(ZonedDateTime.now(eventTime.getZone()))
                .eventType(type)
                .build();
    }

    /**
     * Returns a copy with one additional run-facet property.
     *
     * <p>Used for identifiers that only become known after the event was built, such as an engine
     * job identifier assigned at submission time. A {@code null} value leaves the event unchanged
     * so that callers do not need to branch.
     *
     * @param key run-facet property name
     * @param value run-facet property value; {@code null} is ignored
     * @return a copy carrying the property, or this event when the value is {@code null}
     */
    public LineageEvent withRunProperty(String key, Object value) {
        if (key == null || key.trim().isEmpty() || value == null) {
            return this;
        }
        Map<String, Object> updatedProperties = new LinkedHashMap<>(runProperties);
        updatedProperties.put(key, value);
        return toBuilder().runProperties(updatedProperties).build();
    }

    /** Returns a copy with the same statistics attached to every output dataset. */
    public LineageEvent withOutputStatistics(LineageOutputStatistics statistics) {
        List<LineageDataset> updatedOutputs = new ArrayList<>();
        for (LineageDataset output : outputs) {
            updatedOutputs.add(output.withOutputStatistics(statistics));
        }
        return toBuilder()
                .eventTime(ZonedDateTime.now(eventTime.getZone()))
                .outputs(updatedOutputs)
                .build();
    }

    /** Returns the run ID. */
    public UUID runId() {
        return runId;
    }

    /** Returns the timezone-aware event time. */
    public ZonedDateTime eventTime() {
        return eventTime;
    }

    /** Returns the lifecycle event type. */
    public LineageEventType eventType() {
        return eventType;
    }

    /** Returns the OpenLineage job namespace. */
    public String jobNamespace() {
        return jobNamespace;
    }

    /** Returns the OpenLineage job name. */
    public String jobName() {
        return jobName;
    }

    /** Returns the producer URI. */
    public String producer() {
        return producer;
    }

    /** Returns the custom run facet name. */
    public String runFacet() {
        return runFacet;
    }

    /** Returns immutable custom run properties. */
    public Map<String, Object> runProperties() {
        return runProperties;
    }

    /** Returns immutable input datasets. */
    public List<LineageDataset> inputs() {
        return inputs;
    }

    /** Returns immutable output datasets. */
    public List<LineageDataset> outputs() {
        return outputs;
    }

    /** Builder for the immutable lineage event contract. */
    public static final class Builder {
        private UUID runId;
        private ZonedDateTime eventTime;
        private LineageEventType eventType = LineageEventType.START;
        private String jobNamespace;
        private String jobName;
        private String producer;
        private String runFacet = LineageConfig.DEFAULT_RUN_FACET;
        private Map<String, Object> runProperties = new LinkedHashMap<>();
        private List<LineageDataset> inputs = new ArrayList<>();
        private List<LineageDataset> outputs = new ArrayList<>();

        /** Sets the run ID. */
        public Builder runId(UUID value) {
            this.runId = value;
            return this;
        }

        /** Sets the timezone-aware event time. */
        public Builder eventTime(ZonedDateTime value) {
            this.eventTime = value;
            return this;
        }

        /** Sets the lifecycle event type. */
        public Builder eventType(LineageEventType value) {
            this.eventType = value;
            return this;
        }

        /** Sets the OpenLineage job namespace. */
        public Builder jobNamespace(String value) {
            this.jobNamespace = value;
            return this;
        }

        /** Sets the OpenLineage job name. */
        public Builder jobName(String value) {
            this.jobName = value;
            return this;
        }

        /** Sets the producer URI. */
        public Builder producer(String value) {
            this.producer = value;
            return this;
        }

        /** Sets the custom run facet name. */
        public Builder runFacet(String value) {
            this.runFacet = value;
            return this;
        }

        /** Replaces the custom run properties. */
        public Builder runProperties(Map<String, ?> values) {
            this.runProperties = new LinkedHashMap<>();
            if (values != null) {
                values.forEach((key, value) -> this.runProperties.put(key, value));
            }
            return this;
        }

        /** Replaces the input datasets. */
        public Builder inputs(List<LineageDataset> values) {
            this.inputs = values == null ? new ArrayList<>() : new ArrayList<>(values);
            return this;
        }

        /** Replaces the output datasets. */
        public Builder outputs(List<LineageDataset> values) {
            this.outputs = values == null ? new ArrayList<>() : new ArrayList<>(values);
            return this;
        }

        /** Builds the immutable event, validating required fields. */
        public LineageEvent build() {
            return new LineageEvent(this);
        }
    }

    private static <T> T require(T value, String field) {
        if (value == null) {
            throw new IllegalArgumentException(field + " must not be null");
        }
        return value;
    }
}
