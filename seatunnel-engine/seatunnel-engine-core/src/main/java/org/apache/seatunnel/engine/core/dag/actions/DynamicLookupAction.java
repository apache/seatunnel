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

package org.apache.seatunnel.engine.core.dag.actions;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;

import lombok.NonNull;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Action that declares and executes a dynamic lookup operator with fact and dimension inputs.
 *
 * <p>The action owns the planner-visible lookup contract, stable source identities, and fixed input
 * port bindings. Runtime support is intentionally constrained to the M0 direct-source topology so
 * unsupported routing fails during planning instead of creating ambiguous checkpoint ownership.
 */
public final class DynamicLookupAction extends AbstractAction implements PortAwareAction {

    private static final long serialVersionUID = 1L;

    /** Stable fact-stream input port. */
    public static final int FACT_INPUT = 0;

    /** Stable dimension-bootstrap input port. */
    public static final int DIMENSION_INPUT = 1;

    /** Stable operator identity shared by planner, coordinator, and checkpoint topology. */
    private final String operatorUid;

    /** Logical fact-source action ID used only within the planned DAG. */
    private final long factSourceActionId;

    /** Stable fact-source identity retained when action IDs are regenerated. */
    private final String factSourceActionUid;

    /** Logical dimension-source action ID used only within the planned DAG. */
    private final long dimensionSourceActionId;

    /** Stable dimension-source identity retained when action IDs are regenerated. */
    private final String dimensionSourceActionUid;

    /** Immutable fact and dimension port bindings created by logical planning. */
    private final List<InputPortBinding> inputPortBindings;

    /** Parsed lookup contract retained through planning and execution reconstruction. */
    private final DynamicLookupDescriptor descriptor;

    /** Materialized output schema that later transforms and sinks consume. */
    private final CatalogTable producedCatalogTable;

    /** M0 logical dimension state budget enforced by the runtime before snapshotting. */
    private final long maxLogicalStateBytesPerSubtask;

    /** M0 resident dimension state budget enforced against the serialized in-memory payload. */
    private final long maxResidentStateBytesPerSubtask;

    /**
     * Creates the lookup declaration from two direct source actions.
     *
     * @param id logical action ID
     * @param name action name
     * @param operatorUid stable lookup operator identity
     * @param factInput dedicated fact source
     * @param factSourceActionUid stable fact-source identity
     * @param dimensionInput dedicated dimension source
     * @param dimensionSourceActionUid stable dimension-source identity
     * @param maxLogicalStateBytesPerSubtask logical dimension state cap for each lookup subtask
     * @param maxResidentStateBytesPerSubtask resident dimension state cap for each lookup subtask
     * @param jarUrls action dependency URLs
     * @param connectorJarIdentifiers connector plugin dependencies
     */
    public DynamicLookupAction(
            long id,
            @NonNull String name,
            @NonNull String operatorUid,
            @NonNull Action factInput,
            @NonNull String factSourceActionUid,
            @NonNull Action dimensionInput,
            @NonNull String dimensionSourceActionUid,
            @NonNull DynamicLookupDescriptor descriptor,
            @NonNull CatalogTable producedCatalogTable,
            long maxLogicalStateBytesPerSubtask,
            long maxResidentStateBytesPerSubtask,
            @NonNull Set<URL> jarUrls,
            @NonNull Set<ConnectorJarIdentifier> connectorJarIdentifiers) {
        super(id, name, Arrays.asList(factInput, dimensionInput), jarUrls, connectorJarIdentifiers);
        if (!(factInput instanceof SourceAction) || !(dimensionInput instanceof SourceAction)) {
            throw new IllegalArgumentException(
                    "Dynamic lookup M0 requires direct SourceAction inputs");
        }
        validate(
                operatorUid,
                factInput.getId(),
                factSourceActionUid,
                dimensionInput.getId(),
                dimensionSourceActionUid);
        this.operatorUid = operatorUid;
        this.factSourceActionId = factInput.getId();
        this.factSourceActionUid = factSourceActionUid;
        this.dimensionSourceActionId = dimensionInput.getId();
        this.dimensionSourceActionUid = dimensionSourceActionUid;
        this.inputPortBindings = bindings(id, factSourceActionId, dimensionSourceActionId);
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.producedCatalogTable =
                Objects.requireNonNull(producedCatalogTable, "producedCatalogTable");
        validateResourceBudget(maxLogicalStateBytesPerSubtask, maxResidentStateBytesPerSubtask);
        this.maxLogicalStateBytesPerSubtask = maxLogicalStateBytesPerSubtask;
        this.maxResidentStateBytesPerSubtask = maxResidentStateBytesPerSubtask;
    }

    /**
     * Recreates the action after logical planning, where upstream objects are represented by edge
     * IDs rather than the transient {@link Action#getUpstream()} list.
     *
     * @param id regenerated execution action ID
     * @param name action name
     * @param operatorUid stable lookup operator identity
     * @param factSourceActionId planned fact-source action ID
     * @param factSourceActionUid stable fact-source identity
     * @param dimensionSourceActionId planned dimension-source action ID
     * @param dimensionSourceActionUid stable dimension-source identity
     * @param inputPortBindings preserved planner bindings
     * @param maxLogicalStateBytesPerSubtask logical dimension state cap for each lookup subtask
     * @param maxResidentStateBytesPerSubtask resident dimension state cap for each lookup subtask
     * @param jarUrls action dependency URLs
     * @param connectorJarIdentifiers connector plugin dependencies
     */
    public DynamicLookupAction(
            long id,
            @NonNull String name,
            @NonNull String operatorUid,
            long factSourceActionId,
            @NonNull String factSourceActionUid,
            long dimensionSourceActionId,
            @NonNull String dimensionSourceActionUid,
            @NonNull List<InputPortBinding> inputPortBindings,
            @NonNull DynamicLookupDescriptor descriptor,
            @NonNull CatalogTable producedCatalogTable,
            long maxLogicalStateBytesPerSubtask,
            long maxResidentStateBytesPerSubtask,
            @NonNull Set<URL> jarUrls,
            @NonNull Set<ConnectorJarIdentifier> connectorJarIdentifiers) {
        super(id, name, jarUrls, connectorJarIdentifiers);
        validate(
                operatorUid,
                factSourceActionId,
                factSourceActionUid,
                dimensionSourceActionId,
                dimensionSourceActionUid);
        validateBindings(factSourceActionId, dimensionSourceActionId, inputPortBindings);
        this.operatorUid = operatorUid;
        this.factSourceActionId = factSourceActionId;
        this.factSourceActionUid = factSourceActionUid;
        this.dimensionSourceActionId = dimensionSourceActionId;
        this.dimensionSourceActionUid = dimensionSourceActionUid;
        this.inputPortBindings = Collections.unmodifiableList(new ArrayList<>(inputPortBindings));
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.producedCatalogTable =
                Objects.requireNonNull(producedCatalogTable, "producedCatalogTable");
        validateResourceBudget(maxLogicalStateBytesPerSubtask, maxResidentStateBytesPerSubtask);
        this.maxLogicalStateBytesPerSubtask = maxLogicalStateBytesPerSubtask;
        this.maxResidentStateBytesPerSubtask = maxResidentStateBytesPerSubtask;
    }

    public String getOperatorUid() {
        return operatorUid;
    }

    public long getFactSourceActionId() {
        return factSourceActionId;
    }

    public String getFactSourceActionUid() {
        return factSourceActionUid;
    }

    public long getDimensionSourceActionId() {
        return dimensionSourceActionId;
    }

    public String getDimensionSourceActionUid() {
        return dimensionSourceActionUid;
    }

    /** Returns the planner-provided stable source UID for a declared input port. */
    public String getSourceActionUid(int targetInputPort) {
        if (targetInputPort == FACT_INPUT) {
            return factSourceActionUid;
        }
        if (targetInputPort == DIMENSION_INPUT) {
            return dimensionSourceActionUid;
        }
        throw new IllegalArgumentException("Unknown dynamic lookup input port: " + targetInputPort);
    }

    @Override
    public List<InputPortBinding> getInputPortBindings() {
        return inputPortBindings;
    }

    public DynamicLookupDescriptor getDescriptor() {
        return descriptor;
    }

    public CatalogTable getProducedCatalogTable() {
        return producedCatalogTable;
    }

    public long getMaxLogicalStateBytesPerSubtask() {
        return maxLogicalStateBytesPerSubtask;
    }

    public long getMaxResidentStateBytesPerSubtask() {
        return maxResidentStateBytesPerSubtask;
    }

    private static void validate(
            String operatorUid,
            long factSourceActionId,
            String factSourceActionUid,
            long dimensionSourceActionId,
            String dimensionSourceActionUid) {
        requireNonBlank(operatorUid, "operatorUid");
        requireNonBlank(factSourceActionUid, "factSourceActionUid");
        requireNonBlank(dimensionSourceActionUid, "dimensionSourceActionUid");
        if (factSourceActionId == dimensionSourceActionId) {
            throw new IllegalArgumentException(
                    "Fact and dimension inputs must be different actions: " + factSourceActionId);
        }
        if (factSourceActionUid.equals(dimensionSourceActionUid)) {
            throw new IllegalArgumentException(
                    "Fact and dimension source UIDs must be different: " + factSourceActionUid);
        }
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    private static void validateResourceBudget(
            long maxLogicalStateBytesPerSubtask, long maxResidentStateBytesPerSubtask) {
        if (maxLogicalStateBytesPerSubtask <= 0 || maxResidentStateBytesPerSubtask <= 0) {
            throw new IllegalArgumentException("Dynamic lookup state budgets must be positive");
        }
        if (maxResidentStateBytesPerSubtask < maxLogicalStateBytesPerSubtask) {
            throw new IllegalArgumentException(
                    "Dynamic lookup resident budget must cover logical state budget");
        }
    }

    private static void validateBindings(
            long factSourceActionId,
            long dimensionSourceActionId,
            List<InputPortBinding> inputPortBindings) {
        if (inputPortBindings.size() != 2) {
            throw new IllegalArgumentException(
                    "Dynamic lookup must preserve exactly two input bindings");
        }
        InputPortBinding factBinding =
                inputPortBindings.stream()
                        .filter(binding -> binding.getTargetInputPort() == FACT_INPUT)
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Missing preserved fact input binding"));
        InputPortBinding dimensionBinding =
                inputPortBindings.stream()
                        .filter(binding -> binding.getTargetInputPort() == DIMENSION_INPUT)
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Missing preserved dimension input binding"));
        if (factBinding.getUpstreamActionId() != factSourceActionId
                || dimensionBinding.getUpstreamActionId() != dimensionSourceActionId) {
            throw new IllegalArgumentException(
                    "Preserved input bindings do not match their logical source action IDs");
        }
        if (factBinding.getEdgeId() == dimensionBinding.getEdgeId()) {
            throw new IllegalArgumentException(
                    "Fact and dimension bindings must have different edge IDs");
        }
    }

    private static List<InputPortBinding> bindings(
            long targetActionId, long factSourceActionId, long dimensionSourceActionId) {
        return Collections.unmodifiableList(
                Arrays.asList(
                        InputPortBinding.forward(factSourceActionId, targetActionId, FACT_INPUT),
                        InputPortBinding.forward(
                                dimensionSourceActionId, targetActionId, DIMENSION_INPUT)));
    }
}
