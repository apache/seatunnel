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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.extern.slf4j.Slf4j;

/**
 * Decorator that adds user-defined validation to any {@link DirtyRecordCollector}. All collection
 * methods delegate to the wrapped collector; {@link #validateAndCollectIfDirty} consults the {@link
 * DirtyDataValidator} before writing and collects records that fail validation.
 *
 * <p>Created by {@link DirtyCollectorConfigProcessor} when a {@code dirty.validator} is configured.
 */
@Slf4j
public class ValidatingDirtyRecordCollector implements DirtyRecordCollector {

    private static final long serialVersionUID = 1L;

    private final DirtyRecordCollector delegate;
    private final DirtyDataValidator validator;

    /**
     * Requires a real collection destination before enabling user validation. A no-op destination
     * would drop rejected rows without providing the configured dirty-handling guarantee.
     */
    public ValidatingDirtyRecordCollector(
            DirtyRecordCollector delegate, DirtyDataValidator validator) {
        this.validator = Preconditions.checkNotNull(validator, "validator cannot be null");
        this.delegate = Preconditions.checkNotNull(delegate, "collector cannot be null");
        Preconditions.checkArgument(
                !(delegate instanceof NoOpDirtyRecordCollector),
                "dirty.validator requires an active dirty.collector.");
    }

    @Override
    public void collect(
            int subTaskIndex,
            Object dirtyRecord,
            Throwable exception,
            String errorMessage,
            CatalogTable catalogTable) {
        delegate.collect(subTaskIndex, dirtyRecord, exception, errorMessage, catalogTable);
    }

    @Override
    public void collectFromUserRule(
            int subTaskIndex, Object record, String errorMessage, CatalogTable catalogTable) {
        delegate.collectFromUserRule(subTaskIndex, record, errorMessage, catalogTable);
    }

    /**
     * Validates before writing and collects only explicit dirty results. Validator failures or a
     * missing result abort the row path instead of being interpreted as successful validation.
     */
    @Override
    public boolean validateAndCollectIfDirty(
            int subTaskIndex, SeaTunnelRow record, CatalogTable catalogTable) {
        DirtyDataValidator.ValidationResult result;
        try {
            result = validator.validate(record, catalogTable);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Dirty data validator failed: " + validator.getClass().getName(), e);
        }
        if (result == null) {
            throw new IllegalStateException(
                    "Dirty data validator returned null: " + validator.getClass().getName());
        }
        if (result.isDirty()) {
            collectFromUserRule(
                    subTaskIndex,
                    record,
                    result.getErrorMessage() != null ? result.getErrorMessage() : "user rule",
                    catalogTable);
            return true;
        }
        return false;
    }

    @Override
    public void init(Config config) throws Exception {
        delegate.init(config);
    }

    @Override
    public void init(Config config, CatalogTable catalogTable) throws Exception {
        delegate.init(config, catalogTable);
    }

    /**
     * Closes both owned components while retaining the first failure as the primary exception.
     * Collector closure still runs after a validator failure, with its failure suppressed.
     */
    @Override
    public void close() throws Exception {
        Throwable failure = null;
        try {
            validator.close();
        } catch (Throwable validatorFailure) {
            failure = validatorFailure;
        }
        try {
            delegate.close();
        } catch (Throwable collectorFailure) {
            if (failure == null) {
                failure = collectorFailure;
            } else if (failure != collectorFailure) {
                failure.addSuppressed(collectorFailure);
            }
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure != null) {
            throw (Exception) failure;
        }
    }

    @Override
    public long getDirtyRecordCount() {
        return delegate.getDirtyRecordCount();
    }

    @Override
    public void checkThreshold() throws Exception {
        delegate.checkThreshold();
    }

    @Override
    public void setDistributedCounter(DistributedCounter counter) {
        delegate.setDistributedCounter(counter);
    }

    @Override
    public void incrementDistributedCounter() {
        delegate.incrementDistributedCounter();
    }
}
