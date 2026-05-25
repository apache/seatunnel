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

    public ValidatingDirtyRecordCollector(
            DirtyRecordCollector delegate, DirtyDataValidator validator) {
        this.delegate = delegate;
        this.validator = Preconditions.checkNotNull(validator, "validator cannot be null");
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

    @Override
    public boolean validateAndCollectIfDirty(
            int subTaskIndex, SeaTunnelRow record, CatalogTable catalogTable) {
        DirtyDataValidator.ValidationResult result;
        try {
            result = validator.validate(record, catalogTable);
        } catch (Exception e) {
            log.warn("User validator failed, treating as non-dirty: {}", e.getMessage());
            return false;
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

    @Override
    public void close() throws Exception {
        try {
            validator.close();
        } finally {
            delegate.close();
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
