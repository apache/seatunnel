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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.Getter;

import java.io.Serializable;

/**
 * User-defined dirty data validator interface. Extends {@link Factory} so that implementations are
 * discovered via {@code FactoryUtil.discoverFactories()} — the same SPI used for connectors.
 *
 * <p>Register implementations in {@code
 * META-INF/services/org.apache.seatunnel.api.table.factory.Factory}.
 */
public interface DirtyDataValidator extends Factory, Serializable {

    default void init(Config config, CatalogTable catalogTable) throws Exception {}

    ValidationResult validate(SeaTunnelRow record, CatalogTable catalogTable);

    default void close() throws Exception {}

    @Override
    default String factoryIdentifier() {
        return getClass().getSimpleName();
    }

    @Override
    default OptionRule optionRule() {
        return OptionRule.builder().build();
    }

    @Getter
    class ValidationResult implements Serializable {
        private final boolean isDirty;
        private final String errorMessage;
        private final Throwable exception;

        public ValidationResult(boolean isDirty, String errorMessage) {
            this(isDirty, errorMessage, null);
        }

        public ValidationResult(boolean isDirty, String errorMessage, Throwable exception) {
            this.isDirty = isDirty;
            this.errorMessage = errorMessage;
            this.exception = exception;
        }

        public static ValidationResult clean() {
            return new ValidationResult(false, null);
        }

        public static ValidationResult dirty(String errorMessage) {
            return new ValidationResult(true, errorMessage);
        }

        public static ValidationResult dirty(String errorMessage, Throwable exception) {
            return new ValidationResult(true, errorMessage, exception);
        }
    }
}
