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

package org.apache.seatunnel.api.cdc;

import org.apache.seatunnel.api.annotation.Experimental;

import java.util.Objects;

/**
 * A CDC progress value together with the accuracy of that individual value.
 *
 * <p>{@link CdcProgressAccuracy#EXACT} and {@link CdcProgressAccuracy#BEST_EFFORT} always carry a
 * non-null value. {@link CdcProgressAccuracy#UNSUPPORTED} and {@link
 * CdcProgressAccuracy#UNAVAILABLE} never carry one.
 */
@Experimental
public final class CdcProgressValue<T> {

    private final T value;
    private final CdcProgressAccuracy accuracy;

    private CdcProgressValue(T value, CdcProgressAccuracy accuracy) {
        this.accuracy = Objects.requireNonNull(accuracy, "accuracy must not be null");
        if (accuracy == CdcProgressAccuracy.EXACT || accuracy == CdcProgressAccuracy.BEST_EFFORT) {
            this.value = Objects.requireNonNull(value, "a supported value must not be null");
        } else {
            if (value != null) {
                throw new IllegalArgumentException(
                        "unsupported or unavailable progress must not carry a value");
            }
            this.value = null;
        }
    }

    public static <T> CdcProgressValue<T> exact(T value) {
        return new CdcProgressValue<>(value, CdcProgressAccuracy.EXACT);
    }

    public static <T> CdcProgressValue<T> bestEffort(T value) {
        return new CdcProgressValue<>(value, CdcProgressAccuracy.BEST_EFFORT);
    }

    public static <T> CdcProgressValue<T> unsupported() {
        return new CdcProgressValue<>(null, CdcProgressAccuracy.UNSUPPORTED);
    }

    public static <T> CdcProgressValue<T> unavailable() {
        return new CdcProgressValue<>(null, CdcProgressAccuracy.UNAVAILABLE);
    }

    public T getValue() {
        return value;
    }

    public CdcProgressAccuracy getAccuracy() {
        return accuracy;
    }
}
