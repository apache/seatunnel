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

import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Static utility class for creating various {@link Measurement} filtering
 * predicates.
 *
 */
public final class MeasurementPredicates {

    private MeasurementPredicates() { }

    /**
     * Matches a {@link Measurement} which contain the specified tag.
     *
     * @param tag the tag of interest
     * @return a filtering predicate
     */
    public static Predicate<Measurement> containsTag(String tag) {
        return measurement -> measurement.tag(tag) != null;
    }

    /**
     * Matches a {@link Measurement} which contains the specified tag and
     * the tag has the specified value.
     *
     * @param tag   the tag to match
     * @param value the value the tag has to have
     * @return a filtering predicate
     */
    public static Predicate<Measurement> tagValueEquals(String tag, String value) {
        return measurement -> value.equals(measurement.tag(tag));
    }

    /**
     * Matches a {@link Measurement} which has this exact tag with a value
     * matching the provided regular expression.
     *
     * @param tag         the tag to match
     * @param valueRegexp regular expression to match the value against
     * @return a filtering predicate
     */
    public static Predicate<Measurement> tagValueMatches(String tag, String valueRegexp) {
        return measurement -> {
            String value = measurement.tag(tag);
            return value != null && Pattern.compile(valueRegexp).matcher(value).matches();
        };
    }
}
