/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.seatunnel.engine.server.autoscale;

/** Publishes accepted autoscaling recommendations to a downstream consumer. */
public interface RecommendationPublisher {

    /**
     * Publishes a recommendation after validating its master epoch and generation ordering.
     *
     * <ul>
     *   <li>{@link PublicationResult#ACCEPTED}: newer recommendation stored.
     *   <li>{@link PublicationResult#DUPLICATE}: same master epoch and generation as the latest
     *       accepted recommendation.
     *   <li>{@link PublicationResult#REJECTED}: older than the latest accepted recommendation.
     * </ul>
     *
     * @param recommendation recommendation to publish
     * @return the publication result
     */
    PublicationResult publish(ScalingRecommendation recommendation);

    /** Resets publication ordering for a new active-master incarnation. */
    void reset();

    enum PublicationResult {
        ACCEPTED,
        DUPLICATE,
        REJECTED
    }
}
