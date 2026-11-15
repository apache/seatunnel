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

package org.apache.seatunnel.resource.kubernetes.kubeclient;

import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;

/**
 * Receives one event per change of the watched Pods.
 *
 * <p>The callback mirrors the Kubernetes watch actions instead of a snapshot stream, so a deleted
 * Pod is reported once and immediately. Implementations must not block: callbacks run on the watch
 * threads and an exception stops the watch and is reported through {@link
 * #onWatchError(Exception)}.
 */
public interface PodWatchCallback {

    /** @param pod Pod added to the watched label selection */
    void onPodAdded(KubernetesPod pod);

    /** @param pod Pod whose state or metadata changed */
    void onPodModified(KubernetesPod pod);

    /** @param pod Pod removed from the watched label selection */
    void onPodDeleted(KubernetesPod pod);

    /**
     * Reports a terminal watch failure, either an unrecoverable API error or an exhausted transient
     * failure budget. No callback is delivered after this one.
     *
     * @param failure terminal watch failure
     */
    void onWatchError(Exception failure);
}
