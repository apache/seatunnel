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

package org.apache.seatunnel.engine.server.checkpoint;

/** Stable checkpoint identity for an operator-scoped coordinator. */
public final class CoordinatorStateKey extends ActionStateKey {

    private static final long serialVersionUID = 1L;
    private static final String NAME_PREFIX = "DynamicLookupCoordinatorStateKey - ";

    /** Stable operator identity embedded in the checkpoint map key. */
    private final String operatorUid;

    /**
     * Creates an immutable operator-scoped checkpoint key.
     *
     * @param operatorUid stable lookup operator identity
     */
    public CoordinatorStateKey(String operatorUid) {
        super(NAME_PREFIX + requireOperatorUid(operatorUid));
        this.operatorUid = operatorUid;
    }

    public String getOperatorUid() {
        return operatorUid;
    }

    /** Coordinator checkpoint identities are immutable once used as map keys. */
    @Override
    public void setName(String ignored) {
        throw new UnsupportedOperationException("CoordinatorStateKey is immutable");
    }

    @Override
    public String toString() {
        return "CoordinatorStateKey{" + "operatorUid='" + operatorUid + '\'' + '}';
    }

    private static String requireOperatorUid(String operatorUid) {
        if (operatorUid == null || operatorUid.trim().isEmpty()) {
            throw new IllegalArgumentException("operatorUid must not be blank");
        }
        return operatorUid;
    }
}
