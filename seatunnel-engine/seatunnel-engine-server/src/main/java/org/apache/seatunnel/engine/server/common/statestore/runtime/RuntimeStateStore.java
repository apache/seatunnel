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

package org.apache.seatunnel.engine.server.common.statestore.runtime;

import org.apache.seatunnel.engine.server.common.statestore.IterableStateStore;
import org.apache.seatunnel.engine.server.common.statestore.StateStore;

/**
 * Store for runtime engine coordination state.
 *
 * <p>This contract is intended for states where immediate reads and writes matter more than TTL or
 * expiration events, such as current job state, running job info, or owned slot profiles.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface RuntimeStateStore<K, V> extends StateStore<K, V>, IterableStateStore<K, V> {}
