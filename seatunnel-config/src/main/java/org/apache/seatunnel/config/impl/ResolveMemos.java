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

package org.apache.seatunnel.config.impl;

import java.util.HashMap;
import java.util.Map;

/**
 * This exists because we have to memoize resolved substitutions as we go
 * through the config tree; otherwise we could end up creating multiple copies
 * of values or whole trees of values as we follow chains of substitutions.
 */
final class ResolveMemos {
    // note that we can resolve things to undefined (represented as Java null,
    // rather than ConfigNull) so this map can have null values.
    private final Map<MemoKey, AbstractConfigValue> memos;

    private ResolveMemos(Map<MemoKey, AbstractConfigValue> memos) {
        this.memos = memos;
    }

    ResolveMemos() {
        this(new HashMap<MemoKey, AbstractConfigValue>());
    }

    AbstractConfigValue get(MemoKey key) {
        return memos.get(key);
    }

    ResolveMemos put(MemoKey key, AbstractConfigValue value) {
        // completely inefficient, but so far nobody cares about resolve()
        // performance, we can clean it up someday...
        Map<MemoKey, AbstractConfigValue> copy = new HashMap<MemoKey, AbstractConfigValue>(memos);
        copy.put(key, value);
        return new ResolveMemos(copy);
    }
}
