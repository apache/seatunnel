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

package io.github.interestinglab.waterdrop.config.impl;

/** The key used to memoize already-traversed nodes when resolving substitutions */
final class MemoKey {
    MemoKey(AbstractConfigValue value, Path restrictToChildOrNull) {
        this.value = value;
        this.restrictToChildOrNull = restrictToChildOrNull;
    }

    private final AbstractConfigValue value;
    private final Path restrictToChildOrNull;

    @Override
    public final int hashCode() {
        int h = System.identityHashCode(value);
        if (restrictToChildOrNull != null) {
            return h + 41 * (41 + restrictToChildOrNull.hashCode());
        } else {
            return h;
        }
    }

    @Override
    public final boolean equals(Object other) {
        if (other instanceof MemoKey) {
            MemoKey o = (MemoKey) other;
            if (o.value != this.value) {
                return false;
            } else if (o.restrictToChildOrNull == this.restrictToChildOrNull) {
                return true;
            } else if (o.restrictToChildOrNull == null || this.restrictToChildOrNull == null) {
                return false;
            } else {
                return o.restrictToChildOrNull.equals(this.restrictToChildOrNull);
            }
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "MemoKey(" + value + "@" + System.identityHashCode(value) + "," + restrictToChildOrNull + ")";
    }
}
