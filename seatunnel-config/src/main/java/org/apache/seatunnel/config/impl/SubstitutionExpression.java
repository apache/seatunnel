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

final class SubstitutionExpression {

    private final Path path;
    private final boolean optional;

    SubstitutionExpression(Path path, boolean optional) {
        this.path = path;
        this.optional = optional;
    }

    Path path() {
        return path;
    }

    boolean optional() {
        return optional;
    }

    SubstitutionExpression changePath(Path newPath) {
        if (newPath == path) {
            return this;
        }
        return new SubstitutionExpression(newPath, optional);
    }

    @Override
    public String toString() {
        return "${" + (optional ? "?" : "") + path.render() + "}";
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof SubstitutionExpression) {
            SubstitutionExpression otherExp = (SubstitutionExpression) other;
            return otherExp.path.equals(this.path) && otherExp.optional == this.optional;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 41 * (41 + path.hashCode());
        h = 41 * (h + (optional ? 1 : 0));
        return h;
    }
}
