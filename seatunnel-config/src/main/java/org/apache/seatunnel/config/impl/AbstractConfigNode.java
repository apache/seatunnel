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

import org.apache.seatunnel.config.parser.ConfigNode;

import java.util.Collection;

abstract class AbstractConfigNode implements ConfigNode {
    abstract Collection<Token> tokens();

    @Override
    public final String render() {
        StringBuilder origText = new StringBuilder();
        Iterable<Token> tokens = tokens();
        for (Token t : tokens) {
            origText.append(t.tokenText());
        }
        return origText.toString();
    }

    @Override
    public final boolean equals(Object other) {
        return other instanceof AbstractConfigNode && render().equals(((AbstractConfigNode) other).render());
    }

    @Override
    public final int hashCode() {
        return render().hashCode();
    }
}
