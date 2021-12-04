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

import io.github.interestinglab.waterdrop.config.ConfigValue;

/**
 * An AbstractConfigValue which contains other values. Java has no way to
 * express "this has to be an AbstractConfigValue also" other than making
 * AbstractConfigValue an interface which would be aggravating. But we can say
 * we are a ConfigValue.
 */
interface Container extends ConfigValue {
    /**
     * Replace a child of this value. CAUTION if replacement is null, delete the
     * child, which may also delete the parent, or make the parent into a
     * non-container.
     */
    AbstractConfigValue replaceChild(AbstractConfigValue child, AbstractConfigValue replacement);

    /**
     * Super-expensive full traversal to see if descendant is anywhere
     * underneath this container.
     */
    boolean hasDescendant(AbstractConfigValue descendant);
}
