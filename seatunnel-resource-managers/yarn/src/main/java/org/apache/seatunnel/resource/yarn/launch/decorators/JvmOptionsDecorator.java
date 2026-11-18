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

package org.apache.seatunnel.resource.yarn.launch.decorators;

import org.apache.seatunnel.resource.yarn.config.YarnOptions;
import org.apache.seatunnel.resource.yarn.launch.YarnLaunchParameters;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnContainerSpec;

/** Adds the common JVM argument file first so the role-specific file can override it. */
public final class JvmOptionsDecorator extends AbstractYarnStepDecorator {

    @Override
    public YarnContainerSpec decorate(YarnContainerSpec spec) {
        YarnLaunchParameters parameters = spec.getParameters();
        spec.getCommand().addFileOption("@", parameters.path(YarnOptions.JVM_OPTIONS));
        String roleOptionsFile;
        if (spec.isMaster()) {
            roleOptionsFile = parameters.path(YarnOptions.MASTER_JVM_OPTIONS);
        } else {
            roleOptionsFile = parameters.path(YarnOptions.WORKER_JVM_OPTIONS);
        }
        spec.getCommand().addFileOption("@", roleOptionsFile);
        return spec;
    }
}
