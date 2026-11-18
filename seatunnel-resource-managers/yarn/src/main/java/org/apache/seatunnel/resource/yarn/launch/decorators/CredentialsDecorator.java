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

import org.apache.seatunnel.resource.yarn.config.YarnSecurityContext;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.YarnLaunchParameters;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnContainerSpec;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Obtains the AM's staging tokens or forwards only the AM's current HDFS tokens to a worker. */
public final class CredentialsDecorator extends AbstractYarnStepDecorator {

    @Override
    public YarnContainerSpec decorate(YarnContainerSpec spec) throws IOException {
        YarnLaunchParameters parameters = spec.getParameters();
        ByteBuffer tokens;
        if (spec.isMaster()) {
            tokens =
                    YarnSecurityContext.masterTokens(
                            parameters.getConfiguration(), parameters.getStaging());
        } else {
            tokens = YarnSecurityContext.workerTokens(parameters.getConfiguration());
        }
        if (tokens != null) {
            spec.getInternalResource().getEnvironment().remove(YarnConstants.HADOOP_USER_NAME_ENV);
            spec.getInternalResource().setTokens(tokens);
        }
        return spec;
    }
}
