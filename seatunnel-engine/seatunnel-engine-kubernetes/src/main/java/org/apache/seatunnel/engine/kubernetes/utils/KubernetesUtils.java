/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.kubernetes.utils;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Common utils for Kubernetes. */
public class KubernetesUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesUtils.class);

    private static final YAMLMapper yamlMapper = new YAMLMapper();

    public static String tryToGetPrettyPrintYaml(KubernetesResource kubernetesResource) {
        try {
            return yamlMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(kubernetesResource);
        } catch (Exception ex) {
            LOG.debug(
                    "Failed to get the pretty print yaml, fallback to {}", kubernetesResource, ex);
            return kubernetesResource.toString();
        }
    }


    private KubernetesUtils() {}
}
