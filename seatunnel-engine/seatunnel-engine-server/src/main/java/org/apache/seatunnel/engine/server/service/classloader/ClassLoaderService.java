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

package org.apache.seatunnel.engine.server.service.classloader;

import java.net.URL;
import java.util.Collection;

/** ClassLoaderService is used to manage the classloader of the connector plugin. */
public interface ClassLoaderService {
    /**
     * Get the classloader of the connector plugin.
     *
     * @param jobId the job id
     * @param jars the jars of the connector plugin
     * @return the classloader of the connector plugin
     */
    ClassLoader getClassLoader(long jobId, Collection<URL> jars);

    /**
     * Release the classloader of the connector plugin.
     *
     * @param jobId the job id
     * @param jars the jars of the connector plugin
     */
    void releaseClassLoader(long jobId, Collection<URL> jars);

    /** Close the classloader service. */
    void close();
}
