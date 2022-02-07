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

package org.apache.seatunnel.env;

import org.apache.seatunnel.apis.BaseSink;
import org.apache.seatunnel.apis.BaseSource;
import org.apache.seatunnel.apis.BaseTransform;
import org.apache.seatunnel.plugin.Plugin;

import java.util.List;

/**
 * the SeaTunnel job's execution context
 */
public interface Execution<SR extends BaseSource, TF extends BaseTransform, SK extends BaseSink> extends Plugin<Void> {

    /**
     * start to execute the SeaTunnel job
     *
     * @param sources    source plugin list
     * @param transforms transform plugin list
     * @param sinks      sink plugin list
     */
    void start(List<SR> sources, List<TF> transforms, List<SK> sinks) throws Exception;
}
