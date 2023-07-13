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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer;

import org.apache.iceberg.io.TaskWriter;

import java.io.Serializable;

/**
 * Factory to create {@link TaskWriter}
 *
 * @param <T> data type of record.
 */
public interface TaskWriterFactory<T> extends Serializable {

    /**
     * Initialize the factory with a given taskId and attemptId.
     *
     * @param taskId the identifier of task.
     * @param attemptId the attempt id of this task.
     */
    void initialize(int taskId, int attemptId);

    /**
     * Initialize a {@link TaskWriter} with given task id and attempt id.
     *
     * @return a newly created task writer.
     */
    TaskWriter<T> create();
}
