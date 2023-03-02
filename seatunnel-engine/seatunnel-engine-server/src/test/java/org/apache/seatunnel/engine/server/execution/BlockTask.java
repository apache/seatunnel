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

package org.apache.seatunnel.engine.server.execution;

import lombok.NonNull;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlockTask implements Task {

    @Override
    public boolean isThreadsShare() {
        return true;
    }

    @NonNull @Override
    public ProgressState call() throws Exception {
        BlockingQueue<String> bq = new LinkedBlockingQueue<>();
        bq.poll(1000, TimeUnit.MINUTES);

        return ProgressState.MADE_PROGRESS;
    }

    @NonNull @Override
    public Long getTaskID() {
        return (long) this.hashCode();
    }
}
