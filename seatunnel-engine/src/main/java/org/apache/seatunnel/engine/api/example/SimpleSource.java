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

package org.apache.seatunnel.engine.api.example;

import org.apache.seatunnel.engine.api.source.Boundedness;
import org.apache.seatunnel.engine.api.source.Source;
import org.apache.seatunnel.engine.api.source.SourceReader;
import org.apache.seatunnel.engine.config.Configuration;

public class SimpleSource implements Source {

    @Override
    public void setConfiguration(Configuration configuration) {

    }

    @Override
    public int getTotalTaskNumber() {
        return 2;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader createSourceReader(int taskId, int totalTaskNumber) {
        return new SimpleSourceReader();
    }

    @Override
    public SourceReader restoreSourceReader(int taskId, int totalTaskNumber, byte[] state) {
        return null;
    }
}
