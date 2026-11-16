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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.client;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;

import java.util.function.Supplier;

public interface WriteClientProvider {

    /** Obtain the owned client; authenticated callers must use it only inside {@link #execute}. */
    HoodieJavaWriteClient<HoodieAvroPayload> getOrCreateClient();

    /**
     * Run the entire client operation (including creation and commit) under the provider's
     * identity. The returned value must not be used to access the raw client outside this scope.
     * Caller interruption does not guarantee cancellation of in-flight I/O.
     */
    default <T> T execute(Supplier<T> action) {
        return action.get();
    }

    /** Close owned client/identity once, after all operations finish; do not race with execute. */
    void close();
}
