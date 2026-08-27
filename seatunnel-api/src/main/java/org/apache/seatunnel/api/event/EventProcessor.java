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

package org.apache.seatunnel.api.event;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public interface EventProcessor extends AutoCloseable {
    void process(Event event);

    static List<EventHandler> loadEventHandlers(ClassLoader classLoader) {
        try {
            List<EventHandler> result = new LinkedList<>();
            ServiceLoader.load(EventHandler.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            throw new RuntimeException("Could not load service provider for event handlers.", e);
        }
    }

    static void close(List<EventHandler> handlers) throws Exception {
        if (handlers != null) {
            for (EventHandler handler : handlers) {
                handler.close();
            }
        }
    }
}
