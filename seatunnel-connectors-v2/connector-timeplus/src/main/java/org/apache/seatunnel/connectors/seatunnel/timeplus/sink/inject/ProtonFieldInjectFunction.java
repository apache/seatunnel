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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Injects a field into a Timeplus statement, used to transform a java type into a Timeplus type.
 */
public interface ProtonFieldInjectFunction extends Serializable {

    /**
     * Inject the value into the statement.
     *
     * @param statement statement to inject into
     * @param value value to inject
     * @param index index in the statement
     */
    void injectFields(PreparedStatement statement, int index, Object value) throws SQLException;

    /**
     * If the fieldType need to be injected by the current function.
     *
     * @param fieldType field type to inject
     * @return true if the fieldType need to be injected by the current function
     */
    boolean isCurrentFieldType(String fieldType);
}
