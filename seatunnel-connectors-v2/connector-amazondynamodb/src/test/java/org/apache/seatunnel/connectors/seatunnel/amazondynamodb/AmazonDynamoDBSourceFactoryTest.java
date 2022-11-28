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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.sink.AmazonDynamoDBSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.source.AmazonDynamoDBSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AmazonDynamoDBSourceFactoryTest {

    /**
     * Method: optionRule()
     */
    @Test
    public void testOptionRule() throws Exception {
        AmazonDynamoDBSourceFactory amazonDynamoDBSourceFactory = new AmazonDynamoDBSourceFactory();
        OptionRule sourceOptionRule = amazonDynamoDBSourceFactory.optionRule();
        Assertions.assertNotNull(sourceOptionRule);

        AmazonDynamoDBSinkFactory amazonDynamoDBSinkFactory = new AmazonDynamoDBSinkFactory();
        OptionRule sinkOptionRule = amazonDynamoDBSinkFactory.optionRule();
        Assertions.assertNotNull(sinkOptionRule);
    }
}
