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

package org.apache.seatunnel.connectors.seatunnel.slack.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.slack.config.SlackSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class SlackSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Slack";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        SlackSinkOptions.WEBHOOKS_URL,
                        SlackSinkOptions.OAUTH_TOKEN,
                        SlackSinkOptions.SLACK_CHANNEL)
                .build();
    }
}
