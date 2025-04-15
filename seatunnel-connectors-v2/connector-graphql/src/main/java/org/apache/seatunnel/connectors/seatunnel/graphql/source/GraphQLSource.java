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

package org.apache.seatunnel.connectors.seatunnel.graphql.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.graphql.config.GraphQLSourceParameter;
import org.apache.seatunnel.connectors.seatunnel.graphql.source.reader.GraphQLSourceHttpReader;
import org.apache.seatunnel.connectors.seatunnel.graphql.source.reader.GraphQLSourceSocketReader;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphQLSource extends HttpSource {

    protected GraphQLSourceParameter graphQLSourceParameter;
    protected Boolean enableSubscription;

    public GraphQLSource(ReadonlyConfig pluginConfig) {
        super(pluginConfig);
        graphQLSourceParameter = new GraphQLSourceParameter(pluginConfig, httpParameter);
        enableSubscription = graphQLSourceParameter.getEnableSubscription();
    }

    @Override
    public String getPluginName() {
        return "GraphQL";
    }

    @Override
    protected void buildSchemaWithConfig(ReadonlyConfig pluginConfig) {
        super.buildSchemaWithConfig(pluginConfig);
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        if (enableSubscription) {
            return new GraphQLSourceSocketReader(
                    graphQLSourceParameter, readerContext, contentField, deserializationSchema);
        } else {
            return new GraphQLSourceHttpReader(
                    graphQLSourceParameter, readerContext, contentField, deserializationSchema);
        }
    }
}
