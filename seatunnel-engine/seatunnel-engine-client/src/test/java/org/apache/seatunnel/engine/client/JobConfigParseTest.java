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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.core.dag.actions.Action;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

@RunWith(JUnit4.class)
public class JobConfigParseTest {

    @Test
    public void testParse() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = this.getClass().getResource("/fakesource_to_file.conf").getFile();
        JobConfigParse jobConfigParse = new JobConfigParse(filePath);
        List<Action> actions = jobConfigParse.parse();
        Assert.assertEquals(1, actions.size());

        Assert.assertEquals("LocalFile", actions.get(0).name());
        Assert.assertEquals(1, actions.get(0).upstream().size());
        Assert.assertEquals("FakeSource", actions.get(0).upstream().get(0).name());
    }
}
