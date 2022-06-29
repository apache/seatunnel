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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.Constant;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.FileSinkPartitionDirNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.PartitionDirNameGenerator;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
public class TestFileSinkPartitionDirNameGenerator {

    @SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:RegexpSingleline"})
    @Test
    public void testPartitionDirNameGenerator() {
        String[] fieldNames = new String[]{"c1", "c2", "c3", "c4"};
        SeaTunnelDataType[] seaTunnelDataTypes = new SeaTunnelDataType[]{BasicType.BOOLEAN_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE};
        SeaTunnelRowType seaTunnelRowTypeInfo = new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);

        Object[] row1 = new Object[]{true, 1, "test", 3};
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(row1);

        List<String> partitionFieldList = new ArrayList<>();
        partitionFieldList.add("c3");
        partitionFieldList.add("c4");

        List<Integer> partitionFieldsIndexInRow = new ArrayList<>();
        partitionFieldsIndexInRow.add(2);
        partitionFieldsIndexInRow.add(3);

        PartitionDirNameGenerator partitionDirNameGenerator = new FileSinkPartitionDirNameGenerator(partitionFieldList, partitionFieldsIndexInRow, "${v0}/${v1}");
        String partitionDir = partitionDirNameGenerator.generatorPartitionDir(seaTunnelRow);
        Assert.assertEquals("test/3", partitionDir);

        partitionDirNameGenerator = new FileSinkPartitionDirNameGenerator(partitionFieldList, partitionFieldsIndexInRow, "${k0}=${v0}/${k1}=${v1}");
        partitionDir = partitionDirNameGenerator.generatorPartitionDir(seaTunnelRow);
        Assert.assertEquals("c3=test/c4=3", partitionDir);

        partitionDirNameGenerator = new FileSinkPartitionDirNameGenerator(null, null, "${k0}=${v0}/${k1}=${v1}");
        partitionDir = partitionDirNameGenerator.generatorPartitionDir(seaTunnelRow);
        Assert.assertEquals(Constant.NON_PARTITION, partitionDir);
    }
}
