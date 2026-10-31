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

package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.SerializationUtils;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;

public class GenerateLegacyChainedMapFixture {
    private static final String HEADER =
            "# Licensed to the Apache Software Foundation (ASF) under one or more\n"
                    + "# contributor license agreements.  See the NOTICE file distributed with\n"
                    + "# this work for additional information regarding copyright ownership.\n"
                    + "# The ASF licenses this file to You under the Apache License, Version 2.0\n"
                    + "# (the \"License\"); you may not use this file except in compliance with\n"
                    + "# the License.  You may obtain a copy of the License at\n"
                    + "#\n"
                    + "#    http://www.apache.org/licenses/LICENSE-2.0\n"
                    + "#\n"
                    + "# Unless required by applicable law or agreed to in writing, software\n"
                    + "# distributed under the License is distributed on an \"AS IS\" BASIS,\n"
                    + "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
                    + "# See the License for the specific language governing permissions and\n"
                    + "# limitations under the License.\n"
                    + "-----BEGIN BASE64-----\n";

    public static void main(String[] args) throws Exception {
        for (Field field : ChainedMapTransform.class.getDeclaredFields()) {
            if (field.getName().equals("extraTablePaths")
                    || field.getName().equals("serialVersionUID")) {
                throw new IllegalStateException("Load the pre-#12804 ChainedMapTransform first");
            }
        }
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        CatalogTable table =
                CatalogTableUtil.getCatalogTable("catalog", "database", "schema", "orders", rowType);
        ChainedMapTransform chain =
                new ChainedMapTransform(
                        Arrays.asList(new IdentityMapTransform(table), new IdentityMapTransform(table)));
        byte[] bytes = SerializationUtils.serialize(chain);
        String payload =
                HEADER
                        + new String(
                                Base64.getMimeEncoder(76, new byte[] {'\n'}).encode(bytes),
                                StandardCharsets.US_ASCII);
        Files.write(Paths.get(args[0]), payload.getBytes(StandardCharsets.US_ASCII));
    }
}
