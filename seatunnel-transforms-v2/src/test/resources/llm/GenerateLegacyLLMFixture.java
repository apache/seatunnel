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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.transform.nlpmodel.llm.LLMTransform;
import org.apache.seatunnel.transform.nlpmodel.llm.LLMTransformFactory;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GenerateLegacyLLMFixture {
    public static void main(String[] args) throws Exception {
        // Fail closed if accidentally invoked with the new class instead of the legacy classes.
        for (Field field : LLMTransform.class.getDeclaredFields()) {
            if (field.getName().equals("strictBooleanOutput")
                    || field.getName().equals("serialVersionUID")) {
                throw new IllegalStateException("Compile and load the pre-change LLM classes first");
            }
        }
        Map<String, Object> options = new HashMap<>();
        options.put("model_provider", "OPENAI");
        options.put("model", "test-model");
        options.put("api_key", "test-key");
        options.put("api_path", "http://127.0.0.1:1/legacy-fixture");
        options.put("prompt", "Classify the input");
        options.put("output_data_type", "BOOLEAN");
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.of("test", "input")),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "text",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "test input");
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Collections.singletonList(table),
                        ReadonlyConfig.fromMap(options),
                        Thread.currentThread().getContextClassLoader());
        byte[] bytes =
                SerializationUtils.serialize(
                        new LLMTransformFactory().createTransform(context).createTransform());
        Files.write(Paths.get(args[0]), Base64.getMimeEncoder(76, new byte[] {'\n'}).encode(bytes));
    }
}
