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

package org.apache.seatunnel.connectors.seatunnel.http.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.jayway.jsonpath.JsonPath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Regression test for {@link JsonPathProcessorImpl#processJsonData}.
 *
 * <p>When {@code json_filed_missed_return_null} is enabled the processor intentionally skips {@code
 * validateResultsConsistency()}, because a missing field legitimately yields a shorter result list.
 * But the subsequent {@code dataFlip()} still assumes every per-path list has the same length as
 * the first one and indexes into the already-built rows without a bound check:
 *
 * <pre>
 *   List&lt;String&gt; row = datas.get(j);   // j runs to result.size() of a *later* path
 * </pre>
 *
 * <p>So a response where the first configured field matches fewer elements than a later field
 * throws {@link IndexOutOfBoundsException} instead of producing rows. The reported symptom is a job
 * crash on perfectly valid JSON, e.g. {@code $.items[*].id} matching 1 element while {@code
 * $.items[*].name} matches 2.
 */
public class JsonPathProcessorDataFlipTest {

    /** Builds a processor with the "missing field returns null" flag enabled. */
    private JsonPathProcessorImpl processorWithNullForMissing() {
        JsonPathProcessorImpl processor = new JsonPathProcessorImpl();
        processor.setJsonFiledMissedReturnNull(true);
        return processor;
    }

    @Test
    public void shorterFirstPathMustNotThrowIndexOutOfBounds() {
        // $.a[*] yields 1 element, $.b[*] yields 2.
        String json = "{\"a\":[1],\"b\":[10,20]}";

        JsonPath[] paths = new JsonPath[] {JsonPath.compile("$.a[*]"), JsonPath.compile("$.b[*]")};

        JsonPathProcessorImpl processor = processorWithNullForMissing();

        // Must not throw; a ragged result is acceptable, a crash is not.
        List<List<String>> result =
                Assertions.assertDoesNotThrow(
                        () -> processor.processJsonData(JsonPath.parse(json), paths),
                        "Unequal path cardinality must not crash the reader");

        Assertions.assertEquals(
                Arrays.asList(Arrays.asList("1", "10"), Arrays.asList(null, "20")),
                result,
                "Rows stay index-aligned: the shorter first path is padded with null");
    }

    @Test
    public void shorterLaterPathMustNotThrowIndexOutOfBounds() {
        // $.a[*] yields 2 elements, $.b[*] yields 1 (the mirror case).
        String json = "{\"a\":[1,2],\"b\":[10]}";

        JsonPath[] paths = new JsonPath[] {JsonPath.compile("$.a[*]"), JsonPath.compile("$.b[*]")};

        JsonPathProcessorImpl processor = processorWithNullForMissing();

        List<List<String>> result =
                Assertions.assertDoesNotThrow(
                        () -> processor.processJsonData(JsonPath.parse(json), paths),
                        "Unequal path cardinality must not crash the reader");

        Assertions.assertEquals(
                Arrays.asList(Arrays.asList("1", "10"), Arrays.asList("2", null)),
                result,
                "Rows stay index-aligned: the shorter later path is padded with null");
    }

    @Test
    public void equalCardinalityStillProducesAlignedRows() {
        String json = "{\"a\":[1,2],\"b\":[10,20]}";

        JsonPath[] paths = new JsonPath[] {JsonPath.compile("$.a[*]"), JsonPath.compile("$.b[*]")};

        List<List<String>> result =
                processorWithNullForMissing().processJsonData(JsonPath.parse(json), paths);

        List<List<String>> expected = new ArrayList<>();
        expected.add(Arrays.asList("1", "10"));
        expected.add(Arrays.asList("2", "20"));

        Assertions.assertEquals(expected, result);
    }

    @Test
    public void missingFieldPadsWithNullInsteadOfShifting() {
        // Second path matches nothing at all; rows must stay aligned by index, padded with null.
        String json = "{\"a\":[1,2],\"b\":[]}";

        JsonPath[] paths = new JsonPath[] {JsonPath.compile("$.a[*]"), JsonPath.compile("$.b[*]")};

        List<List<String>> result =
                processorWithNullForMissing().processJsonData(JsonPath.parse(json), paths);

        Assertions.assertEquals(2, result.size(), "One row per element of the longest path");
        Assertions.assertEquals("1", result.get(0).get(0), "first column keeps original order");
        Assertions.assertEquals("2", result.get(1).get(0), "first column keeps original order");
        Assertions.assertNull(result.get(0).get(1), "missing second column is padded with null");
        Assertions.assertNull(result.get(1).get(1), "missing second column is padded with null");
    }

    @Test
    public void factoryRoutedScalarFirstPathPadsRaggedRows() {
        // A first path without [*] is what the factory routes to this processor; the
        // [*]-first reproduction in the PR description actually goes to ArrayJsonPathProcessor.
        // The first path must still read a list: a scalar path such as $.id returns a bare
        // Integer that this processor rejects before and after the padding change.
        String json = "{\"a\":[1],\"b\":[10,20]}";

        JsonPath[] paths =
                new JsonPath[] {
                    JsonPath.compile("$.a"), JsonPath.compile("$.b[*]")
                };

        JsonPathProcessor processor = JsonPathProcessorFactory.getProcessor(paths, true);
        Assertions.assertInstanceOf(
                JsonPathProcessorImpl.class,
                processor,
                "A scalar first path must reach JsonPathProcessorImpl through the factory");

        List<List<String>> result = processor.processJsonData(JsonPath.parse(json), paths);

        Assertions.assertEquals(
                Arrays.asList(Arrays.asList("1", "10"), Arrays.asList(null, "20")),
                result,
                "A short first column is not broadcast; it is null for the remaining rows");
    }

    @Test
    public void factoryStillRoutesArrayFirstPathToTheArrayProcessor() {
        JsonPath[] arrayPaths = new JsonPath[] {JsonPath.compile("$.a[*]")};
        JsonPathProcessor processor = JsonPathProcessorFactory.getProcessor(arrayPaths, true);

        Assertions.assertInstanceOf(
                ArrayJsonPathProcessor.class,
                processor,
                "A [*] first path keeps its dedicated processor");
    }
}
