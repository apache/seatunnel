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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;

class MarkdownReadStrategyTest {

    @Test
    public void testReadMarkdown() throws Exception {
        URL resource = this.getClass().getResource("/test.md");
        String path = Paths.get(resource.toURI()).toString();
        AbstractReadStrategy markdownReadStrategy = new MarkdownReadStrategy();
        TempCollector tempCollector = new TempCollector();
        markdownReadStrategy.read(path, "", tempCollector);

        Assertions.assertEquals(75, tempCollector.getRows().size());

        Assertions.assertEquals("Heading_1", tempCollector.getRows().get(0).getField(0));
        Assertions.assertEquals("Heading", tempCollector.getRows().get(0).getField(1));
        Assertions.assertEquals(1, tempCollector.getRows().get(0).getField(2));
        Assertions.assertEquals(
                "The Essential Guide to Groceries: Shopping, Storing, and Enjoying Food at Home",
                tempCollector.getRows().get(0).getField(3));
        Assertions.assertEquals(1, tempCollector.getRows().get(0).getField(4));
        Assertions.assertEquals(1, tempCollector.getRows().get(0).getField(5));
        Assertions.assertNull(tempCollector.getRows().get(0).getField(6));
        Assertions.assertNull(tempCollector.getRows().get(0).getField(7));

        Assertions.assertEquals("OrderedList_1", tempCollector.getRows().get(3).getField(0));
        Assertions.assertEquals("OrderedList", tempCollector.getRows().get(3).getField(1));
        Assertions.assertNull(tempCollector.getRows().get(3).getField(2));
        Assertions.assertEquals(
                "1. [Introduction](#introduction)\n"
                        + "2. [Grocery Categories](#grocery-categories)\n"
                        + "3. [Planning Your Grocery Trip](#planning-your-grocery-trip)\n"
                        + "4. [Shopping Tips for Savings](#shopping-tips-for-savings)\n"
                        + "5. [Storing and Organizing Groceries](#storing-and-organizing-groceries)\n"
                        + "6. [Healthy Choices](#healthy-choices)\n"
                        + "7. [Modern Grocery Trends](#modern-grocery-trends)\n"
                        + "8. [Comparison Table](#comparison-table)\n"
                        + "9. [Conclusion](#conclusion)\n",
                tempCollector.getRows().get(3).getField(3));
        Assertions.assertEquals(1, tempCollector.getRows().get(3).getField(4));
        Assertions.assertEquals(5, tempCollector.getRows().get(3).getField(5));
        Assertions.assertNull(tempCollector.getRows().get(3).getField(6));
        Assertions.assertEquals(
                "OrderedListItem_1,OrderedListItem_2,OrderedListItem_3,OrderedListItem_4,OrderedListItem_5,OrderedListItem_6,OrderedListItem_7,OrderedListItem_8,OrderedListItem_9",
                tempCollector.getRows().get(3).getField(7));

        Assertions.assertEquals("OrderedListItem_1", tempCollector.getRows().get(4).getField(0));
        Assertions.assertEquals("OrderedListItem", tempCollector.getRows().get(4).getField(1));
        Assertions.assertNull(tempCollector.getRows().get(4).getField(2));
        Assertions.assertEquals(
                "[Introduction](#introduction)", tempCollector.getRows().get(4).getField(3));
        Assertions.assertEquals(1, tempCollector.getRows().get(4).getField(4));
        Assertions.assertEquals(1, tempCollector.getRows().get(4).getField(5));
        Assertions.assertEquals("OrderedList_1", tempCollector.getRows().get(4).getField(6));
        Assertions.assertNull(tempCollector.getRows().get(4).getField(7));
    }
}
