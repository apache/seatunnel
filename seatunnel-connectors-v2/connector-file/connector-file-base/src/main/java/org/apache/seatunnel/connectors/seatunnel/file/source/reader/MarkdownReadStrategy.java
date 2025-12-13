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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import com.vladsch.flexmark.ast.BlockQuote;
import com.vladsch.flexmark.ast.BulletList;
import com.vladsch.flexmark.ast.Code;
import com.vladsch.flexmark.ast.FencedCodeBlock;
import com.vladsch.flexmark.ast.Heading;
import com.vladsch.flexmark.ast.Image;
import com.vladsch.flexmark.ast.Link;
import com.vladsch.flexmark.ast.ListItem;
import com.vladsch.flexmark.ast.OrderedList;
import com.vladsch.flexmark.ast.Paragraph;
import com.vladsch.flexmark.ast.ThematicBreak;
import com.vladsch.flexmark.ext.tables.TableBlock;
import com.vladsch.flexmark.ext.tables.TableCell;
import com.vladsch.flexmark.ext.tables.TableRow;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.ast.Node;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class MarkdownReadStrategy extends AbstractReadStrategy {

    private static final int DEFAULT_PAGE_NUMBER = 1;
    private static final int DEFAULT_POSITION = 1;

    private static class NodeInfo {
        String elementId;
        String parentId;
        List<String> childIds = new ArrayList<>();
        int positionIndex;

        NodeInfo(String elementId, String parentId, int positionIndex) {
            this.elementId = elementId;
            this.parentId = parentId;
            this.positionIndex = positionIndex;
        }
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        String markdown = new String(Files.readAllBytes(Paths.get(path)));
        Parser parser = Parser.builder().build();
        Node document = parser.parse(markdown);

        Map<Node, NodeInfo> nodeInfoMap = new IdentityHashMap<>();
        Map<String, Integer> typeCounters = new HashMap<>();
        List<SeaTunnelRow> rows = new ArrayList<>();

        assignIdsAndCollectTree(document, null, nodeInfoMap, DEFAULT_POSITION, typeCounters);
        generateRows(document, rows, nodeInfoMap, DEFAULT_PAGE_NUMBER);

        for (SeaTunnelRow row : rows) {
            output.collect(row);
        }
    }

    private void assignIdsAndCollectTree(
            Node node,
            Node parent,
            Map<Node, NodeInfo> nodeInfoMap,
            int position,
            Map<String, Integer> typeCounters) {
        String elementType = node.getClass().getSimpleName();
        String elementId = null;

        if (isEligibleForRow(node)) {
            int count = typeCounters.getOrDefault(elementType, 0) + 1;
            typeCounters.put(elementType, count);
            elementId = elementType + "_" + count;
        }

        String parentId = parent == null ? null : nodeInfoMap.get(parent).elementId;
        NodeInfo nodeInfo = new NodeInfo(elementId, parentId, position);
        nodeInfoMap.put(node, nodeInfo);

        int childPosition = 1;
        for (Node child = node.getFirstChild(); child != null; child = child.getNext()) {
            assignIdsAndCollectTree(child, node, nodeInfoMap, childPosition++, typeCounters);
            NodeInfo childInfo = nodeInfoMap.get(child);
            if (childInfo.elementId != null) {
                nodeInfo.childIds.add(childInfo.elementId);
            }
        }
    }

    private void generateRows(
            Node node, List<SeaTunnelRow> rows, Map<Node, NodeInfo> nodeInfoMap, int pageNumber) {
        if (isEligibleForRow(node)) {
            NodeInfo nodeInfo = nodeInfoMap.get(node);
            String elementType = node.getClass().getSimpleName();
            Integer headingLevel = null;
            String text = extractValue(node);

            if (node instanceof Heading) {
                headingLevel = ((Heading) node).getLevel();
            }

            rows.add(
                    new SeaTunnelRow(
                            new Object[] {
                                nodeInfo.elementId,
                                elementType,
                                headingLevel,
                                text,
                                pageNumber,
                                nodeInfo.positionIndex,
                                nodeInfo.parentId,
                                nodeInfo.childIds.isEmpty()
                                        ? null
                                        : String.join(",", nodeInfo.childIds)
                            }));
            log.debug(
                    "Added row: element_id={} type={} heading_level={} text={} parent_id={} child_ids={}",
                    nodeInfo.elementId,
                    elementType,
                    headingLevel,
                    text,
                    nodeInfo.parentId,
                    nodeInfo.childIds);
        }

        for (Node child = node.getFirstChild(); child != null; child = child.getNext()) {
            generateRows(child, rows, nodeInfoMap, pageNumber);
        }
    }

    private boolean isEligibleForRow(Node node) {
        if (node instanceof Paragraph) {
            Node parent = node.getParent();
            if (parent instanceof ListItem || parent instanceof BlockQuote) {
                return false;
            }
        }

        return node instanceof Heading
                || node instanceof Paragraph
                || node instanceof ListItem
                || node instanceof BulletList
                || node instanceof OrderedList
                || node instanceof BlockQuote
                || node instanceof FencedCodeBlock
                || node instanceof TableBlock;
    }

    private String extractValue(Node node) {
        if (node instanceof ListItem) {
            return extractTextFromChildren(node);
        } else if (node instanceof Heading || node instanceof Paragraph) {
            return extractTextFromChildren(node);
        } else if (node instanceof BulletList) {
            return bulletListToString((BulletList) node);
        } else if (node instanceof OrderedList) {
            return orderedListToString((OrderedList) node);
        } else if (node instanceof Code) {
            return ((Code) node).getText().toString();
        } else if (node instanceof FencedCodeBlock) {
            return ((FencedCodeBlock) node).getContentChars().toString();
        } else if (node instanceof BlockQuote) {
            return extractTextFromChildren(node);
        } else if (node instanceof ThematicBreak) {
            return "---";
        } else if (node instanceof Link) {
            return ((Link) node).getUrl().toString();
        } else if (node instanceof Image) {
            return ((Image) node).getUrl().toString();
        } else if (node instanceof TableBlock) {
            return tableToString((TableBlock) node);
        }

        return node.getChars().toString();
    }

    private String extractTextFromChildren(Node node) {
        StringBuilder sb = new StringBuilder();
        for (Node child = node.getFirstChild(); child != null; child = child.getNext()) {
            sb.append(child.getChars());
        }

        return sb.toString().trim();
    }

    private String bulletListToString(BulletList list) {
        StringBuilder sb = new StringBuilder();
        for (Node item = list.getFirstChild(); item != null; item = item.getNext()) {
            if (item instanceof ListItem) {
                sb.append("- ").append(extractTextFromChildren(item)).append("\n");
            }
        }

        return sb.toString();
    }

    private String orderedListToString(OrderedList list) {
        StringBuilder sb = new StringBuilder();
        int num = 1;
        for (Node item = list.getFirstChild(); item != null; item = item.getNext()) {
            if (item instanceof ListItem) {
                sb.append(num++).append(". ").append(extractTextFromChildren(item)).append("\n");
            }
        }

        return sb.toString();
    }

    private String tableToString(TableBlock table) {
        StringBuilder sb = new StringBuilder();
        for (Node row = table.getFirstChild(); row != null; row = row.getNext()) {
            if (row instanceof TableRow) {
                for (Node cell = row.getFirstChild(); cell != null; cell = cell.getNext()) {
                    if (cell instanceof TableCell) {
                        sb.append(((TableCell) cell).getText().toString()).append(" | ");
                    }
                }
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        return new SeaTunnelRowType(
                new String[] {
                    "element_id",
                    "element_type",
                    "heading_level",
                    "text",
                    "page_number",
                    "position_index",
                    "parent_id",
                    "child_ids"
                },
                new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE
                });
    }
}
