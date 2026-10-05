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

package org.apache.seatunnel.connectors.seatunnel.file.config;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Document element structure, used to represent various elements (titles, paragraphs, etc.) in a
 * document
 */
public class DocumentElement {

    /** Element Unique Identifier */
    private String elementId;

    /** Element types: heading, paragraph, link, image, etc. */
    private String elementType;

    /** Title level, only valid when elementType is heading */
    private Integer headingLevel;

    /** Element text content */
    private String text;

    /** Page numbers, page numbers in PDF files, starting from 1 */
    private Integer pageNumber;

    /** Position index in the document, starting from 0 */
    private Integer positionIndex;

    /** Parent element ID, used to construct the hierarchy */
    private String parentId;

    /** List of child Element ids */
    private List<String> childIds;

    public DocumentElement() {
        this.elementId = "uuid-elem-" + UUID.randomUUID().toString();
        this.childIds = new ArrayList<>();
    }

    public DocumentElement(String elementType, String text) {
        this();
        this.elementType = elementType;
        this.text = text;
    }

    public DocumentElement(
            String elementType, String text, Integer pageNumber, Integer positionIndex) {
        this(elementType, text);
        this.pageNumber = pageNumber;
        this.positionIndex = positionIndex;
    }

    // Getters and Setters
    public String getElementId() {
        return elementId;
    }

    public void setElementId(String elementId) {
        this.elementId = elementId;
    }

    public String getElementType() {
        return elementType;
    }

    public void setElementType(String elementType) {
        this.elementType = elementType;
    }

    public Integer getHeadingLevel() {
        return headingLevel;
    }

    public void setHeadingLevel(Integer headingLevel) {
        this.headingLevel = headingLevel;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Integer getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(Integer pageNumber) {
        this.pageNumber = pageNumber;
    }

    public Integer getPositionIndex() {
        return positionIndex;
    }

    public void setPositionIndex(Integer positionIndex) {
        this.positionIndex = positionIndex;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public List<String> getChildIds() {
        return childIds;
    }

    public void setChildIds(List<String> childIds) {
        this.childIds = childIds;
    }

    /** Add the child element ID */
    public void addChildId(String childId) {
        if (this.childIds == null) {
            this.childIds = new ArrayList<>();
        }
        this.childIds.add(childId);
    }

    /** Remove the child element ID */
    public void removeChildId(String childId) {
        if (this.childIds != null) {
            this.childIds.remove(childId);
        }
    }

    @Override
    public String toString() {
        return "DocumentElement{"
                + "elementId='"
                + elementId
                + '\''
                + ", elementType='"
                + elementType
                + '\''
                + ", headingLevel="
                + headingLevel
                + ", text='"
                + (text != null && text.length() > 50 ? text.substring(0, 50) + "..." : text)
                + '\''
                + ", pageNumber="
                + pageNumber
                + ", positionIndex="
                + positionIndex
                + ", parentId='"
                + parentId
                + '\''
                + ", childIds="
                + childIds
                + '}';
    }

    public SeaTunnelRow toSeaTunnelRow() {
        return new SeaTunnelRow(
                new Object[] {
                    elementId,
                    elementType,
                    headingLevel,
                    text,
                    pageNumber,
                    positionIndex,
                    parentId,
                    childIds == null || childIds.isEmpty() ? null : childIds.toArray(new String[0])
                });
    }
}
