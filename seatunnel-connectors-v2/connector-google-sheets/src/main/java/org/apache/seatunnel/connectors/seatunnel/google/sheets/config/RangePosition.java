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

package org.apache.seatunnel.connectors.seatunnel.google.sheets.config;

import lombok.Data;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Data
public class RangePosition implements Serializable {

    private String startX;
    private Integer startY;
    private String endX;
    private Integer endY;

    public RangePosition buildWithRange(String range) {
        RangePosition rangePosition = new RangePosition();
        String[] ranges = range.split(":");
        Pattern xPattern = Pattern.compile("[A-Z]+");
        Pattern yPattern = Pattern.compile("[0-9]+");
        Matcher startXMatch = xPattern.matcher(ranges[0]);
        if (startXMatch.find()) {
            rangePosition.setStartX(startXMatch.group());
        }
        Matcher startYMatch = yPattern.matcher(ranges[0]);
        if (startYMatch.find()) {
            rangePosition.setStartY(Integer.parseInt(startYMatch.group()));
        }
        Matcher endXMatch = xPattern.matcher(ranges[1]);
        if (endXMatch.find()) {
            rangePosition.setEndX(endXMatch.group());
        }
        Matcher endYMatch = yPattern.matcher(ranges[1]);
        if (endYMatch.find()) {
            rangePosition.setEndY(Integer.parseInt(endYMatch.group()));
        }
        return rangePosition;
    }
}
