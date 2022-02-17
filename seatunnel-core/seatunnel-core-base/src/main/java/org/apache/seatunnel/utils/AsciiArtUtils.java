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

package org.apache.seatunnel.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;

public final class AsciiArtUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsciiArtUtils.class);

    private static final int FONT_SIZE = 24;
    private static final int DRAW_X = 6;
    private static final int RGB = -16777216;

    private AsciiArtUtils() {
    }

    /**
     * Print ASCII art of string
     *
     * @param str str
     */
    public static void printAsciiArt(String str) {

        final int width = 144;
        final int height = 32;
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics g = image.getGraphics();
        g.setFont(new Font("Dialog", Font.PLAIN, FONT_SIZE));
        Graphics2D graphics = (Graphics2D) g;
        graphics.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING,
                RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        graphics.drawString(str, DRAW_X, FONT_SIZE);

        for (int y = 0; y < height; y++) {
            StringBuilder sb = new StringBuilder();
            for (int x = 0; x < width; x++) {
                sb.append(image.getRGB(x, y) == RGB ? " " : image.getRGB(x, y) == -1 ? "#" : "*");
            }
            if (sb.toString().trim().isEmpty()) {
                continue;
            }
            LOGGER.info(String.valueOf(sb));
        }
    }
}
