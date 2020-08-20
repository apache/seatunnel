package io.github.interestinglab.waterdrop.utils;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;

public class AsciiArt {

    /**
     * Print ASCII art of string
     * @param str
     * */
    public static void printAsciiArtOnFailure(String str) {

        final int width = 144;
        final int height = 32;
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics g = image.getGraphics();
        g.setFont(new Font("Dialog", Font.PLAIN, 24));
        Graphics2D graphics = (Graphics2D) g;
        graphics.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING,
                RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        graphics.drawString("Waterdrop", 6, 24);

        for (int y = 0; y < height; y++) {
            StringBuilder sb = new StringBuilder();
            for (int x = 0; x < width; x++)
                sb.append(image.getRGB(x, y) == -16777216 ? " " : image.getRGB(x, y) == -1 ? "#" : "*");

            if (sb.toString().trim().isEmpty()) continue;
            System.out.println(sb);
        }
    }

    public static void printAsciiArt(String str) throws Exception {
        String name = System.getProperty("user.dir") + "/plugins/script/files/slant.flf";
        File file = new File(name);
        String asciiArt = FigletFont.convertOneLine(file, "Waterdrop");
        System.out.println("Welcome to");
        System.out.println(asciiArt);
    }

    public static void main(String[] args) throws Exception {
        String name = System.getProperty("user.dir");
        System.out.println("name = " + name);
        printAsciiArtOnFailure("Waterdrop");
    }

}
