package io.github.interestinglab.waterdrop.utils;

import java.io.*;
import java.net.URL;
import java.util.StringTokenizer;

public class FigletFont {
    public char hardblank;
    public int height = -1;
    public int heightWithoutDescenders = -1;
    public int maxLine = -1;
    public int smushMode = -1;
    public char font[][][] = null;
    public String fontName = "";
    final public static int MAX_CHARS = 1024;
    final public static int REGULAR_CHARS = 102;

    public char[][][] getFont() {
        return font;
    }

    public char[][] getChar(int c) {
        return font[c];
    }

    public String getCharLineString(int c, int l) {
        if (font[c][l] == null)
            return null;
        else {
            return new String(font[c][l]);
        }
    }

    public FigletFont(InputStream stream) throws IOException {
        font = new char[MAX_CHARS][][];
        BufferedReader data = null;
        String dummyS;
        int dummyI;
        int charCode;

        try {

            data = new BufferedReader(new InputStreamReader(new BufferedInputStream(stream), "UTF-8"));

            dummyS = data.readLine();
            StringTokenizer st = new StringTokenizer(dummyS, " ");
            String s = st.nextToken();
            hardblank = s.charAt(s.length() - 1);
            height = Integer.parseInt(st.nextToken());
            heightWithoutDescenders = Integer.parseInt(st.nextToken());
            maxLine = Integer.parseInt(st.nextToken());
            smushMode = Integer.parseInt(st.nextToken());
            dummyI = Integer.parseInt(st.nextToken());

            if (dummyI > 0) {
                st = new StringTokenizer(data.readLine(), " ");
                if (st.hasMoreElements())
                    fontName = st.nextToken();
            }

            int[] charsTo = new int[REGULAR_CHARS];

            int j = 0;
            for (int c = 32; c <= 126; ++c) {
                charsTo[j++] = c;
            }
            for (int additional : new int[] { 196, 214, 220, 228, 246, 252, 223 }) {
                charsTo[j++] = additional;
            }

            for (int i = 0; i < dummyI - 1; i++)
                dummyS = data.readLine();
            int charPos = 0;
            while (dummyS != null) {
                if (charPos < REGULAR_CHARS) {
                    charCode = charsTo[charPos++];
                } else {
                    dummyS = data.readLine();
                    if (dummyS == null) {
                        continue;
                    }
                    charCode = convertCharCode(dummyS);
                }
                for (int h = 0; h < height; h++) {
                    dummyS = data.readLine();
                    if (dummyS != null) {
                        if (h == 0)
                            font[charCode] = new char[height][];
                        int t = dummyS.length() - 1 - ((h == height - 1) ? 1 : 0);
                        if (height == 1)
                            t++;
                        font[charCode][h] = new char[t];
                        for (int l = 0; l < t; l++) {
                            char a = dummyS.charAt(l);
                            font[charCode][h][l] = (a == hardblank) ? ' ' : a;
                        }
                    }
                }
            }
        } finally {
            if (data != null) {
                data.close();
            }
        }
    }

    int convertCharCode(String input) {
        String codeTag = input.concat(" ").split(" ")[0];
        if (codeTag.matches("^0[xX][0-9a-fA-F]+$")) {
            return Integer.parseInt(codeTag.substring(2), 16);
        } else if (codeTag.matches("^0[0-7]+$")) {
            return Integer.parseInt(codeTag.substring(1), 8);
        } else {
            return Integer.parseInt(codeTag);
        }
    }

    public String convert(String message) {
        String result = "";
        for (int l = 0; l < this.height; l++) { // for each line
            for (int c = 0; c < message.length(); c++)
                // for each char
                result += this.getCharLineString((int) message.charAt(c), l);
            result += '\n';
        }
        return result;
    }

    public static String convertOneLine(InputStream fontFileStream, String message) throws IOException {
        return new FigletFont(fontFileStream).convert(message);
    }

    public static String convertOneLine(String message) throws IOException {
        return convertOneLine(FigletFont.class.getClassLoader().getResourceAsStream("standard.flf"), message);
    }

    public static String convertOneLine(File fontFile, String message) throws IOException {
        return convertOneLine(new FileInputStream(fontFile), message);
    }

    public static String convertOneLine(String fontPath, String message) throws IOException {
        InputStream fontStream = null;
        if (fontPath.startsWith("classpath:")) {
            fontStream = FigletFont.class.getResourceAsStream(fontPath.substring(10));
        } else if (fontPath.startsWith("http://") || fontPath.startsWith("https://")) {
            fontStream = new URL(fontPath).openStream();
        } else {
            fontStream = new FileInputStream(fontPath);
        }
        return convertOneLine(fontStream, message);
    }
}
