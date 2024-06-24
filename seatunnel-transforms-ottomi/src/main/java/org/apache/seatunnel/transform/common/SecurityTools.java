package org.apache.seatunnel.transform.common;

import java.security.MessageDigest;
import java.util.Base64;
import java.util.Random;

/**
 * @author xuzhengzhou
 * @date 2024/6/7 17:57
 */
public class SecurityTools {
    private static final int ROUNDS = 3;
    private static final int BLOCK_SIZE = 16;
    private static final String SECURITY_KEY = "ottomiKey";
    private static final String SECURITY_SALT = "ottomiSalt";

    // 生成动态轮次密钥
    private static String generateRoundKey(String key, int round) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update((key + round).getBytes());
        byte[] byteData = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : byteData) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    // 派生密钥
    private static char deriveKey(char c, char k, char s, int round) {
        return (char) ((c ^ k ^ s) + round);
    }

    // 加密一个块
    private static String encryptBlock(String block, String key, String salt, int round)
            throws Exception {
        String roundKey = generateRoundKey(key, round);
        StringBuilder encryptedBlock = new StringBuilder(block.length());
        for (int i = 0; i < block.length(); i++) {
            char c = block.charAt(i);
            char k = roundKey.charAt(i % roundKey.length());
            char s = salt.charAt(i % salt.length());
            encryptedBlock.append(deriveKey(c, k, s, round));
        }
        return encryptedBlock.toString();
    }

    // 解密一个块
    private static String decryptBlock(String block, String key, String salt, int round)
            throws Exception {
        String roundKey = generateRoundKey(key, round);
        StringBuilder decryptedBlock = new StringBuilder(block.length());
        for (int i = 0; i < block.length(); i++) {
            char c = block.charAt(i);
            char k = roundKey.charAt(i % roundKey.length());
            char s = salt.charAt(i % salt.length());
            decryptedBlock.append((char) ((c - round) ^ k ^ s));
        }
        return decryptedBlock.toString();
    }

    // 对明文进行加密
    public static String encrypt(String plainText) throws Exception {
        // 生成随机初始化向量 (IV)
        Random random = new Random();
        byte[] iv = new byte[BLOCK_SIZE];
        random.nextBytes(iv);
        String ivString = Base64.getEncoder().encodeToString(iv);

        // 填充以使得文本长度是块大小的倍数
        while (plainText.length() % BLOCK_SIZE != 0) {
            plainText += "\0";
        }

        StringBuilder encryptedText = new StringBuilder(ivString);

        // 分块并进行多轮加密
        String currentText = plainText;
        for (int round = 0; round < ROUNDS; round++) {
            encryptedText.setLength(ivString.length()); // 清空缓冲区，保留IV部分
            for (int i = 0; i < currentText.length(); i += BLOCK_SIZE) {
                String block = currentText.substring(i, i + BLOCK_SIZE);
                block = encryptBlock(block, SECURITY_KEY, SECURITY_SALT, round);
                encryptedText.append(block);
            }
            currentText = encryptedText.substring(ivString.length()); // 准备下一轮
        }

        return Base64.getEncoder().encodeToString(encryptedText.toString().getBytes());
    }

    // 对密文进行解密
    public static String decrypt(String encryptedText) throws Exception {
        String decodedText = new String(Base64.getDecoder().decode(encryptedText));

        // 提取初始化向量 (IV)
        String ivString = decodedText.substring(0, 24);
        decodedText = decodedText.substring(24);

        StringBuilder decryptedText = new StringBuilder();

        // 分块并进行多轮解密
        String currentText = decodedText;
        for (int round = ROUNDS - 1; round >= 0; round--) {
            decryptedText.setLength(0); // 清空缓冲区
            for (int i = 0; i < currentText.length(); i += BLOCK_SIZE) {
                String block = currentText.substring(i, i + BLOCK_SIZE);
                block = decryptBlock(block, SECURITY_KEY, SECURITY_SALT, round);
                decryptedText.append(block);
            }
            currentText = decryptedText.toString(); // 准备下一轮
        }

        // 去掉填充字符
        return currentText.replaceAll("\0+$", "");
    }
}
