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

package org.apache.seatunnel.transform.sql.zeta.functions.udf.chineseToNum;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** */
@Slf4j
public class CNToNumber {

    /** 中文数字 */
    private static final String allChineseNum = "零一二三四五六七八九壹贰叁肆伍陆柒捌玖十拾百佰千仟万萬亿兆京";

    private static final String SIMPLE_ONE = "一";
    private static final String SIMPLE_TWO = "二";
    private static final String SIMPLE_THREE = "三";
    private static final String SIMPLE_FOUR = "四";
    private static final String SIMPLE_FIVE = "五";
    private static final String SIMPLE_SIX = "六";
    private static final String SIMPLE_SEVEN = "七";
    private static final String SIMPLE_EIGHT = "八";
    private static final String SIMPLE_NINE = "九";
    private static final String SIMPLE_TEN = "十";
    private static final String SIMPLE_HUNDRED = "百";
    private static final String SIMPLE_THOUSAND = "千";
    private static final String SIMPLE_TEN_THOUSAND = "万";
    private static final String TRADITION_ONE = "壹";
    private static final String TRADITION_TWO = "贰";
    private static final String TRADITION_THREE = "叁";
    private static final String TRADITION_FOUR = "肆";
    private static final String TRADITION_FIVE = "伍";
    private static final String TRADITION_SIX = "陆";
    private static final String TRADITION_SEVEN = "柒";
    private static final String TRADITION_EIGHT = "捌";
    private static final String TRADITION_NINE = "玖";
    private static final String TRADITION_TEN = "拾";
    private static final String TRADITION_HUNDRED = "佰";
    private static final String TRADITION_THOUSAND = "仟";
    private static final String TRADITION_TEN_THOUSAND = "萬";
    private static final String HUNDRED_MILLION = "亿";
    private static final String HUNDRED_BILLION = "兆";
    private static final String HUNDRED_TRILLION = "京";
    private static final String ZERO = "零";

    private static HashMap<String, Long> chMap = new HashMap<>();

    static {
        chMap.put(ZERO, 0L);
        chMap.put(SIMPLE_ONE, 1L);
        chMap.put(SIMPLE_TWO, 2L);
        chMap.put(SIMPLE_THREE, 3L);
        chMap.put(SIMPLE_FOUR, 4L);
        chMap.put(SIMPLE_FIVE, 5L);
        chMap.put(SIMPLE_SIX, 6L);
        chMap.put(SIMPLE_SEVEN, 7L);
        chMap.put(SIMPLE_EIGHT, 8L);
        chMap.put(SIMPLE_NINE, 9L);
        chMap.put(TRADITION_ONE, 1L);
        chMap.put(TRADITION_TWO, 2L);
        chMap.put(TRADITION_THREE, 3L);
        chMap.put(TRADITION_FOUR, 4L);
        chMap.put(TRADITION_FIVE, 5L);
        chMap.put(TRADITION_SIX, 6L);
        chMap.put(TRADITION_SEVEN, 7L);
        chMap.put(TRADITION_EIGHT, 8L);
        chMap.put(TRADITION_NINE, 9L);
        chMap.put(SIMPLE_TEN, 10L);
        chMap.put(TRADITION_TEN, 10L);
        chMap.put(SIMPLE_HUNDRED, 100L);
        chMap.put(TRADITION_HUNDRED, 100L);
        chMap.put(SIMPLE_THOUSAND, 1000L);
        chMap.put(TRADITION_THOUSAND, 1000L);
        chMap.put(SIMPLE_TEN_THOUSAND, 10000L);
        chMap.put(TRADITION_TEN_THOUSAND, 10000L);
        chMap.put(HUNDRED_MILLION, 100000000L);
        chMap.put(HUNDRED_BILLION, 1000000000000L);
        chMap.put(HUNDRED_TRILLION, 10000000000000000L);
    }

    /**
     * 判断传入的字符串是否全是汉字数字和单位
     *
     * @param chineseStr;
     * @return boolean
     */
    private static boolean isCnNumAll(String chineseStr) {
        char[] charArray = chineseStr.toCharArray();
        for (char c : charArray) {
            if (!allChineseNum.contains(String.valueOf(c))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 中文数字转换为阿拉伯数字
     *
     * @param chineseNum
     * @return
     */
    public static Long convertToArabic(String chineseNum) {
        if (!isCnNumAll(chineseNum)) {
            throw new RuntimeException(
                    "Non-Chinese numeral characters detected in the input string:" + chineseNum);
        }
        // 去掉所有的'零'
        chineseNum = chineseNum.replace("零", "");
        if (chineseNum.equals("")) {
            return 0l;
        }
        chineseNum = convertSimpleToTraditionalChinese(chineseNum);
        List<Long> list = new ArrayList<>();
        segmentNum(chineseNum, list);
        long result = 0;
        for (Long num : list) {
            result += num;
        }
        return result;
    }

    /**
     * 简体中文转换为繁体中文
     *
     * @param chineseNum
     * @return
     */
    private static String convertSimpleToTraditionalChinese(String chineseNum) {
        return chineseNum
                .replace(SIMPLE_TEN, TRADITION_TEN)
                .replace(SIMPLE_HUNDRED, TRADITION_HUNDRED)
                .replace(SIMPLE_THOUSAND, TRADITION_THOUSAND)
                .replace(SIMPLE_TEN_THOUSAND, TRADITION_TEN_THOUSAND)
                .replace(SIMPLE_ONE, TRADITION_ONE)
                .replace(SIMPLE_TWO, TRADITION_TWO)
                .replace(SIMPLE_THREE, TRADITION_THREE)
                .replace(SIMPLE_FOUR, TRADITION_FOUR)
                .replace(SIMPLE_FIVE, TRADITION_FIVE)
                .replace(SIMPLE_SIX, TRADITION_SIX)
                .replace(SIMPLE_SEVEN, TRADITION_SEVEN)
                .replace(SIMPLE_EIGHT, TRADITION_EIGHT)
                .replace(SIMPLE_NINE, TRADITION_NINE);
    }

    /**
     * 按京，兆，亿，万单位进行切分
     *
     * @param chineseNum
     * @param list
     */
    private static void segmentNum(String chineseNum, List<Long> list) {
        String chineseUnit = getChineseSegmentUnit(chineseNum);
        long unit = chMap.get(chineseUnit);
        String substring;
        if (unit == 1 && chineseNum != null) {
            substring = chineseNum;
        } else {
            substring = chineseNum.substring(0, chineseNum.indexOf(chineseUnit));
        }
        trans(substring, unit, list);
        if (!chineseNum.endsWith(chineseUnit) && unit > 1) {
            chineseNum = chineseNum.substring(chineseNum.indexOf(chineseUnit) + 1);
            segmentNum(chineseNum, list);
        }
    }

    /**
     * 处理以分段的数字
     *
     * @param chineseNum 数字
     * @param unit 单位
     * @param list
     */
    private static void trans(String chineseNum, Long unit, List<Long> list) {
        // 判断十开头的情况
        if (chineseNum.startsWith(TRADITION_TEN)) {
            chineseNum = TRADITION_ONE + chineseNum;
        }
        String chineseUnit = getChineseUnit(chineseNum);
        if (chineseNum.length() <= 1) {
            list.add(chMap.get(chineseNum) * unit);
            return;
        }
        String number = chineseNum.substring(0, chineseNum.indexOf(chineseUnit));
        list.add(chMap.get(number) * chMap.get(chineseUnit) * unit);
        if (!chineseNum.endsWith(chineseUnit)) {
            chineseNum = chineseNum.substring(chineseNum.indexOf(chineseUnit) + 1);
            trans(chineseNum, unit, list);
        }
    }

    /**
     * 返回数字中”仟佰拾“中最大的单位
     *
     * @param chineseNum
     * @return
     */
    private static String getChineseUnit(String chineseNum) {
        if (chineseNum.contains(TRADITION_THOUSAND) || chineseNum.contains(SIMPLE_THOUSAND)) {
            return TRADITION_THOUSAND;
        } else if (chineseNum.contains(TRADITION_HUNDRED) || chineseNum.contains(SIMPLE_HUNDRED)) {
            return TRADITION_HUNDRED;
        } else if (chineseNum.contains(TRADITION_TEN) || chineseNum.contains(SIMPLE_TEN)) {
            return TRADITION_TEN;
        }
        return TRADITION_ONE;
    }

    /**
     * 返回数字中”京兆亿萬“中最大的单位
     *
     * @param chineseNum
     * @return
     */
    private static String getChineseSegmentUnit(String chineseNum) {
        if (chineseNum.contains(HUNDRED_TRILLION)) {
            return HUNDRED_TRILLION;
        } else if (chineseNum.contains(HUNDRED_BILLION)) {
            return HUNDRED_BILLION;
        } else if (chineseNum.contains(HUNDRED_MILLION)) {
            return HUNDRED_MILLION;
        } else if (chineseNum.contains(TRADITION_TEN_THOUSAND)
                || chineseNum.contains(SIMPLE_TEN_THOUSAND)) {
            return TRADITION_TEN_THOUSAND;
        }
        return TRADITION_ONE;
    }
}
