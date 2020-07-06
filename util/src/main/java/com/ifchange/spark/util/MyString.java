package com.ifchange.spark.util;



import java.io.*;
import java.lang.Character.UnicodeBlock;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

public class MyString implements Serializable {

    /*
     * 字符串补齐
     * @param source     源字符串
     * @param fillLength 补齐长度
     * @param fillChar   补齐的字符
     * @param isLeftFill true为左补齐，false为右补齐
     * @return
     */
    public static String stringFill(String source, int fillLength, char fillChar,
                                    boolean isLeftFill) {
        if (source == null || source.length() >= fillLength) {
            return source;
        }

        StringBuilder result = new StringBuilder(fillLength);
        int len = fillLength - source.length();
        if (isLeftFill) {
            for (; len > 0; len--) {
                result.append(fillChar);
            }
            result.append(source);
        } else {
            result.append(source);
            for (; len > 0; len--) {
                result.append(fillChar);
            }
        }
        return result.toString();
    }

    public static String stringFill2(String source, int fillLength, char fillChar,
                                     boolean isLeftFill) {
        if (source == null || source.length() >= fillLength) {
            return source;
        }

        char[] c = new char[fillLength];
        char[] s = source.toCharArray();
        int len = s.length;
        if (isLeftFill) {
            int fl = fillLength - len;
            for (int i = 0; i < fl; i++) {
                c[i] = fillChar;
            }
            System.arraycopy(s, 0, c, fl, len);
        } else {
            System.arraycopy(s, 0, c, 0, len);
            for (int i = len; i < fillLength; i++) {
                c[i] = fillChar;
            }
        }
        return String.valueOf(c);
    }

    /*
     * 解压(zlib)
     * @param  zippedText 要解压的字符串
     * @return boolean 返回解压缩后的字符串
     */
    public static String unzipString(String zippedText) throws IOException {
        String unzipped;
        byte[] zbytes = zippedText.getBytes(StandardCharsets.ISO_8859_1);
        // Add extra byte to array when Inflater is set to true
        byte[] input = new byte[zbytes.length + 1];
        System.arraycopy(zbytes, 0, input, 0, zbytes.length);
        input[zbytes.length] = 0;
        ByteArrayInputStream bin = new ByteArrayInputStream(input);
        InflaterInputStream in = new InflaterInputStream(bin);
        ByteArrayOutputStream bout = new ByteArrayOutputStream(512);
        int b;
        while ((b = in.read()) != -1) {
            bout.write(b);
        }
        bout.close();
        unzipped = bout.toString();
        return unzipped;
    }

    public static String unzipString(byte[] zbytes) throws Exception {
        String unzipped = null;
        // Add extra byte to array when Inflater is set to true
        byte[] input = new byte[zbytes.length + 1];
        System.arraycopy(zbytes, 0, input, 0, zbytes.length);
        input[zbytes.length] = 0;
        ByteArrayInputStream bin = new ByteArrayInputStream(input);
        InflaterInputStream in = new InflaterInputStream(bin);
        ByteArrayOutputStream bout = new ByteArrayOutputStream(512);
        int b;
        while ((b = in.read()) != -1) {
            bout.write(b);
        }
        bout.close();
        unzipped = bout.toString();
        return unzipped;
    }

    /*
     * 压缩(zlib)
     * @param input 要压缩的字符串
     * @return 压缩后的二进制数据
     * @throws Exception
     */
    public static byte[] zlib(String input) throws Exception {
        byte[] output;
        Deflater compresser = new Deflater();
        compresser.reset();
        compresser.setInput(input.getBytes());
        compresser.finish();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(input.getBytes().length);
        try {
            byte[] buf = new byte[1024];
            while (!compresser.finished()) {
                int i = compresser.deflate(buf);
                bos.write(buf, 0, i);
            }
            output = bos.toByteArray();
        } catch (Exception e) {
            output = input.getBytes();
            e.printStackTrace();
        } finally {
            try {
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        compresser.end();
        return output;
    }

    /*
     * 中文转换成 unicode
     *
     * @param inStr 原始数据
     * @return 加密字符串
     * @author fanhui 2007-3-15
     */
    public static String encodeUnicode(String inStr) {
        char[] myBuffer = inStr.toCharArray();

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < inStr.length(); i++) {
            char ch = myBuffer[i];
            if ((int) ch < 10) {
                sb.append("\\u000" + (int) ch);
                continue;
            }
            UnicodeBlock ub = UnicodeBlock.of(ch);
            if (ub == UnicodeBlock.BASIC_LATIN) {
                //英文及数字等
                sb.append(myBuffer[i]);
            } else if (ub == UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
                //全角半角字符
                int j = (int) myBuffer[i] - 65248;
                sb.append((char) j);
            } else {
                //汉字
                short s = (short) myBuffer[i];
                String hexS = Integer.toHexString(s);
                String unicode = "\\u" + hexS;
                sb.append(unicode.toLowerCase());
            }
        }
        return sb.toString();
    }

    /*
     * unicode 转换成 中文
     *
     * @param theString 原始字符串
     * @return 解码后的字符串
     * @author fanhui 2007-3-15
     */
    public static String decodeUnicode(String theString) {
        char aChar;
        int len = theString.length();
        StringBuilder outBuffer = new StringBuilder(len);
        for (int x = 0; x < len; ) {
            aChar = theString.charAt(x++);
            if (aChar == '\\') {
                aChar = theString.charAt(x++);
                if (aChar == 'u') {
                    // Read the xxxx
                    int value = 0;
                    for (int i = 0; i < 4; i++) {
                        aChar = theString.charAt(x++);
                        switch (aChar) {
                            case '0':
                            case '1':
                            case '2':
                            case '3':
                            case '4':
                            case '5':
                            case '6':
                            case '7':
                            case '8':
                            case '9':
                                value = (value << 4) + aChar - '0';
                                break;
                            case 'a':
                            case 'b':
                            case 'c':
                            case 'd':
                            case 'e':
                            case 'f':
                                value = (value << 4) + 10 + aChar - 'a';
                                break;
                            case 'A':
                            case 'B':
                            case 'C':
                            case 'D':
                            case 'E':
                            case 'F':
                                value = (value << 4) + 10 + aChar - 'A';
                                break;
                            default:
                                throw new IllegalArgumentException(
                                    "Malformed   \\uxxxx   encoding.");
                        }
                    }
                    outBuffer.append((char) value);
                } else {
                    if (aChar == 't') {
                        aChar = '\t';
                    } else if (aChar == 'r') {
                        aChar = '\r';
                    } else if (aChar == 'n') {
                        aChar = '\n';
                    } else if (aChar == 'f') {
                        aChar = '\f';
                    }
                    outBuffer.append(aChar);
                }
            } else {
                outBuffer.append(aChar);
            }
        }
        return outBuffer.toString();
    }

    private static final char[] hexDigit = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    private static char toHex(int nibble) {
        return hexDigit[(nibble & 0xF)];
    }

    public static String toUnicode(String theString, boolean escapeSpace) {
        return toUnicode(theString, escapeSpace, true);
    }

    public static String toUnicode(String theString) {
        return toUnicode(theString, true, false);
    }

    /*
     * 将字符串编码成 Unicode 。
     *
     * @param theString   待转换成Unicode编码的字符串。
     * @param escapeSpace 是否忽略空格。
     * @param issavedb    时候需要写库,写库的花斜杠会变成\\
     * @return 返回转换后Unicode编码的字符串。
     */
    public static String toUnicode(String theString, boolean escapeSpace, boolean issavedb) {
        int len = theString.length();
        int bufLen = len * 2;
        if (bufLen < 0) {
            bufLen = Integer.MAX_VALUE;
        }
        StringBuffer outBuffer = new StringBuffer(bufLen);

        for (int x = 0; x < len; x++) {
            char aChar = theString.charAt(x);
            // Handle common case first, selecting largest block that
            // avoids the specials below
            if ((aChar > 61) && (aChar < 127)) {
                if (aChar == '\\') {
                    outBuffer.append('\\');
                    outBuffer.append('\\');
                    continue;
                }
                outBuffer.append(aChar);
                continue;
            }
            switch (aChar) {
                case ' ':
                    if (x == 0 || escapeSpace) {
                        outBuffer.append('\\');
                    }
                    outBuffer.append(' ');
                    break;
                case '\t':
                    outBuffer.append('\\');
                    outBuffer.append('t');
                    break;
                case '\n':
                    outBuffer.append('\\');
                    outBuffer.append('n');
                    break;
                case '\r':
                    outBuffer.append('\\');
                    outBuffer.append('r');
                    break;
                case '\f':
                    outBuffer.append('\\');
                    outBuffer.append('f');
                    break;
                case '=': // Fall through
                case ':': // Fall through
                case '#': // Fall through
                case '!':
                default:
                    if ((aChar < 0x0020) || (aChar > 0x007e)) {
                        outBuffer.append('\\');
                        if (issavedb) {
                            outBuffer.append('\\');
                        }
                        outBuffer.append('u');
                        outBuffer.append(toHex((aChar >> 12) & 0xF));
                        outBuffer.append(toHex((aChar >> 8) & 0xF));
                        outBuffer.append(toHex((aChar >> 4) & 0xF));
                        outBuffer.append(toHex(aChar & 0xF));
                    } else {
                        outBuffer.append(aChar);
                    }
            }
        }
        return outBuffer.toString();
    }

    /*
     * 从 Unicode 码转换成编码前的特殊字符串。
     *
     * @param in       Unicode编码的字符数组。
     * @param off      转换的起始偏移量。
     * @param len      转换的字符长度。
     * @param convtBuf 转换的缓存字符数组。
     * @return 完成转换，返回编码前的特殊字符串。
     */
    public String fromUnicode(char[] in, int off, int len, char[] convtBuf) {
        if (convtBuf.length < len) {
            int newLen = len * 2;
            if (newLen < 0) {
                newLen = Integer.MAX_VALUE;
            }
            convtBuf = new char[newLen];
        }
        char aChar;
        char[] out = convtBuf;
        int outLen = 0;
        int end = off + len;

        while (off < end) {
            aChar = in[off++];
            if (aChar == '\\') {
                aChar = in[off++];
                if (aChar == 'u') {
                    // Read the xxxx
                    int value = 0;
                    for (int i = 0; i < 4; i++) {
                        aChar = in[off++];
                        switch (aChar) {
                            case '0':
                            case '1':
                            case '2':
                            case '3':
                            case '4':
                            case '5':
                            case '6':
                            case '7':
                            case '8':
                            case '9':
                                value = (value << 4) + aChar - '0';
                                break;
                            case 'a':
                            case 'b':
                            case 'c':
                            case 'd':
                            case 'e':
                            case 'f':
                                value = (value << 4) + 10 + aChar - 'a';
                                break;
                            case 'A':
                            case 'B':
                            case 'C':
                            case 'D':
                            case 'E':
                            case 'F':
                                value = (value << 4) + 10 + aChar - 'A';
                                break;
                            default:
                                throw new IllegalArgumentException(
                                    "Malformed \\uxxxx encoding.");
                        }
                    }
                    out[outLen++] = (char) value;
                } else {
                    if (aChar == 't') {
                        aChar = '\t';
                    } else if (aChar == 'r') {
                        aChar = '\r';
                    } else if (aChar == 'n') {
                        aChar = '\n';
                    } else if (aChar == 'f') {
                        aChar = '\f';
                    }
                    out[outLen++] = aChar;
                }
            } else {
                out[outLen++] = aChar;
            }
        }
        return new String(out, 0, outLen);
    }

    //汉字文本相似度计算
    public static double getSimilarity(String doc1, String doc2) {
        if (doc1 != null && doc1.trim().length() > 0 && doc2 != null
            && doc2.trim().length() > 0) {

            Map<Integer, int[]> AlgorithmMap = new HashMap<>();
            //将两个字符串中的中文字符以及出现的总数封装到，AlgorithmMap中
            for (int i = 0; i < doc1.length(); i++) {
                char d1 = doc1.charAt(i);
                if (isHanZi(d1)) {
                    int charIndex = getGB2312Id(d1);
                    if (charIndex != -1) {
                        int[] fq = AlgorithmMap.get(charIndex);
                        if (fq != null && fq.length == 2) {
                            fq[0]++;
                        } else {
                            fq = new int[2];
                            fq[0] = 1;
                            fq[1] = 0;
                            AlgorithmMap.put(charIndex, fq);
                        }
                    }
                }
            }

            for (int i = 0; i < doc2.length(); i++) {
                char d2 = doc2.charAt(i);
                if (isHanZi(d2)) {
                    int charIndex = getGB2312Id(d2);
                    if (charIndex != -1) {
                        int[] fq = AlgorithmMap.get(charIndex);
                        if (fq != null && fq.length == 2) {
                            fq[1]++;
                        } else {
                            fq = new int[2];
                            fq[0] = 0;
                            fq[1] = 1;
                            AlgorithmMap.put(charIndex, fq);
                        }
                    }
                }
            }

            Iterator<Integer> iterator = AlgorithmMap.keySet().iterator();
            double sqdoc1 = 0;
            double sqdoc2 = 0;
            double denominator = 0;
            while (iterator.hasNext()) {
                int[] c = AlgorithmMap.get(iterator.next());
                denominator += c[0] * c[1];
                sqdoc1 += c[0] * c[0];
                sqdoc2 += c[1] * c[1];
            }

            return denominator / Math.sqrt(sqdoc1 * sqdoc2);
        } else {
            throw new NullPointerException(
                " the Document is null or have not cahrs!!");
        }
    }

    // 判断是否汉字
    private static boolean isHanZi(char ch) {
        return (ch >= 0x4E00 && ch <= 0x9FA5);

    }

    /*
     * 根据输入的Unicode字符，获取它的GB2312编码或者ascii编码，
     * @param ch 输入的GB2312中文字符或者ASCII字符(128个)
     * @return ch在GB2312中的位置，-1表示该字符不认识
     */
    private static short getGB2312Id(char ch) {
        try {
            byte[] buffer = Character.toString(ch).getBytes("GB2312");
            if (buffer.length != 2) {
                // 正常情况下buffer应该是两个字节，否则说明ch不属于GB2312编码，故返回'?'，此时说明不认识该字符
                return -1;
            }
            int b0 = (buffer[0] & 0x0FF) - 161; // 编码从A1开始，因此减去0xA1=161
            int b1 = (buffer[1] & 0x0FF) - 161; // 第一个字符和最后一个字符没有汉字，因此每个区只收16*6-2=94个汉字
            return (short) (b0 * 94 + b1);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private static int min(int one, int two, int three) {
        int min = one;
        if (two < min) {
            min = two;
        }
        if (three < min) {
            min = three;
        }
        return min;
    }

    private static int ld(String str1, String str2) {
        int d[][]; // 矩阵
        int n = str1.length();
        int m = str2.length();
        int i; // 遍历str1的
        int j; // 遍历str2的
        char ch1; // str1的
        char ch2; // str2的
        int temp; // 记录相同字符,在某个矩阵位置值的增量,不是0就是1
        if (n == 0) {
            return m;
        }
        if (m == 0) {
            return n;
        }
        d = new int[n + 1][m + 1];
        for (i = 0; i <= n; i++) { // 初始化第一列
            d[i][0] = i;
        }
        for (j = 0; j <= m; j++) { // 初始化第一行
            d[0][j] = j;
        }
        for (i = 1; i <= n; i++) { // 遍历str1
            ch1 = str1.charAt(i - 1);
            // 去匹配str2
            for (j = 1; j <= m; j++) {
                ch2 = str2.charAt(j - 1);
                if (ch1 == ch2) {
                    temp = 0;
                } else {
                    temp = 1;
                }
                // 左边+1,上边+1, 左上角+temp取最小
                d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + temp);
            }
        }
        return d[n][m];
    }

    //文本相似度计算
    public static double getSimilarity2(String str1, String str2) {
        try {
            double ld = (double) ld(str1, str2);
            return (1 - ld / (double) Math.max(str1.length(), str2.length()));
        } catch (Exception e) {
            return 0.1;
        }
    }

    //比较字符串的顺序
    public static int getSortOrder(String string1, String string2) {
        if (string1.equals(string2)) {
            return 0;
        }
        int length1 = string1.length();
        int length2 = string2.length();
        for (int i = 0; i < length1; i++) {
            int char_int1 = (int) string1.charAt(i);
            int char_int2 = length2 <= i ? 0 : (int) string2.charAt(i);
            if (char_int1 != char_int2) {
                return char_int1 - char_int2;
            }
        }
        return (int) string2.charAt(length1);

    }

    /**
     * 二进制转16进制
     *
     * @param src 原始byte数据
     * @return 16进制字符串
     */
    public static String bytesToHexString(byte[] src) {
        StringBuilder stringBuilder = new StringBuilder("");
        if (src == null || src.length <= 0) {
            return null;
        }
        for (byte aSrc : src) {
            int v = aSrc & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                stringBuilder.append(0);
            }
            stringBuilder.append(hv);
        }
        return stringBuilder.toString();
    }

    /**
     * 16进制字符串转成二进制数组 Convert hex string to byte[]
     *
     * @param hexString 16进制string
     * @return byte[]
     */
    public static byte[] hexStringToBytes(String hexString) {
        if (hexString == null || hexString.equals("")) {
            return null;
        }
        hexString = hexString.toUpperCase();
        int length = hexString.length() / 2;
        char[] hexChars = hexString.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return d;
    }

    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }


    /**
     * gzip 压缩
     */
    public static byte[] gzipCompress(String str, String encoding) {
        if (str == null || str.length() == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(str.getBytes(encoding));
            gzip.finish();
            gzip.flush();
            gzip.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out.toByteArray();
    }

    /**
     * gzip uncompress
     */
    public static byte[] gzipUncompress(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        try {
            GZIPInputStream gzipInputStream = new GZIPInputStream(in);
            byte[] buffer = new byte[256];
            int n;
            while ((n = gzipInputStream.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
            gzipInputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out.toByteArray();
    }

    public static String getMD5(String plainText) {
//        long start = System.currentTimeMillis();
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");//获取MD5实例
            md.update(plainText.getBytes());//此处传入要加密的byte类型值
            byte[] digest = md.digest();//此处得到的是md5加密后的byte类型值

            /*
               下边的运算就是自己添加的一些二次小加密，记住这个千万不能弄错乱，
                   否则在解密的时候，你会发现值不对的（举例：在注册的时候加密方式是一种，
                在我们登录的时候是不是还需要加密它的密码然后和数据库的进行比对，但是
            最后我们发现，明明密码对啊，就是打不到预期效果，这时候你就要想一下，你是否
             有改动前后的加密方式）
            */
            int i;
            StringBuilder sb = new StringBuilder();
            for (int offset = 0; offset < digest.length; offset++) {
                i = digest[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    sb.append(0);
                sb.append(Integer.toHexString(i));//通过Integer.toHexString方法把值变为16进制
            }
//            long end = System.currentTimeMillis();
//            System.out.println("use time:" + (end - start));
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        String str1 = "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";
//        String result = new String(MyString.gzipUncompress(Base64.getDecoder().decode(str1)));
        String md5 = getMD5(str1);
        System.out.println(md5);

    }

}
