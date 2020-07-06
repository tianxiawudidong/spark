package com.ifchange.spark.util;

import java.util.Stack;

public class Xxtea {

    public static class XxteaDecodeException extends RuntimeException{
        public XxteaDecodeException(String message){
            super(message);
        }
    }
    public static class XxteaEncodeException extends RuntimeException{
        public XxteaEncodeException(String message){
            super(message);
        }
    }
    /**
     * 将字符串加密并返回
     * @param value 需要加密的字符串
     * @param key 长度必须为16的key
     * @return  加密结果
     */
    public static  String decode(String value,String key)  // 1.声明这是一个native函数，由本地代码实现
    {
        try {
            return jdecode(value,key);
        }catch (Throwable throwable){
            throw new XxteaDecodeException("key = " + key +"\tvalue = " + value);
        }
    }

    /**
     * 将加密串解密并返回
     * @param str 需要解密的字符串
     * @param key 长度必须为16的key
     * @return
     */
    public static String  encode(String str,String key)  // 1.声明这是一个native函数，由本地代码实现
    {
        try {
            return jencode(str,key);
        }catch (Throwable throwable){
            throw new XxteaEncodeException("key = " + key +"\tvalue = " + str);
        }
    }


    private static final long KEY_LENGTH = 16;
    private static final int ALIGNMENT_BYTES = 4;
    private static final long DELTA = 2654435769L;

    /**
     * 将字符串加密并返回
     * @param value 需要加密的字符串
     * @param key 长度必须为16的key
     * @return  加密结果
     */
    private static String jdecode(String value, String key) {
        if (value == null || key == null || key.length() != KEY_LENGTH) {
            return null;
        }
        return contactDecrypt(value.toCharArray(),key.toCharArray());
    }

    /**
     * 将加密串解密并返回
     * @param value 需要解密的字符串
     * @param key 长度必须为16的key
     * @return
     */
    private static String jencode(String value, String key) {
        if (value == null || key == null || key.length() != KEY_LENGTH) {
            return null;
        }
        byte[] bytes = value.getBytes();
        char[] chars = new char[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            chars[i] = (char) bytes[i];
        }
        return contactEncrypt(chars, key.toCharArray());
    }

    private static String contactEncrypt(char[] data, char[] key) {
        int data_len = data.length;
        key[0] = 0xF8;
        key[3] = 0x30;
        key[8] = 0x9B;
        key[11] = 0x01;
        int nSize = data_len / ALIGNMENT_BYTES + (data_len % ALIGNMENT_BYTES > 0 ? 1 : 0);
        int need_size = nSize * ALIGNMENT_BYTES;
        char[] tmp = new char[need_size];
        for (int i = 0; i < data.length; i++) {
            tmp[i] = data[i];
        }
        data = tmp;
        xxteaAlgorithm(data, nSize, key);
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < data.length; i++) {
            String upperCase = Integer.toHexString(data[i]).toUpperCase();
            if(upperCase.length() == 1){
                stringBuffer.append("0").append(Integer.toHexString(data[i]).toUpperCase());
            }else {
                stringBuffer.append(Integer.toHexString(data[i]).toUpperCase());
            }
        }
        return stringBuffer.toString();
    }


    public static String contactDecrypt(char[] data, char[] key) {
        key[0] = 0xF8;
        key[3] = 0x30;
        key[8] = 0x9B;
        key[11] = 0x01;
        int tmpLen = data.length / 2;
        char[] tmp = new char[tmpLen];
        for (int i = 0; i < tmp.length ; i ++ ) {
            int index = i * 2;
            int value = Integer.parseUnsignedInt(data[index] +""+ data[index + 1], 16);
            tmp[i] = (char) value;
        }
        xxteaAlgorithm( tmp, -(tmpLen / ALIGNMENT_BYTES),key);
        int ret = tmpLen;


        for (int i = tmpLen - 1; i >= 0; i--) {
            if (tmp[i] != '\0')
                break;
            else
                ret--;
        }
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < ret; i++) {
            if( (byte)tmp[i] < 0 ){
                byte[] bytes = {(byte) tmp[i], (byte) tmp[i + 1], (byte) tmp[i + 2]};
                String s = new String(bytes);
                stringBuffer.append(s);
                i+= 2;
            }else {
                stringBuffer.append(tmp[i]);
            }
        }
        return stringBuffer.toString();
    }


    private static void xxteaAlgorithm(char[] v, int n, char[] key) {
        long y, z, sum;
        long p, rounds, e;
        if (n > 1) {
            rounds = 6 + 52 / n;
            sum = 0;
            z = getValue(v, n - 1);
            do {
                sum = intValue(sum + DELTA);
                e = (sum >> 2) & 3;
                for (p = 0; p < n - 1; p++) {
                    y = getValue(v, p + 1);
                    long ii = MX(z, y, sum, key, p, e);
                    z = intValue( getValue(v, p) + ii );
                    setValue(v, p, z);
                }
                y = getValue(v, 0);
                long ii = MX(z, y, sum, key, p, e);
                z = intValue ( ii + getValue(v, n - 1));
                setValue(v, n - 1, z);
            } while (--rounds > 0);

        } else if (n < -1) {
            n = -n;
            rounds = 6 + 52 / n;
            sum = intValue( rounds * DELTA ) ;
            y = getValue(v,0);
            do {
                e = (sum >> 2) & 3;
                for (p = n - 1; p > 0; p--) {
                    z = getValue(v, p - 1);
                    y =  intValue( getValue(v, p) - MX(z, y, sum, key, p, e) );
                    setValue(v, p, y);
                }
                z = getValue(v, n - 1);
                long ii = MX(z, y, sum, key, p, e);
                y = intValue( getValue(v, 0) - ii);

                setValue(v, 0, y);
            } while ((sum = intValue(sum - DELTA) ) != 0);
        }
    }

    private static long MX(long z, long y, long sum, char[] key, long p, long e) {
        long index = (p & 3) ^ e;
        long l = (((z >> 5) ^ intValue(y << 2)) + ((y >> 3) ^ intValue(z << 4))) ^ ((sum ^ y) + (getValue(key, index) ^ z));
        return  intValue(l);
    }

    private static long intValue(long v){
        return v & (4294967295L);
    }

    private static void setValue(char[] v, long index, long value) {
        String s = Long.toBinaryString(value);
        int size = (int) (Math.min((index + 1) * ALIGNMENT_BYTES, v.length) - index * ALIGNMENT_BYTES);
        for (int j = s.length(); j < size * 8; j++) {
            s = "0" + s;
        }
        for (long i = Math.min(v.length, (index + 1) * ALIGNMENT_BYTES) - 1; i >= index * ALIGNMENT_BYTES; i--) {
            StringBuffer stringBuffer = new StringBuffer();
            for (int k = 0; k < 8; k++) {
                stringBuffer.append(s.toCharArray()[k]);
            }
            s = s.substring(8);
            int integer = Integer.parseUnsignedInt(stringBuffer.toString(), 2) ;
            v[(int) i] = (char) integer;
        }
    }

    private static long getValue(char[] key, long index) {
        Stack<String> stack = new Stack<>();
        for (long i = index * ALIGNMENT_BYTES; i < Math.min(key.length, (index + 1) * ALIGNMENT_BYTES); i++) {
            String s = // Integer.toBinaryString(Integer.valueOf(key[(int) i]));
                    Integer.toBinaryString(((byte)(key[(int) i]) & 0xFF) + 0x100).substring(1);
            for (int j = s.length(); j < 8; j++) {
                s = "0" + s;
            }
            stack.push(s);
        }
        StringBuffer stringBuffer = new StringBuffer();
        while (!stack.isEmpty()) {
            String pop = stack.pop();
            stringBuffer.append(pop);
        }
        return intValue(Integer.parseUnsignedInt( stringBuffer.toString(), 2));
    }

}
