package com.myFlink.java.project.link.utils;

import com.myFlink.java.project.link.bean.SoaLog;

import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TransFormSoaLog {

    private static final String YYYYMMDD_HHMMSS = "yyyy-MM-dd HH:mm:ss";
    private static final String YYYYMMDD_HHMMSS_S = "yyyy-MM-dd HH:mm:ss.S";
    private static final String YYYYMMDD_HHMMSS_SS = "yyyy-MM-dd HH:mm:ss.SS";
    private static final String YYYYMMDD_HHMMSS_SSS = "yyyy-MM-dd HH:mm:ss.SSS";

    private static final ZoneOffset ZONE = ZoneOffset.of("+8");

    private final static Charset UTF8 = Charset.forName("utf-8");

    private final static byte[] t1 = "\"reqid\":\"".getBytes(UTF8);
    private final static byte[] t2 = "\"rpcid\":\"".getBytes(UTF8);
    private final static byte[] t3 = "\"appid\":\"".getBytes(UTF8);
    private final static byte[] t4 = "\"service\":\"".getBytes(UTF8);
    private final static byte[] t5 = "\"iface\":\"".getBytes(UTF8);
    private final static byte[] t6 = "\"method\":\"".getBytes(UTF8);
    private final static byte[] t7 = "\"metric\":\"".getBytes(UTF8);
    private final static byte[] t8 = "\"ipAddress\":\"".getBytes(UTF8);

    private final static byte[][] SOA_KEY_ARRAY = {t1, t2, t3, t4, t5, t6, t7, t8};
    private final static String[] SOA_KEYS = new String[]{"reqid", "rpcid", "appid", "service", "iface", "method", "metric", "ipAddress"};

    public static boolean checkString(String timeStamp){
        char[] chars = timeStamp.toCharArray();
        for (char aChar : chars) {
            int c = (int) aChar;
            boolean flag = (c < 48 || c > 58) && (c != 32 && c != 45 && c != 46);
            if (flag) {
                return false;
            }
        }
        return true;
    }

    public static SoaLog lookUp(String log) {
        if (log == null || log.length() == 0) {
            return new SoaLog(true);
        }

        long logTime = System.currentTimeMillis();

        int offset = 0;

        for (int i = 0; i < 3; i++) {
            Integer pos = log.indexOf(',', offset + 1);
            if (pos > 0 && i == 1) {
                String timestamp = log.substring(offset, pos);
                if(checkString(timestamp)){
                    int d = timestamp.indexOf(".");
                    if (-1 != d) {
                        int le = timestamp.substring(d + 1, timestamp.length()).length();
                        String format = YYYYMMDD_HHMMSS_SSS;
                        switch (le){
                            case 1:
                                format = YYYYMMDD_HHMMSS_S;
                                break;
                            case 2:
                                format = YYYYMMDD_HHMMSS_SS;
                                break;
                            case 3:
                                break;
                        }
                        DateTimeFormatter df = DateTimeFormatter.ofPattern(format);
                        logTime = LocalDateTime.parse(timestamp, df).toInstant(ZONE).toEpochMilli();
                    } else {
                        DateTimeFormatter df = DateTimeFormatter.ofPattern(YYYYMMDD_HHMMSS);
                        logTime = LocalDateTime.parse(timestamp, df).toInstant(ZONE).toEpochMilli();
                    }

                } else {
                    return new SoaLog(true);
                }
            }
            offset += pos - offset + 1;
        }


        byte[] s = log.getBytes(UTF8);
        int LENGTH = 512;

        byte[][] valueArray = new byte[SOA_KEY_ARRAY.length][LENGTH];
        int[] nextArray = new int[SOA_KEY_ARRAY.length];
        int[] positionArray = new int[SOA_KEY_ARRAY.length];

        int state = -1;
        while (offset < s.length) {
            byte b = s[offset];

            if (state == -1) {
                for (int i = 0; i < SOA_KEY_ARRAY.length; i ++) {
                    int next = nextArray[i];
                    int len = SOA_KEY_ARRAY[i].length;
                    byte[] key = SOA_KEY_ARRAY[i];

                    if (next < len) {
                        if (key[next] == b) {
                            nextArray[i] ++;
                            if (nextArray[i] == len) {
                                state = i;
                            }
                        } else {
                            nextArray[i] = 0;
                        }
                    }
                }
            } else {
                if (b == '"' || b == ',' || b == '}') {
                    state = -1;
                } else {
                    for (int i = 0; i < SOA_KEY_ARRAY.length; ++i) {
                        if (state == i) {
                            if (positionArray[i] < LENGTH) {
                                valueArray[i][positionArray[i]++] = b;
                            }
                            break;
                        }
                    }
                }
            }
            offset ++;
        }

        SoaLog soa = new SoaLog(false);

        if (positionArray[0] > 0) {
            soa.setReqId(new String(valueArray[0], 0, positionArray[0], UTF8));
        }
        if (positionArray[1] > 0) {
            soa.setRpcId(new String(valueArray[1], 0, positionArray[1], UTF8));
        }
        if (positionArray[2] > 0) {
            soa.setAppId(new String(valueArray[2], 0, positionArray[2], UTF8));
        }
        if (positionArray[3] > 0) {
            soa.setService(new String(valueArray[3], 0, positionArray[3], UTF8));
        }
        if (positionArray[4] > 0) {
            soa.setiFace(new String(valueArray[4], 0, positionArray[4], UTF8));
        }
        if (positionArray[5] > 0) {
            soa.setMethod(new String(valueArray[5], 0, positionArray[5], UTF8));
        }
        if (positionArray[6] > 0) {
            soa.setMetric(new String(valueArray[6], 0, positionArray[6], UTF8));
        }
        if (positionArray[7] > 0) {
            soa.setIpAddress(new String(valueArray[7], 0, positionArray[7], UTF8));
        }
        soa.setLogTime(logTime);
        return soa;
    }

    public static void main(String[] args) {
        String log = "ANALYSIS,2018-11-07 14:44:27.22,2,{\"condition\":{\"method\":\"getInfo\",\"classname\":\"com.carkey.core.rpc.json.client.JsonTask\",\"provider\":\"http://10.46.76.37:40005\",\"service\":\"EasybikeUserService\",\"logowner\":\"soa\",\"appid\":\"easybike.gateway\",\"rpcid\":\"1.1\",\"weak\":\"FALSE\",\"reqid\":\"427f9586b54549b0beb2060bdd0a188a\",\"timestamp\":1510037067906},\"entityMata\":{\"ipAddress\":\"10.47.50.103\",\"logValue\":8.0,\"metric\":\"rpc_client_used_time\"}}";
        SoaLog info = TransFormSoaLog.lookUp(log);
        System.out.println(info);

    }
}
