package com.tencent.hbase;


public class RowKeyUtil {

    /**
     * 补齐20位，再反转
     *
     * @param userId
     * @return
     */
    public String formatUserId(long userId) {
        String str = String.format("%0" + 20 + "d", userId);
        StringBuilder sb = new StringBuilder(str);
        return sb.reverse().toString();
    }

    /**
     * Long.MAX_VALUE-lastupdate得到的值再补齐20位
     *
     * @param lastupdate, eg: "1479024369000"
     * @return
     */
    public String formatLastUpdate(long lastupdate) {
        if (lastupdate < 0) {
            lastupdate = 0;
        }
        long diff = Long.MAX_VALUE - lastupdate;
        return String.format("%0" + 20 + "d", diff);
    }

    public static void main(String[] args) {

        RowKeyUtil rowKeyUtil = new RowKeyUtil();
        // 运行时下行输出结果为54321000000000000000
        System.out.println(rowKeyUtil.formatUserId(12345L));

        long time = System.currentTimeMillis();
        // 运行时下行输出结果为09223370520503124434
        System.out.println(rowKeyUtil.formatLastUpdate(time));

    }
}
