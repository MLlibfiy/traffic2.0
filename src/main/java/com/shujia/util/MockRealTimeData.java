package com.shujia.util;

import java.util.Random;

public class MockRealTimeData extends Thread {

    private static final Random random = new Random();
    private static final String[] locations = new String[]{"鲁", "京", "京", "京", "沪", "京", "京", "深", "京", "京"};


    /**
     * 模拟摄像头
     *
     * @param args
     */
    public static void main(String[] args) {
        while (true) {
            String date = DateUtils.getTodayDate();
            String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24) + "");
            baseActionTime = date + " " + StringUtils.fulfuill((Integer.parseInt(baseActionTime.split(" ")[1]) + 1) + "");
            String actionTime = baseActionTime + ":" + StringUtils.fulfuill(random.nextInt(60) + "") + ":" + StringUtils.fulfuill(random.nextInt(60) + "");
            String monitorId = StringUtils.fulfuill(4, random.nextInt(9) + "");
            String car = locations[random.nextInt(10)] + (char) (65 + random.nextInt(26)) + StringUtils.fulfuill(5, random.nextInt(99999) + "");
            String speed = random.nextInt(260) + "";
            String roadId = random.nextInt(50) + 1 + "";
            String cameraId = StringUtils.fulfuill(5, random.nextInt(9999) + "");
            String areaId = StringUtils.fulfuill(2, random.nextInt(8) + "");
            String line = date + "\t" + monitorId + "\t" + cameraId + "\t" + car + "\t" + actionTime + "\t" + speed + "\t" + roadId + "\t" + areaId;
            System.out.println(line);

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
