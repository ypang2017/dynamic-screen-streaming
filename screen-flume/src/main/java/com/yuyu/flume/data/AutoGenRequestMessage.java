package com.yuyu.flume.data;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

import static java.lang.Thread.sleep;

/**
 * @author yuyu
 *
 *
 */
public class AutoGenRequestMessage {
    private static class PropertiesUtil{
        public static Properties getProperties(String path) throws IOException {
            Properties props = null;
            InputStream in = null;

            try {
                in = new BufferedInputStream(new FileInputStream(new File(path)));
                props = new Properties();
                props.load(in);
                return props;
            } catch (IOException e) {
                throw e;
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }
    private static String dataAutoGen() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        Long beginTime = calendar.getTimeInMillis();
        Long endTime = beginTime + 160000L;
        String date = sdf.format(calendar.getTime());
        Random random = new Random();

        JSONObject message = new JSONObject();
        JSONObject data1 = new JSONObject();
        JSONObject data2 = new JSONObject();
        JSONArray dataArray = new JSONArray();
        data1.put("system", "com.browser1");
        data1.put("transactionsum", random.nextInt(60000) + 60000);
        data2.put("system", "com.browser2");
        data2.put("transactionsum", random.nextInt(100000) + 60000);
        dataArray.add(data1);
        dataArray.add(data2);
        message.put("userId", random.nextInt(1000) + 1000);
        message.put("day", date);
        message.put("begintime", beginTime);
        message.put("endtime", endTime);
        message.put("data", dataArray);

        return message.toString() + '\n';
    }

    private static void dataAppend(String file, String conent) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
            out.write(conent);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(out != null){
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        String configFile = args[0];
        Properties dataProps = null;
        try {
            dataProps = PropertiesUtil.getProperties(configFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final int interval = Integer.parseInt(dataProps.getProperty("data.autogen.interval"));
        final String file = dataProps.getProperty("flume.data.path");
        dataAutoGen();
        while (true) {
            try {
                dataAppend(file, dataAutoGen());
                sleep(interval * 1000);
            }
            catch (Exception e) {
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
            }
        }
    }
}
