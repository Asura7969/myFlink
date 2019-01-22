package com.myFlink.java.project.opentsdb;

import com.myFlink.java.project.opentsdb.builder.MetricBuilder;
import com.myFlink.java.project.opentsdb.response.Response;

import java.io.IOException;

public class WriteIntoOpentsdb {

    /** 
     * @Description: flink读取kafka数据，此处处理一条数据仅有一个数据项 
     * @param value
     * @return void 
     * @throws 
     */
    public void writeIntoOpentsdb(String value) {
        HttpClientImpl client = new HttpClientImpl("http://master1:4242");
        MetricBuilder builder = MetricBuilder.getInstance();
        Double value2 = Double.parseDouble(value);
        builder.addMetric("com.xxxxx.test.windpower").setDataPoint(value2).addTag("tag1","tab1value").addTag("tag2","tab2value");
        try {
            Response response = client.pushMetrics(builder,ExpectResponse.SUMMARY);
            System.out.println(response);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * @Description: flink读取kafka数据，此处处理一条数据中的多个数据项 
     * @param value
     * @return void
     * @throws 
     */
    public void writeIntoOpentsdb(String[] value){
        HttpClientImpl client = new HttpClientImpl("http://master1:4242");
        MetricBuilder builder = MetricBuilder.getInstance();
        //时间戳 
        long timeStramp = Long.parseLong(value[0]);
        //风机编号 
        String fanNumber = value[1];
        //机型编号 
        String model = value[2];
        //环境温度 
        Double ambientTemperature = Double.valueOf(value[3]);
        //机身温度 
        Double fuselageTemperature = Double.valueOf(value[4]);
        //风速 
        Double windSpeed = Double.valueOf(value[5]);
        //发电量 
        Double powerGeneration = Double.valueOf(value[6]);
        builder.addMetric("com.windpower.ambientTemperature").setDataPoint(timeStramp,ambientTemperature).addTag("fanNumber",fanNumber).addTag("model",model);
        builder.addMetric("com.windpower.fuselageTemperature").setDataPoint(timeStramp,fuselageTemperature).addTag("fanNumber",fanNumber).addTag("model",model);
        builder.addMetric("com.windpower.windSpeed").setDataPoint(timeStramp,windSpeed).addTag("fanNumber",fanNumber).addTag("model",model);
        builder.addMetric("com.windpower.powerGeneration").setDataPoint(timeStramp,powerGeneration).addTag("fanNumber",fanNumber).addTag("model",model);
        try {
            Response response = client.pushMetrics(builder,ExpectResponse.SUMMARY);
            System.out.println(response);
        } catch (IOException e){
            e.printStackTrace();
        }

        try {
            client.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
