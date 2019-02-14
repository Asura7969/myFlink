package com.myFlink.java.project.link.bean;

import java.util.Objects;

public class SoaLog {

    public String reqId;
    public String rpcId;
    public String appId;
    public String service;
    public String iFace;
    public String method;
    public String metric;
    public String ipAddress;
    public long logTime;

    /**
     * 标记对象是否可用
     */
    private Mark mark;

    public SoaLog(boolean userLess) {
        if (userLess) {
            this.mark = Mark.USELESS;
        }
        this.mark = Mark.USEFUL;
    }

    public SoaLog(String reqId, String rpcId, String appId, String service, String iFace, String method, String metric, String ipAddress, long logTime) {
        this.reqId = reqId;
        this.rpcId = rpcId;
        this.appId = appId;
        this.service = service;
        this.iFace = iFace;
        this.method = method;
        this.metric = metric;
        this.ipAddress = ipAddress;
        this.logTime = logTime;
        this.mark = Mark.USEFUL;
    }

    public SoaLog() {
    }

    public String getReqId() {
        return reqId;
    }

    public void setReqId(String reqId) {
        this.reqId = reqId;
    }

    public String getRpcId() {
        return rpcId;
    }

    public void setRpcId(String rpcId) {
        this.rpcId = rpcId;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getiFace() {
        return iFace;
    }

    public void setiFace(String iFace) {
        this.iFace = iFace;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public long getLogTime() {
        return logTime;
    }

    public void setLogTime(long logTime) {
        this.logTime = logTime;
    }

    public Mark getMark() {
        return mark;
    }

    @Override
    public String toString() {
        return "SoaLog{" +
                "reqId='" + reqId + '\'' +
                ", rpcId='" + rpcId + '\'' +
                ", appId='" + appId + '\'' +
                ", service='" + service + '\'' +
                ", iFace='" + iFace + '\'' +
                ", method='" + method + '\'' +
                ", metric='" + metric + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", logTime=" + logTime +
                ", mark=" + mark +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SoaLog soaLog = (SoaLog) o;
        return Objects.equals(rpcId, soaLog.rpcId) &&
                Objects.equals(appId, soaLog.appId) &&
                Objects.equals(service, soaLog.service) &&
                Objects.equals(iFace, soaLog.iFace) &&
                Objects.equals(method, soaLog.method) &&
                Objects.equals(ipAddress, soaLog.ipAddress);
    }

    @Override
    public int hashCode() {

        return Objects.hash(rpcId, appId, service, iFace, method, ipAddress);
    }
}
