package com.myFlink.java.project.link.bean;

import java.util.Objects;

public class Node implements Comparable<Node>{

    public String rpcId;
    public String parentAppId;
    public String appId;
    public String iFace;
    public String service;
    public String method;
    public String ipAddress;

    @Override
    public int compareTo(Node o) {
        int this_len = this.rpcId.split("\\.").length;
        int that_len = o.rpcId.split("\\.").length;
        int compare = this_len - that_len;

        if (compare == 0) {
            return 0;
        } else if (compare > 0){
            return 1;
        } else {
            return -1;
        }
    }

    public Node() {
    }

    public Node(String rpcId, String appId, String parentAppId, String iFace, String service, String method, String ipAddress) {
        this.rpcId = rpcId;
        this.appId = appId;
        this.parentAppId = parentAppId;
        this.iFace = iFace;
        this.service = service;
        this.method = method;
        this.ipAddress = ipAddress;
    }

    public String getParentAppId() {
        return parentAppId;
    }

    public void setParentAppId(String parentAppId) {
        this.parentAppId = parentAppId;
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

    public String getiFace() {
        return iFace;
    }

    public void setiFace(String iFace) {
        this.iFace = iFace;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return Objects.equals(rpcId, node.rpcId) &&
                Objects.equals(parentAppId, node.parentAppId) &&
                Objects.equals(appId, node.appId) &&
                Objects.equals(iFace, node.iFace) &&
                Objects.equals(service, node.service) &&
                Objects.equals(method, node.method) &&
                Objects.equals(ipAddress, node.ipAddress);
    }

    @Override
    public int hashCode() {

        return Objects.hash(rpcId, parentAppId, appId, iFace, service, method, ipAddress);
    }
}
